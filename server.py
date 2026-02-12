"""PM Analytics MCP Server

Provides computed project management analytics over OpenProject data:
- Project health scoring and burndown projections
- Earned Value Management (EVM) metrics
- Risk scanning and risk register generation
- Design-build document tracking (RFIs, submittals, change orders, permits)
- Portfolio-level rollup across multiple projects

Architecture: Fetches raw data from OpenProject API, computes analytics
in-memory, returns structured results. Single parameterized tool with
5 operations following the openproject-mcp registry pattern.
"""
import asyncio
import base64
import json
import logging
import os
import re
from collections import defaultdict, deque
from datetime import datetime, timedelta, date
from typing import Any, Optional

import httpx
from fastmcp import FastMCP

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

mcp = FastMCP("PM Analytics")

OPENPROJECT_URL = os.environ.get("OPENPROJECT_URL", "http://localhost:8080")
OPENPROJECT_API_KEY = os.environ.get("OPENPROJECT_API_KEY", "")


def _basic_auth() -> str:
    return base64.b64encode(f"apikey:{OPENPROJECT_API_KEY}".encode()).decode()


def _api_headers() -> dict:
    return {
        "Content-Type": "application/json",
        "Authorization": f"Basic {_basic_auth()}",
    }


# ═══════════════════════════════════════════════════════════════════════
# OpenProject API Helpers
# ═══════════════════════════════════════════════════════════════════════


async def _fetch_all(
    client: httpx.AsyncClient,
    endpoint: str,
    params: Optional[dict] = None,
) -> list[dict]:
    """Fetch all pages from a paginated OP collection endpoint."""
    items = []
    offset = 1
    page_size = 100
    base_params = params or {}

    while True:
        p = {**base_params, "offset": offset, "pageSize": page_size}
        resp = await client.get(
            f"{OPENPROJECT_URL}/api/v3{endpoint}",
            params=p,
            headers=_api_headers(),
        )
        resp.raise_for_status()
        data = resp.json()
        elements = data.get("_embedded", {}).get("elements", [])
        items.extend(elements)
        total = data.get("total", 0)
        if offset * page_size >= total:
            break
        offset += 1

    return items


async def _fetch_one(
    client: httpx.AsyncClient,
    endpoint: str,
) -> dict:
    """Fetch a single resource."""
    resp = await client.get(
        f"{OPENPROJECT_URL}/api/v3{endpoint}",
        headers=_api_headers(),
    )
    resp.raise_for_status()
    return resp.json()


async def _fetch_projects(client: httpx.AsyncClient, active_only: bool = True) -> list[dict]:
    """Fetch all projects."""
    params = {}
    if active_only:
        params["filters"] = json.dumps([{"active": {"operator": "=", "values": ["t"]}}])
    return await _fetch_all(client, "/projects", params)


async def _fetch_work_packages(
    client: httpx.AsyncClient,
    project_id: int | str,
    status: str = "all",
    filters: Optional[list] = None,
) -> list[dict]:
    """Fetch work packages for a project with optional status filter."""
    f = filters or []
    if status == "open":
        f.append({"status_id": {"operator": "o", "values": None}})
    elif status == "closed":
        f.append({"status_id": {"operator": "c", "values": None}})
    params = {}
    if f:
        params["filters"] = json.dumps(f)
    return await _fetch_all(client, f"/projects/{project_id}/work_packages", params)


async def _fetch_relations(
    client: httpx.AsyncClient,
    wp_ids: list[int],
) -> list[dict]:
    """Fetch all relations involving the given WP IDs."""
    if not wp_ids:
        return []
    params = {
        "filters": json.dumps([
            {"involved": {"operator": "=", "values": [str(wid) for wid in wp_ids]}}
        ])
    }
    return await _fetch_all(client, "/relations", params)


async def _fetch_time_entries(
    client: httpx.AsyncClient,
    project_id: Optional[int | str] = None,
    wp_id: Optional[int] = None,
) -> list[dict]:
    """Fetch time entries optionally filtered by project or WP."""
    filters = []
    if project_id:
        filters.append({"project": {"operator": "=", "values": [str(project_id)]}})
    if wp_id:
        filters.append({"workPackage": {"operator": "=", "values": [str(wp_id)]}})
    params = {}
    if filters:
        params["filters"] = json.dumps(filters)
    return await _fetch_all(client, "/time_entries", params)


# ═══════════════════════════════════════════════════════════════════════
# WP Data Extraction Helpers
# ═══════════════════════════════════════════════════════════════════════


def _wp_status_name(wp: dict) -> str:
    return wp.get("_embedded", {}).get("status", {}).get("name", "Unknown")


def _wp_is_closed(wp: dict) -> bool:
    return wp.get("_embedded", {}).get("status", {}).get("isClosed", False)


def _wp_assignee_name(wp: dict) -> str:
    a = wp.get("_embedded", {}).get("assignee")
    return a.get("name", "Unassigned") if a else "Unassigned"


def _wp_assignee_id(wp: dict) -> Optional[int]:
    a = wp.get("_embedded", {}).get("assignee")
    return a.get("id") if a else None


def _wp_type_name(wp: dict) -> str:
    return wp.get("_embedded", {}).get("type", {}).get("name", "Task")


def _wp_project_name(wp: dict) -> str:
    return wp.get("_embedded", {}).get("project", {}).get("name", "?")


def _wp_category_name(wp: dict) -> str:
    cat = wp.get("_embedded", {}).get("category")
    return cat.get("name", "") if cat else ""


def _parse_date(d: Optional[str]) -> Optional[date]:
    if not d:
        return None
    try:
        return datetime.fromisoformat(d.replace("Z", "+00:00")).date() if "T" in d else date.fromisoformat(d)
    except (ValueError, TypeError):
        return None


def _parse_iso_hours(duration_str: str) -> float:
    """Parse ISO 8601 duration (PT2H30M) to decimal hours."""
    if not duration_str or "PT" not in duration_str:
        return 0.0
    match = re.match(
        r"PT(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?",
        duration_str,
    )
    if not match:
        return 0.0
    h = float(match.group(1) or 0)
    m = float(match.group(2) or 0)
    return h + m / 60


def _today() -> date:
    return date.today()


# ═══════════════════════════════════════════════════════════════════════
# CPM Functions (adapted from cpm-mcp for include_cpm enrichment)
# ═══════════════════════════════════════════════════════════════════════


def _parse_iso_duration_days(duration_str: str) -> float:
    """Parse ISO 8601 duration to working days. Assumes 8h = 1 day."""
    if not duration_str:
        return 1
    if not duration_str.startswith("P"):
        return 1
    hours = 0
    days = 0
    h_match = re.search(r"(\d+(?:\.\d+)?)H", duration_str)
    d_match = re.search(r"(\d+(?:\.\d+)?)D", duration_str)
    w_match = re.search(r"(\d+(?:\.\d+)?)W", duration_str)
    if h_match:
        hours = float(h_match.group(1))
    if d_match:
        days = float(d_match.group(1))
    if w_match:
        days += float(w_match.group(1)) * 5
    return days + hours / 8


def _build_task_graph(work_packages: list[dict], relations: list[dict]) -> dict:
    """Build adjacency list from work packages + relations for CPM.

    Returns dict of task_id -> {name, duration, successors, predecessors, ...}
    """
    tasks = {}
    for wp in work_packages:
        wp_id = wp["id"]
        duration = 1
        estimated = wp.get("estimatedTime")
        if estimated:
            duration = _parse_iso_duration_days(estimated)

        tasks[wp_id] = {
            "id": wp_id,
            "name": wp.get("subject", f"WP#{wp_id}"),
            "duration": max(duration, 0),
            "successors": [],
            "predecessors": [],
            "start_date": wp.get("startDate"),
            "due_date": wp.get("dueDate"),
        }

    for rel in relations:
        from_href = rel.get("_links", {}).get("from", {}).get("href", "")
        to_href = rel.get("_links", {}).get("to", {}).get("href", "")
        from_id = _extract_id(from_href)
        to_id = _extract_id(to_href)

        if from_id not in tasks or to_id not in tasks:
            continue

        rel_type = rel.get("type", "")
        if rel_type == "follows":
            # from follows to => to is predecessor of from
            tasks[from_id]["predecessors"].append(to_id)
            tasks[to_id]["successors"].append(from_id)
        elif rel_type == "precedes":
            # from precedes to => from is predecessor of to
            tasks[to_id]["predecessors"].append(from_id)
            tasks[from_id]["successors"].append(to_id)

    return tasks


def _compute_cpm(tasks: dict) -> dict:
    """Run forward and backward pass to compute CPM schedule.

    Returns dict with tasks (enriched with ES/EF/LS/LF/float), critical_path,
    project_duration, and negative_float_alerts. Uses offset-based CPM (no calendar).
    """
    if not tasks:
        return {"tasks": {}, "critical_path": [], "project_duration": 0, "negative_float_alerts": []}

    for t in tasks.values():
        t["ES"] = 0
        t["EF"] = 0
        t["LS"] = float("inf")
        t["LF"] = float("inf")
        t["total_float"] = 0
        t["is_critical"] = False

    # Topological sort (Kahn's algorithm)
    in_degree = defaultdict(int)
    for t in tasks.values():
        for succ in t["successors"]:
            in_degree[succ] += 1

    queue = deque()
    for tid, t in tasks.items():
        if not t["predecessors"]:
            queue.append(tid)
            t["ES"] = 0
            t["EF"] = t["duration"]

    topo_order = []
    while queue:
        tid = queue.popleft()
        topo_order.append(tid)
        t = tasks[tid]
        for succ_id in t["successors"]:
            succ = tasks[succ_id]
            if t["EF"] >= succ["ES"]:
                succ["ES"] = t["EF"]
                succ["EF"] = succ["ES"] + succ["duration"]
            in_degree[succ_id] -= 1
            if in_degree[succ_id] == 0:
                queue.append(succ_id)

    project_duration = max((t["EF"] for t in tasks.values()), default=0)

    # Backward pass
    for tid in reversed(topo_order):
        t = tasks[tid]
        if not t["successors"]:
            t["LF"] = project_duration
            t["LS"] = t["LF"] - t["duration"]
        else:
            t["LF"] = min(tasks[s]["LS"] for s in t["successors"])
            t["LS"] = t["LF"] - t["duration"]

        t["total_float"] = t["LS"] - t["ES"]
        t["is_critical"] = abs(t["total_float"]) < 0.001

    critical_path = [tid for tid in topo_order if tasks[tid]["is_critical"]]

    negative_float_alerts = [
        {"id": tid, "name": tasks[tid]["name"], "float": round(tasks[tid]["total_float"], 2)}
        for tid in topo_order
        if tasks[tid]["total_float"] < -0.001
    ]

    return {
        "tasks": {
            tid: {
                "id": t["id"],
                "name": t["name"],
                "duration": t["duration"],
                "ES": t["ES"],
                "EF": t["EF"],
                "LS": round(t["LS"], 2),
                "LF": round(t["LF"], 2),
                "total_float": round(t["total_float"], 2),
                "is_critical": t["is_critical"],
            }
            for tid, t in tasks.items()
        },
        "critical_path": critical_path,
        "project_duration": project_duration,
        "negative_float_alerts": negative_float_alerts,
    }


# ═══════════════════════════════════════════════════════════════════════
# B1: project_health
# ═══════════════════════════════════════════════════════════════════════


async def _project_health(client: httpx.AsyncClient, args: dict) -> dict:
    """Compute project health metrics."""
    project_id = args["project_id"]
    as_of = _parse_date(args.get("as_of")) or _today()

    all_wps = await _fetch_work_packages(client, project_id, status="all")
    if not all_wps:
        return {"error": f"No work packages found for project {project_id}"}

    total = len(all_wps)
    closed = [wp for wp in all_wps if _wp_is_closed(wp)]
    open_wps = [wp for wp in all_wps if not _wp_is_closed(wp)]

    # Overdue: open WPs past due date
    overdue = []
    for wp in open_wps:
        due = _parse_date(wp.get("dueDate"))
        if due and due < as_of:
            days_over = (as_of - due).days
            overdue.append({
                "id": wp["id"],
                "subject": wp.get("subject"),
                "assignee": _wp_assignee_name(wp),
                "due_date": str(due),
                "days_overdue": days_over,
            })
    overdue.sort(key=lambda x: x["days_overdue"], reverse=True)

    # Blocked: WPs with unresolved 'blocks' relations
    wp_ids = [wp["id"] for wp in all_wps]
    relations = await _fetch_relations(client, wp_ids)
    closed_ids = {wp["id"] for wp in closed}

    blocked_ids = set()
    for rel in relations:
        if rel.get("type") == "blocks":
            from_href = rel.get("_links", {}).get("from", {}).get("href", "")
            to_href = rel.get("_links", {}).get("to", {}).get("href", "")
            from_id = _extract_id(from_href)
            to_id = _extract_id(to_href)
            # 'from' blocks 'to' — if 'from' is not closed, 'to' is blocked
            if from_id and to_id and from_id not in closed_ids:
                blocked_ids.add(to_id)

    blocked = [
        {"id": wp["id"], "subject": wp.get("subject"), "assignee": _wp_assignee_name(wp)}
        for wp in open_wps if wp["id"] in blocked_ids
    ]

    # Unassigned
    unassigned = [
        {"id": wp["id"], "subject": wp.get("subject")}
        for wp in open_wps if not _wp_assignee_id(wp)
    ]

    # Velocity: WPs closed per week over trailing 4 weeks
    four_weeks_ago = as_of - timedelta(weeks=4)
    recent_closed = 0
    for wp in closed:
        updated = _parse_date(wp.get("updatedAt"))
        if updated and updated >= four_weeks_ago:
            recent_closed += 1
    velocity = recent_closed / 4.0 if recent_closed > 0 else 0

    # Burndown projection
    remaining = total - len(closed)
    projected_weeks = remaining / velocity if velocity > 0 else None

    # Completion %
    completion_pct = round((len(closed) / total) * 100, 1) if total > 0 else 0

    # Health status
    if len(overdue) == 0 and len(blocked) == 0:
        health = "on_track"
    elif len(overdue) <= 2 and len(blocked) <= 1:
        health = "at_risk"
    else:
        health = "behind"

    result = {
        "project_id": project_id,
        "as_of": str(as_of),
        "total_work_packages": total,
        "closed": len(closed),
        "open": len(open_wps),
        "completion_pct": completion_pct,
        "health": health,
        "overdue_count": len(overdue),
        "overdue": overdue[:20],
        "blocked_count": len(blocked),
        "blocked": blocked[:20],
        "unassigned_count": len(unassigned),
        "unassigned": unassigned[:20],
        "velocity_per_week": round(velocity, 2),
        "remaining": remaining,
        "projected_weeks_to_complete": round(projected_weeks, 1) if projected_weeks else None,
    }

    # CPM enrichment (when include_cpm=true)
    if args.get("include_cpm"):
        try:
            task_graph = _build_task_graph(all_wps, relations)
            cpm = _compute_cpm(task_graph)
            result["critical_path"] = [
                {"id": tid, "name": cpm["tasks"][tid]["name"]}
                for tid in cpm["critical_path"]
                if tid in cpm["tasks"]
            ]
            result["project_duration_days"] = cpm["project_duration"]
            result["negative_float_alerts"] = cpm["negative_float_alerts"]
        except Exception as e:
            logger.warning(f"CPM enrichment failed for project {project_id}: {e}")
            result["cpm_error"] = str(e)

    return result


# ═══════════════════════════════════════════════════════════════════════
# B2: earned_value
# ═══════════════════════════════════════════════════════════════════════


async def _earned_value(client: httpx.AsyncClient, args: dict) -> dict:
    """Compute Earned Value Management metrics."""
    project_id = args["project_id"]
    as_of = _parse_date(args.get("as_of")) or _today()
    hourly_rate = args.get("hourly_rate", 100.0)

    all_wps = await _fetch_work_packages(client, project_id, status="all")
    if not all_wps:
        return {"error": f"No work packages found for project {project_id}"}

    # Gather time entries for actual cost
    time_entries = await _fetch_time_entries(client, project_id=project_id)

    # Budget at completion (BAC) — sum of estimated hours across all WPs
    total_estimated_hours = 0.0
    for wp in all_wps:
        est = wp.get("estimatedTime")
        if est:
            total_estimated_hours += _parse_iso_hours(est)

    bac = total_estimated_hours * hourly_rate

    # Actual Cost (AC) — sum of logged time
    total_actual_hours = 0.0
    for te in time_entries:
        h = _parse_iso_hours(te.get("hours", "PT0H"))
        spent_on = _parse_date(te.get("spentOn"))
        if spent_on and spent_on <= as_of:
            total_actual_hours += h
    ac = total_actual_hours * hourly_rate

    # Planned % complete at as_of date
    # Simple: proportion of WPs whose due date <= as_of
    total_wps = len(all_wps)
    planned_complete = 0
    for wp in all_wps:
        due = _parse_date(wp.get("dueDate"))
        if due and due <= as_of:
            planned_complete += 1
    planned_pct = planned_complete / total_wps if total_wps > 0 else 0

    # Actual % complete — weighted by WP percentage_done
    actual_pct_sum = sum((wp.get("percentageDone") or 0) for wp in all_wps)
    actual_pct = actual_pct_sum / (total_wps * 100) if total_wps > 0 else 0

    # EVM calculations
    pv = bac * planned_pct  # Planned Value
    ev = bac * actual_pct   # Earned Value

    sv = ev - pv            # Schedule Variance
    cv = ev - ac            # Cost Variance
    spi = ev / pv if pv > 0 else 0  # Schedule Performance Index
    cpi = ev / ac if ac > 0 else 0  # Cost Performance Index
    eac = bac / cpi if cpi > 0 else 0  # Estimate at Completion

    return {
        "project_id": project_id,
        "as_of": str(as_of),
        "hourly_rate": hourly_rate,
        "bac": round(bac, 2),
        "total_estimated_hours": round(total_estimated_hours, 2),
        "total_actual_hours": round(total_actual_hours, 2),
        "planned_pct_complete": round(planned_pct * 100, 1),
        "actual_pct_complete": round(actual_pct * 100, 1),
        "pv": round(pv, 2),
        "ev": round(ev, 2),
        "ac": round(ac, 2),
        "sv": round(sv, 2),
        "cv": round(cv, 2),
        "spi": round(spi, 3),
        "cpi": round(cpi, 3),
        "eac": round(eac, 2),
        "variance_at_completion": round(bac - eac, 2),
        "interpretation": {
            "schedule": "ahead" if spi > 1 else ("on_track" if spi == 1 else "behind"),
            "cost": "under_budget" if cpi > 1 else ("on_budget" if cpi == 1 else "over_budget"),
        },
    }


# ═══════════════════════════════════════════════════════════════════════
# B3: risk_scan
# ═══════════════════════════════════════════════════════════════════════


async def _risk_scan(client: httpx.AsyncClient, args: dict) -> dict:
    """Scan for project risks and generate risk register."""
    project_id = args["project_id"]
    as_of = _parse_date(args.get("as_of")) or _today()

    # Configurable thresholds
    thresholds = args.get("thresholds", {})
    if isinstance(thresholds, str):
        thresholds = json.loads(thresholds)
    stale_days = thresholds.get("stale_days", 7)
    overload_threshold = thresholds.get("overload_threshold", 5)
    approaching_days = thresholds.get("approaching_days", 7)

    all_wps = await _fetch_work_packages(client, project_id, status="all")
    open_wps = [wp for wp in all_wps if not _wp_is_closed(wp)]
    closed_ids = {wp["id"] for wp in all_wps if _wp_is_closed(wp)}
    wp_ids = [wp["id"] for wp in all_wps]
    relations = await _fetch_relations(client, wp_ids)

    risks = []

    # 1. Overdue cascade — overdue WPs and their downstream successors
    successor_map = defaultdict(list)
    for rel in relations:
        rel_type = rel.get("type", "")
        from_href = rel.get("_links", {}).get("from", {}).get("href", "")
        to_href = rel.get("_links", {}).get("to", {}).get("href", "")
        from_id = _extract_id(from_href)
        to_id = _extract_id(to_href)
        if not from_id or not to_id:
            continue
        if rel_type == "follows":
            # from follows to => to precedes from
            successor_map[to_id].append(from_id)
        elif rel_type == "precedes":
            successor_map[from_id].append(to_id)

    for wp in open_wps:
        due = _parse_date(wp.get("dueDate"))
        if due and due < as_of:
            days_over = (as_of - due).days
            downstream = _get_downstream(wp["id"], successor_map, closed_ids)
            risks.append({
                "type": "overdue_cascade",
                "severity": "critical" if days_over > 14 or len(downstream) > 3 else "high",
                "wp_id": wp["id"],
                "subject": wp.get("subject"),
                "days_overdue": days_over,
                "downstream_impact": len(downstream),
                "downstream_ids": downstream[:10],
            })

    # 2. Unresolved blockers
    for rel in relations:
        if rel.get("type") == "blocks":
            from_id = _extract_id(rel.get("_links", {}).get("from", {}).get("href", ""))
            to_id = _extract_id(rel.get("_links", {}).get("to", {}).get("href", ""))
            if from_id and to_id and from_id not in closed_ids and to_id not in closed_ids:
                blocker_wp = next((wp for wp in all_wps if wp["id"] == from_id), None)
                blocked_wp = next((wp for wp in all_wps if wp["id"] == to_id), None)
                risks.append({
                    "type": "unresolved_blocker",
                    "severity": "high",
                    "blocker_id": from_id,
                    "blocker_subject": blocker_wp.get("subject") if blocker_wp else "?",
                    "blocked_id": to_id,
                    "blocked_subject": blocked_wp.get("subject") if blocked_wp else "?",
                })

    # 3. Stale tasks — in-progress with no recent activity
    for wp in open_wps:
        status_name = _wp_status_name(wp).lower()
        if "progress" in status_name or "active" in status_name:
            updated = _parse_date(wp.get("updatedAt"))
            if updated and (as_of - updated).days > stale_days:
                risks.append({
                    "type": "stale_task",
                    "severity": "medium",
                    "wp_id": wp["id"],
                    "subject": wp.get("subject"),
                    "assignee": _wp_assignee_name(wp),
                    "days_since_update": (as_of - updated).days,
                })

    # 4. Resource overload
    assignee_workload = defaultdict(list)
    for wp in open_wps:
        due = _parse_date(wp.get("dueDate"))
        assignee = _wp_assignee_name(wp)
        aid = _wp_assignee_id(wp)
        if aid and due and as_of <= due <= as_of + timedelta(days=7):
            assignee_workload[assignee].append(wp["id"])

    for assignee, wp_list in assignee_workload.items():
        if len(wp_list) > overload_threshold:
            risks.append({
                "type": "resource_overload",
                "severity": "high",
                "assignee": assignee,
                "wp_count_this_week": len(wp_list),
                "wp_ids": wp_list[:10],
            })

    # 5. Missing dependencies — phase tasks with no predecessor
    for wp in open_wps:
        wp_type = _wp_type_name(wp).lower()
        if "phase" in wp_type or "milestone" in wp_type:
            has_pred = any(
                _extract_id(r.get("_links", {}).get("to", {}).get("href", "")) == wp["id"]
                and r.get("type") in ("follows", "precedes")
                for r in relations
            ) or any(
                _extract_id(r.get("_links", {}).get("from", {}).get("href", "")) == wp["id"]
                and r.get("type") == "follows"
                for r in relations
            )
            if not has_pred:
                risks.append({
                    "type": "missing_dependency",
                    "severity": "medium",
                    "wp_id": wp["id"],
                    "subject": wp.get("subject"),
                    "wp_type": _wp_type_name(wp),
                })

    # 6. Approaching deadlines — due within N days, not started or <50%
    for wp in open_wps:
        due = _parse_date(wp.get("dueDate"))
        pct = wp.get("percentageDone") or 0
        if due and as_of <= due <= as_of + timedelta(days=approaching_days) and pct < 50:
            risks.append({
                "type": "approaching_deadline",
                "severity": "high" if pct == 0 else "medium",
                "wp_id": wp["id"],
                "subject": wp.get("subject"),
                "due_date": str(due),
                "days_until_due": (due - as_of).days,
                "percent_complete": pct,
                "assignee": _wp_assignee_name(wp),
            })

    # 7. Negative float (CPM) — tasks behind schedule per CPM analysis
    if args.get("include_cpm"):
        try:
            task_graph = _build_task_graph(all_wps, relations)
            cpm = _compute_cpm(task_graph)
            for alert in cpm.get("negative_float_alerts", []):
                wp_id = alert["id"]
                wp_match = next((wp for wp in all_wps if wp["id"] == wp_id), None)
                risks.append({
                    "type": "negative_float",
                    "severity": "critical" if alert["float"] < -5 else "high",
                    "wp_id": wp_id,
                    "subject": alert["name"],
                    "float_days": alert["float"],
                    "assignee": _wp_assignee_name(wp_match) if wp_match else "Unknown",
                })
        except Exception as e:
            logger.warning(f"CPM enrichment failed for risk_scan project {project_id}: {e}")

    # Sort by severity
    severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    risks.sort(key=lambda r: severity_order.get(r.get("severity", "low"), 3))

    return {
        "project_id": project_id,
        "as_of": str(as_of),
        "total_risks": len(risks),
        "by_severity": {
            s: len([r for r in risks if r["severity"] == s])
            for s in ("critical", "high", "medium", "low")
        },
        "risks": risks,
    }


def _get_downstream(
    wp_id: int,
    successor_map: dict[int, list[int]],
    closed_ids: set[int],
    visited: Optional[set] = None,
) -> list[int]:
    """Get all downstream (successor) WP IDs that are not closed."""
    if visited is None:
        visited = set()
    result = []
    for succ_id in successor_map.get(wp_id, []):
        if succ_id not in visited and succ_id not in closed_ids:
            visited.add(succ_id)
            result.append(succ_id)
            result.extend(_get_downstream(succ_id, successor_map, closed_ids, visited))
    return result


def _extract_id(href: str) -> Optional[int]:
    """Extract numeric ID from OP API href."""
    if not href:
        return None
    try:
        return int(href.rstrip("/").split("/")[-1])
    except (ValueError, IndexError):
        return None


# ═══════════════════════════════════════════════════════════════════════
# B4: design_build_status
# ═══════════════════════════════════════════════════════════════════════


async def _design_build_status(client: httpx.AsyncClient, args: dict) -> dict:
    """Track design-build documents by category convention."""
    project_id = args["project_id"]
    report_type = args.get("report_type", "all")
    as_of = _parse_date(args.get("as_of")) or _today()

    all_wps = await _fetch_work_packages(client, project_id, status="all")

    # Categorize WPs by their category name
    categories = {"rfi": [], "submittal": [], "change_order": [], "permit": []}
    category_map = {
        "rfi": "rfi",
        "request for information": "rfi",
        "submittal": "submittal",
        "change order": "change_order",
        "change_order": "change_order",
        "permit": "permit",
    }

    for wp in all_wps:
        cat = _wp_category_name(wp).lower().strip()
        mapped = category_map.get(cat)
        if mapped:
            categories[mapped].append(wp)

    result = {"project_id": project_id, "as_of": str(as_of), "reports": {}}

    if report_type in ("rfi", "all"):
        result["reports"]["rfi"] = _analyze_rfis(categories["rfi"], as_of)

    if report_type in ("submittal", "all"):
        result["reports"]["submittal"] = _analyze_submittals(categories["submittal"], as_of)

    if report_type in ("change_order", "all"):
        result["reports"]["change_order"] = _analyze_change_orders(categories["change_order"], as_of)

    if report_type in ("permit", "all"):
        result["reports"]["permit"] = _analyze_permits(categories["permit"], as_of)

    return result


def _analyze_rfis(wps: list[dict], as_of: date) -> dict:
    """Analyze RFI work packages."""
    total = len(wps)
    closed = [wp for wp in wps if _wp_is_closed(wp)]
    open_rfis = [wp for wp in wps if not _wp_is_closed(wp)]
    overdue = []
    for wp in open_rfis:
        due = _parse_date(wp.get("dueDate"))
        if due and due < as_of:
            overdue.append({
                "id": wp["id"],
                "subject": wp.get("subject"),
                "due_date": str(due),
                "days_overdue": (as_of - due).days,
                "assignee": _wp_assignee_name(wp),
            })

    # Average response time for closed RFIs (created -> updated as proxy)
    response_times = []
    for wp in closed:
        created = _parse_date(wp.get("createdAt"))
        updated = _parse_date(wp.get("updatedAt"))
        if created and updated:
            response_times.append((updated - created).days)

    avg_response = sum(response_times) / len(response_times) if response_times else None

    # Ball-in-court summary
    bic = defaultdict(int)
    for wp in open_rfis:
        bic[_wp_assignee_name(wp)] += 1

    return {
        "total": total,
        "open": len(open_rfis),
        "closed": len(closed),
        "overdue": len(overdue),
        "overdue_items": overdue[:10],
        "avg_response_days": round(avg_response, 1) if avg_response else None,
        "ball_in_court": dict(bic),
    }


def _analyze_submittals(wps: list[dict], as_of: date) -> dict:
    """Analyze submittal work packages."""
    total = len(wps)
    closed = [wp for wp in wps if _wp_is_closed(wp)]
    open_subs = [wp for wp in wps if not _wp_is_closed(wp)]
    overdue = [
        {
            "id": wp["id"],
            "subject": wp.get("subject"),
            "due_date": str(_parse_date(wp.get("dueDate"))),
            "assignee": _wp_assignee_name(wp),
        }
        for wp in open_subs
        if _parse_date(wp.get("dueDate")) and _parse_date(wp.get("dueDate")) < as_of
    ]

    approval_rate = round(len(closed) / total * 100, 1) if total > 0 else 0

    return {
        "total": total,
        "open": len(open_subs),
        "closed": len(closed),
        "overdue": len(overdue),
        "overdue_items": overdue[:10],
        "approval_rate_pct": approval_rate,
    }


def _analyze_change_orders(wps: list[dict], as_of: date) -> dict:
    """Analyze change order work packages."""
    total = len(wps)
    closed = [wp for wp in wps if _wp_is_closed(wp)]
    open_cos = [wp for wp in wps if not _wp_is_closed(wp)]

    # Cost impact from custom fields or description parsing
    cost_impact = 0.0
    for wp in wps:
        # Try customField for cost impact
        ci = wp.get("costImpact") or wp.get("customField7")
        if ci and isinstance(ci, (int, float)):
            cost_impact += ci

    return {
        "total": total,
        "pending": len(open_cos),
        "approved_closed": len(closed),
        "cumulative_cost_impact": round(cost_impact, 2),
    }


def _analyze_permits(wps: list[dict], as_of: date) -> dict:
    """Analyze permit work packages."""
    total = len(wps)
    active = [wp for wp in wps if not _wp_is_closed(wp)]

    # Check for upcoming expirations
    upcoming_expirations = []
    for wp in wps:
        due = _parse_date(wp.get("dueDate"))
        if due and due >= as_of and due <= as_of + timedelta(days=30):
            upcoming_expirations.append({
                "id": wp["id"],
                "subject": wp.get("subject"),
                "expiration": str(due),
                "days_until": (due - as_of).days,
            })

    return {
        "total": total,
        "active": len(active),
        "upcoming_expirations_30d": len(upcoming_expirations),
        "upcoming_expirations": upcoming_expirations,
    }


# ═══════════════════════════════════════════════════════════════════════
# B5: portfolio_rollup
# ═══════════════════════════════════════════════════════════════════════


async def _portfolio_rollup(client: httpx.AsyncClient, args: dict) -> dict:
    """Multi-project portfolio summary."""
    as_of = _parse_date(args.get("as_of")) or _today()
    milestone_days = args.get("milestone_days", 14)

    # Determine which projects to include
    project_ids = args.get("project_ids")
    if project_ids:
        if isinstance(project_ids, str):
            project_ids = json.loads(project_ids)
    else:
        # All active projects
        projects = await _fetch_projects(client, active_only=True)
        project_ids = [p["id"] for p in projects]

    summaries = []
    all_risks = []
    all_milestones = []
    resource_load = defaultdict(lambda: {"total": 0, "projects": set()})

    for pid in project_ids:
        try:
            # Get project info
            project = await _fetch_one(client, f"/projects/{pid}")
            pname = project.get("name", f"Project #{pid}")

            # Health for this project
            health = await _project_health(client, {"project_id": pid, "as_of": str(as_of)})

            summaries.append({
                "project_id": pid,
                "name": pname,
                "health": health.get("health"),
                "completion_pct": health.get("completion_pct"),
                "total_wps": health.get("total_work_packages"),
                "overdue_count": health.get("overdue_count"),
                "velocity": health.get("velocity_per_week"),
            })

            # Aggregate risks (top 5 per project)
            risks = await _risk_scan(client, {"project_id": pid, "as_of": str(as_of)})
            for r in risks.get("risks", [])[:5]:
                r["project"] = pname
                all_risks.append(r)

            # Upcoming milestones
            all_wps = await _fetch_work_packages(client, pid, status="open")
            for wp in all_wps:
                wp_type = _wp_type_name(wp).lower()
                if "milestone" in wp_type:
                    due = _parse_date(wp.get("dueDate") or wp.get("date"))
                    if due and as_of <= due <= as_of + timedelta(days=milestone_days):
                        all_milestones.append({
                            "project": pname,
                            "wp_id": wp["id"],
                            "subject": wp.get("subject"),
                            "date": str(due),
                            "days_until": (due - as_of).days,
                        })

                # Resource utilization
                aid = _wp_assignee_id(wp)
                aname = _wp_assignee_name(wp)
                if aid:
                    resource_load[aname]["total"] += 1
                    resource_load[aname]["projects"].add(pname)

        except Exception as e:
            summaries.append({
                "project_id": pid,
                "name": f"Project #{pid}",
                "error": str(e),
            })

    # Sort milestones by date
    all_milestones.sort(key=lambda m: m["date"])

    # Sort risks by severity
    severity_order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    all_risks.sort(key=lambda r: severity_order.get(r.get("severity", "low"), 3))

    # Format resource load
    resources = [
        {
            "assignee": name,
            "open_wp_count": data["total"],
            "project_count": len(data["projects"]),
            "projects": list(data["projects"]),
        }
        for name, data in sorted(resource_load.items(), key=lambda x: x[1]["total"], reverse=True)
    ]

    return {
        "as_of": str(as_of),
        "project_count": len(project_ids),
        "projects": summaries,
        "cross_project_risks": all_risks[:20],
        "upcoming_milestones": all_milestones[:20],
        "resource_utilization": resources[:20],
    }


# ═══════════════════════════════════════════════════════════════════════
# MCP Tool Definition
# ═══════════════════════════════════════════════════════════════════════

OPERATION_HANDLERS = {
    "project_health": _project_health,
    "earned_value": _earned_value,
    "risk_scan": _risk_scan,
    "design_build_status": _design_build_status,
    "portfolio_rollup": _portfolio_rollup,
}


@mcp.tool()
async def pm_analytics(
    operation: str,
    project_id: Optional[int] = None,
    project_ids: Optional[str] = None,
    as_of: Optional[str] = None,
    hourly_rate: Optional[float] = None,
    thresholds: Optional[str] = None,
    report_type: Optional[str] = None,
    include_cpm: Optional[bool] = None,
    milestone_days: Optional[int] = None,
) -> dict[str, Any]:
    """PM analytics engine: project_health, earned_value, risk_scan, design_build_status, portfolio_rollup.

    Computes project management metrics from OpenProject data.

    Args:
        operation: One of: project_health, earned_value, risk_scan, design_build_status, portfolio_rollup
        project_id: Project ID (required for single-project operations)
        project_ids: JSON array of project IDs (for portfolio_rollup, default: all active)
        as_of: Date for analysis in YYYY-MM-DD format (default: today)
        hourly_rate: Hourly rate for EVM cost calculations (default: 100.0)
        thresholds: JSON object with custom risk thresholds (for risk_scan)
        report_type: For design_build_status: rfi, submittal, change_order, permit, or all
        include_cpm: Enrich with CPM critical path data (project_health adds critical_path/duration; risk_scan adds negative-float risks)
        milestone_days: Look-ahead window in days for milestones (default: 14)
    """
    handler = OPERATION_HANDLERS.get(operation)
    if not handler:
        return {
            "error": f"Unknown operation '{operation}'",
            "available": list(OPERATION_HANDLERS.keys()),
        }

    # Build args dict from all parameters
    args = {"operation": operation}
    if project_id is not None:
        args["project_id"] = project_id
    if project_ids is not None:
        args["project_ids"] = project_ids
    if as_of is not None:
        args["as_of"] = as_of
    if hourly_rate is not None:
        args["hourly_rate"] = hourly_rate
    if thresholds is not None:
        args["thresholds"] = thresholds
    if report_type is not None:
        args["report_type"] = report_type
    if include_cpm is not None:
        args["include_cpm"] = include_cpm
    if milestone_days is not None:
        args["milestone_days"] = milestone_days

    # Validate required params
    if operation in ("project_health", "earned_value", "risk_scan", "design_build_status"):
        if "project_id" not in args:
            return {"error": f"project_id is required for {operation}"}

    async with httpx.AsyncClient(timeout=60) as client:
        result = await handler(client, args)

    return {
        "operation": operation,
        **result,
        "warnings": [],
        "assumptions": ["Using 5-day work week", f"Hourly rate: ${args.get('hourly_rate', 100.0)}"],
    }


if __name__ == "__main__":
    mcp.run()
