"""Match pipeline projects against a user's GC preferences.

Each project gets a status:
- "match": every preference is satisfied by known facts
- "unconfirmed": nothing rules it out, but something the profile needs is unknown
- "excluded": a known fact conflicts with the profile
plus human-readable reasons for whatever isn't a clean match.
"""

from typing import Optional

DEFAULT_DELIVERY = ["cmr", "design-build", "qualifications"]
DELIVERY_LABELS = {
    "cmr": "CM at Risk", "design-build": "design-build",
    "qualifications": "qualifications-based", "design-bid-build": "hard bid",
}


def _fmt_money(v: float) -> str:
    return f"${v / 1e6:.1f}M" if v >= 1e6 else f"${v / 1e3:.0f}K"


def match_project(
    *,
    delivery_method: str,
    construction_type: str,
    estimate: Optional[float],
    building_type: str,
    in_charleston_area: Optional[bool],
    delivery_methods: Optional[list] = None,
    exclude_wood_frame: bool = True,
    min_value: Optional[float] = 1_000_000,
    project_types: Optional[list] = None,
) -> tuple:
    wanted = delivery_methods if delivery_methods is not None else DEFAULT_DELIVERY
    excluded, unknown = [], []

    if in_charleston_area is False:
        excluded.append("Outside the Charleston area")

    if not delivery_method:
        unknown.append("Delivery method not stated yet")
    elif wanted and delivery_method not in wanted:
        excluded.append(f"Delivery is {DELIVERY_LABELS.get(delivery_method, delivery_method)}")

    if exclude_wood_frame:
        if construction_type == "wood":
            excluded.append("Likely wood frame")
        elif construction_type != "non-wood":
            unknown.append("Construction type not confirmed")

    if min_value:
        if estimate is None:
            unknown.append("No budget published")
        elif estimate < min_value:
            excluded.append(f"Budget {_fmt_money(estimate)} is under {_fmt_money(min_value)}")

    if project_types:
        if not building_type or building_type == "other":
            unknown.append("Project type unclear")
        elif building_type not in project_types:
            excluded.append(f"Project type is {building_type}")

    if excluded:
        return "excluded", excluded + unknown
    if unknown:
        return "unconfirmed", unknown
    return "match", []


def match_for_user(project, user) -> tuple:
    stages = {e.stage for e in (getattr(project, "events", None) or [])}
    if stages and stages <= {"land"}:
        return "excluded", ["Land purchase only (no construction yet)"]
    return match_project(
        delivery_method=project.delivery_method or "",
        construction_type=project.construction_type or "unknown",
        estimate=project.estimate,
        building_type=project.building_type or "",
        in_charleston_area=project.in_charleston_area,
        delivery_methods=user.gc_delivery_methods if user.gc_delivery_methods is not None else None,
        exclude_wood_frame=True if user.gc_exclude_wood_frame is None else user.gc_exclude_wood_frame,
        min_value=1_000_000 if user.gc_min_value is None else user.gc_min_value,
        project_types=user.gc_project_types or [],
    )
