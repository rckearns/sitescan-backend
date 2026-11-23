"""Building-permit sources and the markets they belong to, in one place.

To add a city: write its scanner in scanners.py, then add one entry here (and a Market if
it's a new metro area). The contractor
views (By trade, GC leaders, subs) read every source with `contractors=True`; trade-permit
sub matching reads the ones with `trade_permits=True`.

  contractors    its permits feed the contractor views. Turn on once the state's license
                 data is loaded (license_state), or firms whose names carry no trade word
                 fall back to General.
  trade_permits  the source stores trade permits (electrical, plumbing, ...) under those
                 categories, so subs can be matched to jobs on the same parcel.
  parcel_field   raw_data key holding the parcel number, used to group permits by site.
  type_fields    raw_data keys whose text says whether a permit is residential ("Single
                 Family/Duplex Dwelling", "Residential Standalone"); see is_residential_permit.

A Market is a metro area. A sub whose business address is in one of its counties is local
there; its center steers the contact lookup toward the right business.
"""
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class Market:
    id: str
    name: str
    state: str
    counties: tuple[str, ...]          # "Charleston County, SC" (county as Google writes it)
    center: tuple[float, float]        # lat, lng
    license_cities: tuple[str, ...] = ()   # cities the state license list is scraped for here

    def is_local(self, state: str | None, county: str | None) -> bool | None:
        """None when the sub's address is unknown."""
        if not state or not county:
            return None
        return f"{county}, {state}".lower() in {c.lower() for c in self.counties}


MARKETS: dict[str, Market] = {m.id: m for m in [
    Market("charleston-sc", "Charleston", "SC",
           ("Charleston County, SC", "Berkeley County, SC", "Dorchester County, SC"), (32.7765, -79.9311),
           license_cities=("Charleston", "North Charleston", "Mount Pleasant", "Summerville", "Ladson",
                           "Goose Creek", "Hanahan", "Johns Island", "James Island", "West Ashley",
                           "Moncks Corner", "Ridgeville", "Walterboro")),
    Market("charlotte-nc", "Charlotte", "NC",
           ("Mecklenburg County, NC", "Cabarrus County, NC", "Gaston County, NC", "Union County, NC",
            "Iredell County, NC", "York County, SC", "Lancaster County, SC"), (35.2271, -80.8431)),
]}


@dataclass(frozen=True)
class PermitSource:
    id: str
    city: str
    state: str
    market: str
    contractors: bool = False
    trade_permits: bool = False
    parcel_field: str | None = None
    type_fields: tuple[str, ...] = ()


SOURCES: dict[str, PermitSource] = {s.id: s for s in [
    PermitSource("charleston-permits", "Charleston", "SC", "charleston-sc", contractors=True, trade_permits=True,
                 parcel_field="MAIN_PARCEL_NUMBER", type_fields=("PERMIT_TYPE", "WORK_CLASS")),
    PermitSource("north-charleston-permits", "North Charleston", "SC", "charleston-sc", contractors=True),
    PermitSource("mt-pleasant-permits", "Mount Pleasant", "SC", "charleston-sc", contractors=True),
    PermitSource("charlotte-permits", "Charlotte", "NC", "charlotte-nc"),
    PermitSource("charlotte-land-dev", "Charlotte", "NC", "charlotte-nc"),
]}

ALL_PERMIT_SOURCES = tuple(SOURCES)
CONTRACTOR_SOURCES = tuple(s.id for s in SOURCES.values() if s.contractors)
TRADE_PERMIT_SOURCES = tuple(s.id for s in SOURCES.values() if s.trade_permits)
PARCEL_FIELDS = tuple(dict.fromkeys(s.parcel_field for s in SOURCES.values() if s.parcel_field))


def market_for(source_id: str | None) -> Market | None:
    src = SOURCES.get(source_id or "")
    return MARKETS.get(src.market) if src else None


_RESIDENTIAL_RE = re.compile(r"single\s*family|duplex|\bresidential\b|townho", re.IGNORECASE)


def is_residential_permit(p) -> bool:
    """A house-scale permit: category "residential", or the source's permit-type fields say
    so (Charleston: "Electrical - Single Family/Duplex Dwelling", "Plumbing | Residential
    Standalone"). Multi-family stays commercial."""
    if getattr(p, "category", None) == "residential":
        return True
    src = SOURCES.get(getattr(p, "source_id", "") or "")
    raw = getattr(p, "raw_data", None)
    if not src or not src.type_fields or not isinstance(raw, dict):
        return False
    return any(_RESIDENTIAL_RE.search(str(raw.get(f) or "")) for f in src.type_fields)
