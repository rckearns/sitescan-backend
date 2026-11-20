"""Building-permit sources, in one place.

To add a city: write its scanner in scanners.py, then add one entry here. The contractor
views (By trade, GC leaders, subs) read every source with `contractors=True`; trade-permit
sub matching reads the ones with `trade_permits=True`.

  contractors    its permits feed the contractor views. Turn on once the state's license
                 data is loaded (license_state), or firms whose names carry no trade word
                 fall back to General.
  trade_permits  the source stores trade permits (electrical, plumbing, ...) under those
                 categories, so subs can be matched to jobs on the same parcel.
  parcel_field   raw_data key holding the parcel number, used to group permits by site.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class PermitSource:
    id: str
    city: str
    state: str
    contractors: bool = False
    trade_permits: bool = False
    parcel_field: str | None = None


SOURCES: dict[str, PermitSource] = {s.id: s for s in [
    PermitSource("charleston-permits", "Charleston", "SC", contractors=True, trade_permits=True,
                 parcel_field="MAIN_PARCEL_NUMBER"),
    PermitSource("north-charleston-permits", "North Charleston", "SC", contractors=True),
    PermitSource("mt-pleasant-permits", "Mount Pleasant", "SC", contractors=True),
    PermitSource("charlotte-permits", "Charlotte", "NC"),
    PermitSource("charlotte-land-dev", "Charlotte", "NC"),
]}

ALL_PERMIT_SOURCES = tuple(SOURCES)
CONTRACTOR_SOURCES = tuple(s.id for s in SOURCES.values() if s.contractors)
TRADE_PERMIT_SOURCES = tuple(s.id for s in SOURCES.values() if s.trade_permits)
PARCEL_FIELDS = tuple(dict.fromkeys(s.parcel_field for s in SOURCES.values() if s.parcel_field))
