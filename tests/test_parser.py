"""Parser + classifier tests against real agenda text.

Fixtures are verbatim-structure samples from actual Charleston agendas
(BAR-L Jan 14 2026, PC Feb 18 2026, BAR-S Nov 13 2025). Run:
    python3 -m pytest tests/  (or)  python3 tests/test_parser.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sitescan_boards.classifier import classify
from sitescan_boards.parser import parse_agenda_text

BAR_L_SAMPLE = """\
BOARD OF ARCHITECTURAL REVIEW – LARGE
AGENDA
JANUARY 14, 2026
A meeting of the Board of Architectural Review – Large (BAR-L) will be held on
Wednesday, January 14, 2026, at 4:30 p.m. in the Public Meeting Room.
B. APPLICATIONS
3. 474 Meeting Street
BAR2025-002335 | TMS #459-05-03-071 | Eastside | Council District 4
New Construction | Height Districts 2.5-3 | Height District 4
Old and Historic District, Old City District
Requesting Conceptual Approval for a mixed-use, affordable housing building.
Owner: Star Gospel Mission
Applicant: Bryanna Dering-Weyrich (LS3P)
Board of Architectural Review – Large
Agenda | January 14, 2026
Page 2
4. 162 Ashley Avenue
BAR2025-002334 | TMS #460-15-04-061 | Medical Complex | Council District 6
New Construction | Height District 85/30 | Old City District
Requesting Preliminary Approval of an apartment building with ground-level
retail.
Owner: Evan Marko & John Marko
Applicant: Jeffrey Rengering
"""

PC_SAMPLE = """\
PLANNING COMMISSION
AGENDA
FEBRUARY 18, 2026
B. REZONINGS
1. 1234 Sample Rd
West Ashley | TMS# 3500100080 | Council District 7 | Approx. 0.513 ac.
Request to rezone from Single Family Residential (SR-2) to Job Center (JC).
Owner: John J. Tecklenburg
Applicant: John J. Tecklenburg
2. Harris St & Jackson St
Peninsula | TMS# 4611301035 | Council District 4 | Approx. 2.41 ac.
Request to rezone from Upper Peninsula (UP) & Diverse Residential (DR-2) to
Mixed-Use/Workforce Housing (MU-3/WH).
Owner: Sample Owner LLC
Applicant: Kimley-Horn & Associates, Inc.
"""

BAR_S_SAMPLE = """\
BOARD OF ARCHITECTURAL REVIEW – SMALL
AGENDA
NOVEMBER 13, 2025
B. APPLICATIONS
4. 34 Smith Street
BAR2025-002213 | TMS# 457-03-04-042 | Harleston Village | Council District 8
Category 2 | c. 1855 | Old & Historic District
Requesting conceptual approval for piazza restoration, exterior repair work,
and changes to fenestration.
Owner: Shea & John Kuhn
Applicant: Laura F. Altman – LFA Architecture LLC
Site visit on 11/13/2025 at 9:30 a.m.
"""


def test_bar_l():
    items = parse_agenda_text(BAR_L_SAMPLE)
    assert len(items) == 2, f"expected 2 items, got {len(items)}"

    a = items[0]
    assert a.item_number == 3
    assert a.address == "474 Meeting Street"
    assert a.case_number == "BAR2025-002335"
    assert a.tms == "459-05-03-071"
    assert a.neighborhood == "Eastside"
    assert a.council_district == 4
    assert "mixed-use" in a.request_text.lower()
    assert a.owner == "Star Gospel Mission"
    assert "LS3P" in a.applicant

    c = classify(a, "BAR-L")
    assert c.stage == "conceptual"
    assert c.score >= 50, f"mixed-use new construction scored only {c.score}"

    b = items[1]
    assert b.case_number == "BAR2025-002334"
    assert b.council_district == 6
    c2 = classify(b, "BAR-L")
    assert c2.stage == "preliminary"
    assert c2.score >= 50


def test_pc_rezonings():
    items = parse_agenda_text(PC_SAMPLE)
    assert len(items) == 2, f"expected 2 items, got {len(items)}"

    a = items[0]
    assert a.section == "REZONINGS"
    assert a.tms == "3500100080"
    assert a.acreage == 0.513
    assert "JC" in a.rezoning_to
    ca = classify(a, "PC")
    assert ca.stage == "rezoning"
    assert ca.score >= 25  # commercial rezone target

    b = items[1]
    assert b.acreage == 2.41
    assert b.neighborhood == "Peninsula"
    assert "MU-3/WH" in b.rezoning_to
    cb = classify(b, "PC")
    assert cb.score >= 60, f"large mixed-use rezone scored only {cb.score}"
    assert any(t.startswith("firm:Kimley-Horn") for t in cb.tags)


def test_bar_s_noise_scores_low():
    items = parse_agenda_text(BAR_S_SAMPLE)
    assert len(items) == 1
    a = items[0]
    assert a.case_number == "BAR2025-002213"
    assert a.owner == "Shea & John Kuhn"
    assert "Site visit" not in (a.applicant or "")
    c = classify(a, "BAR-S")
    assert c.score < 25, f"piazza restoration scored {c.score}, too high"


if __name__ == "__main__":
    test_bar_l()
    test_pc_rezonings()
    test_bar_s_noise_scores_low()
    print("All parser/classifier tests passed.")
