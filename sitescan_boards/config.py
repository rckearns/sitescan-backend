"""Configuration for Charleston board agenda monitoring.

Charleston runs all board agendas through CivicPlus AgendaCenter:
    https://www.charleston-sc.gov/AgendaCenter

Agenda PDFs live at predictable URLs:
    /AgendaCenter/ViewFile/Agenda/_MMDDYYYY-NNNN

Discovery works two ways (poller tries both):
  1. Per-category RSS feeds (CivicPlus standard):  /AgendaCenter/RSS
  2. Scraping the AgendaCenter index HTML and matching section headers.
"""

BASE_URL = "https://www.charleston-sc.gov"
AGENDA_CENTER_URL = f"{BASE_URL}/AgendaCenter"

# Board name patterns (matched case-insensitively against AgendaCenter
# section headers / RSS category titles) -> canonical board code.
# Ordered by commercial signal value.
BOARDS = {
    "PC": {
        "name": "Planning Commission",
        "patterns": [r"planning\s+commission"],
        "signal_weight": 1.0,   # earliest signal: rezonings, PUDs, concept plans
    },
    "BAR-L": {
        "name": "Board of Architectural Review - Large",
        "patterns": [r"architectural\s+review.*large", r"\bBAR-?L\b"],
        "signal_weight": 0.9,
    },
    "TRC": {
        "name": "Technical Review Committee",
        "patterns": [r"technical\s+review"],
        "signal_weight": 0.85,  # commercial site plans, closest to permit stage
    },
    "DRB": {
        "name": "Design Review Board",
        "patterns": [r"design\s+review\s+board"],
        "signal_weight": 0.8,
    },
    "BAR-S": {
        "name": "Board of Architectural Review - Small",
        "patterns": [r"architectural\s+review.*small", r"\bBAR-?S\b"],
        "signal_weight": 0.2,   # mostly residential renovation noise
    },
}

# Exclude public-comment compilations etc. -- we only want agendas.
EXCLUDE_TITLE_PATTERNS = [
    r"public\s+comment", r"minutes", r"results",
    # "BAR-L Agenda (Image Overview)" is a large slide-deck PDF of the same
    # items; parsing it duplicates or garbles the real agenda.
    r"image\s+overview",
    # PC "Meeting Report" repeats the agenda items with vote tallies (and
    # unlettered section headers the item splitter can't see); drafts and
    # cancellation notices have no items.
    r"meeting\s+report", r"\bdraft\b", r"cancell?ation",
]

# AgendaCenter category IDs (from the changeYear(year, catID) links on
# /AgendaCenter), used to list a past year via POST UpdateCategoryList.
# BAR-L and BAR-S share the "Board of Architectural Review" category; the
# link text ("BAR-L Agenda") tells them apart.
CATEGORY_IDS = {
    "Board of Architectural Review": 1,
    "Design Review Board": 4,
    "Planning Commission": 5,
    "Technical Review Committee": 8,
}

HTTP_TIMEOUT = 30
USER_AGENT = (
    "SiteScan/1.0 (construction opportunity monitoring; contact via site)"
)
# AgendaCenter is a plain CivicPlus site -- no anti-bot layer observed,
# so no proxy needed. Be a polite citizen anyway:
REQUEST_DELAY_SECONDS = 2

# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

# Commercial / mixed-use zoning codes seen in Charleston rezoning requests.
COMMERCIAL_ZONING_CODES = {
    "GB", "LB", "JC", "CT", "MU-1", "MU-2", "MU-3", "MU-1/WH", "MU-2/WH",
    "MU-3/WH", "UP", "LI", "HI", "PUD",
}

# Keyword -> score contribution. Tuned so that >= 50 is "worth a look"
# and >= 75 is "high priority". Max raw score is capped at 100.
KEYWORD_SCORES = {
    r"mixed[\s-]?use": 30,
    r"new construction": 25,
    r"hotel|hospitality": 30,
    r"apartment|multifamily|multi-family": 25,
    r"retail|restaurant|office": 20,
    r"rezon": 15,
    r"\bPUD\b|planned unit development": 20,
    r"concept plan": 15,
    r"workforce housing|affordable housing": 20,
    r"medical|hospital|clinic": 20,
    r"warehouse|industrial|distribution": 20,
    r"subdivision": 10,
    r"demolition": 10,   # full demos often precede redevelopment
    r"single[\s-]?family": -25,  # residential noise
    r"piazza|fenestration|porch|window|door replacement": -20,
}

# Approval stage -> lifecycle position. "final" = bidding window opening.
STAGE_KEYWORDS = {
    "conceptual": r"conceptual\s+approval",
    "preliminary": r"preliminary\s+approval",
    "final": r"final\s+approval",
    "demolition": r"\bdemolition\b",
    "rezoning": r"\brezon",
    "concept_plan": r"concept\s+plan",
}

# Known commercial architecture / engineering firms -- an item with one of
# these as applicant is almost never residential noise. Also feeds the
# relationship-targeting list.
KNOWN_FIRMS = [
    "LS3P", "McMillan Pazdan Smith", "Liollio", "Kimley-Horn",
    "SeamonWhiteside", "Thomas & Hutton", "ADC Engineering",
    "Goff D'Antonio", "Cline Design", "Bello Garris", "SGA NarmourWright",
    "Stantec", "HDR", "Forsberg", "Davis & Floyd",
]

# ---------------------------------------------------------------------------
# Institutional work (universities, hospitals, schools)
# ---------------------------------------------------------------------------
# The developer keywords above miss most institutional projects: a College of
# Charleston student-housing tower reads "New Construction ... student housing
# building ... Owner: College of Charleston" and scored in the 20s-40s.

# Institutional building types. Only the single best match counts, so
# "university hospital clinic" doesn't stack three times.
INSTITUTIONAL_USE_SCORES = {
    r"student\s+housing|residence\s+hall|dormitor": 30,
    r"hospital|medical\s+office|\bclinic\b": 25,
    r"academic|laborator|classroom": 20,
    r"parking\s+(?:garage|deck|structure)": 20,
    r"\bschool\b|\buniversity\b": 10,
}

# Institutional owners (matched against owner, applicant and request text).
INSTITUTIONAL_OWNERS = {
    "College of Charleston": r"college\s+of\s+charleston|\bCofC\b",
    "MUSC": r"\bMUSC\b|medical\s+university",
    "The Citadel": r"\bthe\s+citadel\b",
    "Trident Technical College": r"trident\s+tech",
    "Charleston County School District": r"charleston\s+county\s+school|\bCCSD\b",
    "SC State Ports Authority": r"ports\s+authority",
}
INSTITUTIONAL_OWNER_SCORE = 20

# "New Construction" together with a height district / stories means a
# full-size building rather than an accessory structure.
NEW_CONSTRUCTION_HEIGHT_RE = (
    r"new\s+construction[\s\S]*?(?:height\s+district|stor(?:y|ies))"
)
NEW_CONSTRUCTION_HEIGHT_SCORE = 10

# An institutional owner doing new construction or a full demolition is a
# redevelopment signal (e.g. CofC demolishing the YWCA for Project 205).
INSTITUTIONAL_REDEVELOPMENT_RE = r"new\s+construction|full\s+demolition"
INSTITUTIONAL_REDEVELOPMENT_SCORE = 15

# Requests that are minor scope even on a new building.
MINOR_SCOPE_RE = (
    r"mock[\s-]?up|signage|\bsigns?\b|lighting|mural|fenestration|"
    r"window|lantern|awning|storefront"
)

ACREAGE_SCORE_THRESHOLD = 1.0   # parcels >= 1 ac get a bump
ACREAGE_SCORE = 15
KNOWN_FIRM_SCORE = 20
