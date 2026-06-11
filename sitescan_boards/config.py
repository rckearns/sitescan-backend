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
EXCLUDE_TITLE_PATTERNS = [r"public\s+comment", r"minutes", r"results"]

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

ACREAGE_SCORE_THRESHOLD = 1.0   # parcels >= 1 ac get a bump
ACREAGE_SCORE = 15
KNOWN_FIRM_SCORE = 20
