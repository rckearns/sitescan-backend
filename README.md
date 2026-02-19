# SiteScan Backend

Construction project opportunity intelligence API. Automatically scans multiple data sources for masonry, historic restoration, structural, and general construction opportunities in the Charleston, SC area.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                    FastAPI Application                     │
│                                                           │
│  ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌───────────┐ │
│  │  Auth    │  │ Projects │  │   Scan   │  │ Scheduler │ │
│  │  Router  │  │  Router  │  │  Router  │  │(APSchedul)│ │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └─────┬─────┘ │
│       │             │             │               │       │
│  ┌────┴─────────────┴─────────────┴───────────────┴─────┐ │
│  │                   Service Layer                       │ │
│  │  ┌────────────┐ ┌────────────┐ ┌───────────────────┐ │ │
│  │  │  Scanners  │ │  Scoring   │ │  Notifications    │ │ │
│  │  │            │ │  Engine    │ │  (Email + SMS)    │ │ │
│  │  └──────┬─────┘ └────────────┘ └───────────────────┘ │ │
│  └─────────┼────────────────────────────────────────────┘ │
│            │                                              │
│  ┌─────────┴────────────────────────────────────────────┐ │
│  │               Data Sources                            │ │
│  │  SAM.gov API │ Charleston ArcGIS │ SCBO │ City Bids  │ │
│  └──────────────────────────────────────────────────────┘ │
│                           │                               │
│              ┌────────────┴────────────┐                  │
│              │   SQLite / PostgreSQL   │                  │
│              └─────────────────────────┘                  │
└──────────────────────────────────────────────────────────┘
```

## Data Sources

| Source | Type | Cost | Key Required | Notes |
|--------|------|------|-------------|-------|
| SAM.gov | REST API | Free | Yes (free) | Federal contract opportunities. Register at sam.gov |
| Charleston Permits | ArcGIS REST | Free | No | Active building permits from City of Charleston |
| SCBO | Web scrape | Free | No | SC Business Opportunities — state solicitations |
| Charleston City Bids | Web scrape | Free | No | City procurement postings |
| ConstructConnect | REST API | $129+/mo | Yes (paid) | 500K+ private project leads (optional) |

## Quick Start

### 1. Clone and install

```bash
cd sitescan-backend
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your values — at minimum:
#   SECRET_KEY=<random-64-char-string>
#   SAM_GOV_API_KEY=<your-key-from-sam.gov>
```

### 3. Run the server

```bash
uvicorn app.main:app --reload --port 8000
```

The API will be live at `http://localhost:8000` with interactive docs at `/docs`.

### 4. Register and start scanning

```bash
# Create account
curl -X POST http://localhost:8000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email": "ryan@example.com", "password": "yourpassword", "full_name": "Ryan", "company": "Your Company"}'

# Save the access_token from the response, then:

# Trigger a scan
curl -X POST http://localhost:8000/api/v1/scan/trigger \
  -H "Authorization: Bearer YOUR_TOKEN"

# View results
curl http://localhost:8000/api/v1/projects?min_match=70&sort_by=match_score \
  -H "Authorization: Bearer YOUR_TOKEN"
```

## API Endpoints

### Auth
- `POST /api/v1/auth/register` — Create account
- `POST /api/v1/auth/login` — Get JWT token
- `GET /api/v1/auth/me` — Get profile
- `PATCH /api/v1/auth/me` — Update profile/preferences

### Projects
- `GET /api/v1/projects` — List with filters (categories, sources, min_match, search, sort)
- `GET /api/v1/projects/{id}` — Single project detail
- `GET /api/v1/projects/stats/summary` — Pipeline stats
- `POST /api/v1/projects/save` — Bookmark a project
- `GET /api/v1/projects/saved/list` — List saved projects
- `DELETE /api/v1/projects/saved/{id}` — Remove bookmark

### Scan
- `POST /api/v1/scan/trigger` — Manual scan (all or specific sources)
- `GET /api/v1/scan/history` — View scan logs
- `GET /api/v1/scan/sources` — List available sources

### System
- `GET /health` — Health check
- `GET /docs` — Swagger UI

## Automated Scanning

The backend runs an APScheduler job every 6 hours (configurable via `SCAN_CRON_HOURS`):

1. Queries all enabled sources
2. Deduplicates by source + external ID
3. Upserts into the database
4. Scores each project against your profile
5. Sends email/SMS alerts for new high-match opportunities

## Scoring Engine

Projects are scored 0-99 based on keyword relevance:

- **90+**: Direct match to core specialties (historic masonry, lime mortar, facade restoration)
- **75-89**: Strong match (structural reinforcement, foundation repair, masonry)
- **60-74**: Moderate match (general construction, renovation, commercial)
- **Below 60**: Weak match (tangentially related)

Location bonuses for Charleston metro area. Penalties for irrelevant trades (IT, landscaping, etc.).

## Deployment

### Docker

```bash
docker build -t sitescan-api .
docker run -p 8000:8000 --env-file .env sitescan-api
```

### Railway / Render

1. Push to GitHub
2. Connect to Railway or Render
3. Set environment variables from `.env.example`
4. Deploy — auto-detects Dockerfile

### Production Checklist

- [ ] Change `SECRET_KEY` to a random 64-character string
- [ ] Set `APP_ENV=production`
- [ ] Switch `DATABASE_URL` to PostgreSQL: `postgresql+asyncpg://user:pass@host/sitescan`
- [ ] Add `asyncpg` to requirements.txt for Postgres
- [ ] Configure SMTP for email alerts
- [ ] (Optional) Configure Twilio for SMS alerts
- [ ] Set up a reverse proxy (nginx/Caddy) with HTTPS
- [ ] Add rate limiting at the proxy level

## File Structure

```
sitescan-backend/
├── app/
│   ├── main.py              # FastAPI app + scheduler setup
│   ├── config.py            # Settings from environment
│   ├── auth.py              # JWT + password utilities
│   ├── models/
│   │   ├── database.py      # SQLAlchemy models + engine
│   │   └── schemas.py       # Pydantic request/response schemas
│   ├── routers/
│   │   ├── auth.py          # Auth endpoints
│   │   ├── projects.py      # Project CRUD + filtering
│   │   └── scan.py          # Scan trigger + history
│   └── services/
│       ├── scanners.py      # Individual source scanners
│       ├── scoring.py       # Classification + match scoring
│       ├── orchestrator.py  # Scan coordination + DB upsert
│       └── notifications.py # Email + SMS alerts
├── .env.example
├── Dockerfile
├── requirements.txt
└── README.md
```
