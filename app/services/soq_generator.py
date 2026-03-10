"""SOQ document generator — fills SC OSE Statement of Qualifications template."""

import logging
import re
import zipfile
from io import BytesIO
from pathlib import Path

logger = logging.getLogger("sitescan.soq")

TEMPLATE_PATH = Path(__file__).parent.parent / "templates" / "soq_template.docx"


def _patch_template_rels(template_path: Path) -> BytesIO:
    """Return a patched copy of the docx with mailto: URLs sanitized.

    docxtpl processes every XML file (including */_rels/*.rels) as a Jinja2
    template. The '@' in 'mailto:user@example.com' is an invalid Jinja2 token
    and triggers TemplateSyntaxError. We strip the address from relationship
    targets before rendering so Jinja2 never sees the '@'.
    """
    buf = BytesIO()
    with zipfile.ZipFile(template_path, "r") as zin:
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
            for item in zin.infolist():
                data = zin.read(item.filename)
                if item.filename.endswith(".rels"):
                    text = data.decode("utf-8")
                    # Replace any mailto: hyperlink target that contains '@'
                    text = re.sub(r'Target="mailto:[^"]*@[^"]*"', 'Target="mailto:"', text)
                    data = text.encode("utf-8")
                zout.writestr(item, data)
    buf.seek(0)
    return buf


def _fmt_value(v) -> str:
    """Format a float dollar value, e.g. 1500000 → '$1,500,000'."""
    if not v:
        return ""
    try:
        return f"${float(v):,.0f}"
    except (TypeError, ValueError):
        return str(v)


def _proj_ctx(p) -> dict:
    """Convert a ProjectReference ORM object into a template context dict."""
    return {
        "name": p.project_name,
        "owner": p.owner_name,
        "contact": p.owner_contact,
        "phone": p.owner_phone,
        "value": _fmt_value(p.contract_value),
        "completed": p.completion_date,
        "scope": p.scope_of_work,
        "role": p.your_role,
        "description": p.description,
    }


def generate_soq(org, pm, superintendent, general_projects, state_projects) -> bytes:
    """Render the SOQ docxtpl template with org/personnel data and return raw bytes.

    Raises FileNotFoundError if the template hasn't been placed in app/templates/.
    Raises any docxtpl rendering errors as-is so the router can return a 500.
    """
    try:
        from docxtpl import DocxTemplate
    except ImportError as exc:
        raise RuntimeError("docxtpl is not installed — add it to requirements.txt") from exc

    if not TEMPLATE_PATH.exists():
        raise FileNotFoundError(
            f"SOQ template not found at {TEMPLATE_PATH}. "
            "Copy soq_template.docx to app/templates/ and add {{ }} placeholders."
        )

    patched = _patch_template_rels(TEMPLATE_PATH)
    tpl = DocxTemplate(patched)

    context = {
        # Part I — Contractor info
        "contractor_name": org.legal_name,
        "entity_type": org.entity_type,
        "address": (
            f"{org.address_street}, {org.address_city}, "
            f"{org.address_state} {org.address_zip}"
        ).strip(", "),
        "address_street": org.address_street,
        "address_city": org.address_city,
        "address_state": org.address_state,
        "address_zip": org.address_zip,
        "phone": org.phone,
        "fax": org.fax,
        "email": org.email,
        "website": org.website,
        "license_number": org.contractor_license_number,
        "classifications": ", ".join(org.license_classifications or []),

        # Part II — Organization
        "principals": [
            {
                "name": p.name,
                "title": p.title,
                "other": p.other_businesses,
            }
            for p in (org.principals or [])
        ],

        # Part III-A — Project references
        "general_projects": [_proj_ctx(p) for p in (general_projects or [])],
        "state_projects": [_proj_ctx(p) for p in (state_projects or [])],

        # Part III-B — Key personnel
        "pm_name": pm.name if pm else "",
        "pm_resume": pm.resume_summary if pm else "",
        "pm_projects": pm.projects if pm else [],
        "super_name": superintendent.name if superintendent else "",
        "super_resume": superintendent.resume_summary if superintendent else "",
        "super_projects": superintendent.projects if superintendent else [],

        # Part III-C — Insurance & bonding
        "insurance_company": org.insurance_company,
        "insurance_agent": org.insurance_agent_name,
        "insurance_phone": org.insurance_agent_phone,
        "bonding_company": org.bonding_company,
        "bonding_agent": org.bonding_agent_name,
        "bonding_phone": org.bonding_agent_phone,
        "bonding_capacity": org.bonding_capacity,

        # Part III — Safety
        "emr": org.emr,
        "safety_frequency": org.safety_meeting_frequency,

        # Part III-D — Compliance flags (dict of {key: {value, explanation}})
        "compliance": org.compliance_flags or {},
    }

    tpl.render(context)
    buf = BytesIO()
    tpl.save(buf)
    logger.info(f"SOQ generated for org {org.id} ({org.legal_name})")
    return buf.getvalue()
