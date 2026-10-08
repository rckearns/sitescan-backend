"""Bid Assist — uses Claude to analyze an RFQ and generate a tailored bid narrative."""

import hashlib
import logging

from app.config import get_settings

logger = logging.getLogger("sitescan.bid_assist")


def _build_org_context(org) -> str:
    """Build a concise company profile summary for the Claude prompt."""
    lines = []

    if org.legal_name:
        lines.append(f"Company: {org.legal_name}")
    if org.entity_type:
        lines.append(f"Entity Type: {org.entity_type}")
    if org.address_city or org.address_state:
        lines.append(f"Location: {org.address_city}, {org.address_state}")
    if org.contractor_license_number:
        lines.append(f"SC Contractor License: {org.contractor_license_number}")
    if org.license_classifications:
        lines.append(f"License Classifications: {', '.join(org.license_classifications)}")
    if org.bonding_capacity:
        lines.append(f"Bonding Capacity: {org.bonding_capacity}")
    if org.emr:
        lines.append(f"Experience Modification Rate (EMR): {org.emr}")
    if org.safety_meeting_frequency:
        lines.append(f"Safety Meeting Frequency: {org.safety_meeting_frequency}")

    if org.principals:
        principals = ", ".join(
            f"{p.name} ({p.title})" for p in org.principals if p.name
        )
        if principals:
            lines.append(f"Principals: {principals}")

    if org.project_refs:
        lines.append("\nRelevant Past Projects:")
        for r in org.project_refs[:8]:
            val = f"${float(r.contract_value):,.0f}" if r.contract_value else ""
            scope = f" — {r.scope_of_work[:120]}" if r.scope_of_work else ""
            lines.append(
                f"  • {r.project_name or '(unnamed)'} | Owner: {r.owner_name or 'N/A'}"
                f"{' | ' + val if val else ''}"
                f" | Completed: {r.completion_date or 'N/A'}"
                f"{scope}"
            )

    if org.personnel:
        lines.append("\nKey Personnel:")
        for p in org.personnel:
            role_label = "Project Manager" if p.role == "pm" else "Superintendent"
            lines.append(f"  {role_label}: {p.name}")
            if p.resume_summary:
                lines.append(f"    {p.resume_summary[:300]}")

    return "\n".join(lines)


BID_MODEL = "claude-sonnet-4-6"
BID_MAX_TOKENS = 1500

SYSTEM_PROMPT = (
    "You are an expert construction bid writer specializing in government and commercial "
    "contracts in South Carolina. Write professional, compelling bid narratives that "
    "highlight a contractor's relevant experience and qualifications. Be specific — "
    "reference actual project names, values, and personnel from the company profile. "
    "Use clear section headers. Keep it concise (500-800 words) unless more is needed."
)


def build_user_prompt(org, rfq_text: str) -> str:
    org_context = _build_org_context(org)
    return (
        f"Using the company profile below, write a bid narrative / qualifications statement "
        f"for the following RFQ.\n\n"
        f"COMPANY PROFILE:\n{org_context}\n\n"
        f"RFQ / PROJECT DESCRIPTION:\n{rfq_text[:6000]}\n\n"
        f"Write a compelling bid narrative that:\n"
        f"1. Opens with a strong statement of interest and relevant qualifications\n"
        f"2. Highlights the most relevant past projects from the portfolio\n"
        f"3. Demonstrates key personnel's experience\n"
        f"4. Addresses specific requirements mentioned in the RFQ\n"
        f"5. Closes with a confident statement of capability and readiness\n\n"
        f"Format with clear section headers."
    )


def narrative_cache_key(user_prompt: str) -> str:
    """Same model + prompt (company profile + RFQ text) -> same saved narrative.
    Any profile edit changes the prompt, so it produces a fresh narrative."""
    return hashlib.sha256(f"{BID_MODEL}\n{SYSTEM_PROMPT}\n{user_prompt}".encode()).hexdigest()


async def generate_bid_narrative(user_prompt: str, client=None) -> str:
    """Call Claude for a bid narrative. Uses the async client so a 10-30 s
    generation doesn't block the web server for everyone else.

    Raises RuntimeError if the API key is missing.
    """
    import anthropic

    settings = get_settings()
    if client is None:
        if not settings.anthropic_api_key:
            raise RuntimeError("ANTHROPIC_API_KEY is not configured in environment variables")
        client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)
    message = await client.messages.create(
        model=BID_MODEL,
        max_tokens=BID_MAX_TOKENS,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )
    from app.services.ai_text import response_text
    return response_text(message)
