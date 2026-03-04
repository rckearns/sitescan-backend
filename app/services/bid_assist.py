"""Bid Assist — uses Claude to analyze an RFQ and generate a tailored bid narrative."""

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


def generate_bid_narrative(org, rfq_text: str) -> str:
    """Call Claude to generate a bid narrative for the given RFQ.

    Returns the narrative as a plain string.
    Raises RuntimeError if anthropic is not installed or API key is missing.
    """
    try:
        import anthropic
    except ImportError:
        raise RuntimeError("anthropic package not installed — add it to requirements.txt")

    settings = get_settings()
    if not settings.anthropic_api_key:
        raise RuntimeError("ANTHROPIC_API_KEY is not configured in environment variables")

    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
    org_context = _build_org_context(org)

    system_prompt = (
        "You are an expert construction bid writer specializing in government and commercial "
        "contracts in South Carolina. Write professional, compelling bid narratives that "
        "highlight a contractor's relevant experience and qualifications. Be specific — "
        "reference actual project names, values, and personnel from the company profile. "
        "Use clear section headers. Keep it concise (500-800 words) unless more is needed."
    )

    user_prompt = (
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

    message = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1500,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )

    logger.info(f"Bid narrative generated for org {org.id} ({org.legal_name})")
    return message.content[0].text
