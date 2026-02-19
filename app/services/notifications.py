"""Notification service — email and SMS alerts for new high-match opportunities."""

import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.database import User, Project, AlertHistory, get_session_factory
from app.config import get_settings

logger = logging.getLogger("sitescan.notifications")


def _format_currency(value):
    if not value:
        return "N/A"
    if value >= 1_000_000:
        return f"${value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"${value / 1_000:.0f}K"
    return f"${value:,.0f}"


def _build_email_html(projects: list, user_name: str) -> str:
    """Build HTML email body for opportunity alerts."""
    rows = ""
    for p in projects[:10]:  # max 10 per email
        status_color = "#34d399" if p.status == "Open" else "#fbbf24"
        rows += f"""
        <tr style="border-bottom: 1px solid #1a1a1f;">
            <td style="padding: 16px 12px;">
                <div style="font-weight: 700; color: #e8e6e1; margin-bottom: 4px;">{p.title}</div>
                <div style="font-size: 13px; color: #888;">📍 {p.location}</div>
                {f'<div style="font-size: 13px; color: #888;">🏢 {p.agency}</div>' if p.agency else ''}
            </td>
            <td style="padding: 16px 12px; text-align: center;">
                <span style="
                    background: #0a2e1a; color: #34d399; padding: 4px 12px;
                    border-radius: 20px; font-size: 13px; font-weight: 700;
                ">{p.match_score}%</span>
            </td>
            <td style="padding: 16px 12px; text-align: right; font-weight: 700; color: #34d399;">
                {_format_currency(p.value)}
            </td>
            <td style="padding: 16px 12px; text-align: center;">
                <span style="color: {status_color}; font-size: 12px; font-weight: 600;">{p.status}</span>
            </td>
            <td style="padding: 16px 12px; text-align: center;">
                <a href="{p.source_url}" style="
                    color: #818cf8; text-decoration: none; font-size: 13px; font-weight: 600;
                ">View →</a>
            </td>
        </tr>
        """
    
    return f"""
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; max-width: 700px; margin: 0 auto; background: #08080a; color: #e8e6e1; padding: 32px;">
        <div style="margin-bottom: 24px;">
            <h1 style="font-size: 24px; margin: 0 0 4px 0; color: #e8e6e1;">
                🔍 SiteScan Alert
            </h1>
            <p style="color: #666; margin: 0; font-size: 14px;">
                {len(projects)} new high-match opportunit{'y' if len(projects) == 1 else 'ies'} found
            </p>
        </div>
        
        <table style="width: 100%; border-collapse: collapse; background: #0c0c0e; border-radius: 12px; overflow: hidden;">
            <thead>
                <tr style="background: #111113;">
                    <th style="padding: 12px; text-align: left; color: #666; font-size: 11px; text-transform: uppercase; letter-spacing: 0.08em;">Project</th>
                    <th style="padding: 12px; text-align: center; color: #666; font-size: 11px; text-transform: uppercase;">Match</th>
                    <th style="padding: 12px; text-align: right; color: #666; font-size: 11px; text-transform: uppercase;">Value</th>
                    <th style="padding: 12px; text-align: center; color: #666; font-size: 11px; text-transform: uppercase;">Status</th>
                    <th style="padding: 12px; text-align: center; color: #666; font-size: 11px; text-transform: uppercase;">Link</th>
                </tr>
            </thead>
            <tbody>{rows}</tbody>
        </table>
        
        <p style="color: #444; font-size: 12px; margin-top: 24px; text-align: center;">
            SiteScan scans SAM.gov, Charleston permits, SCBO, and local bid portals every 6 hours.
            <br>Adjust alert settings in your SiteScan dashboard.
        </p>
    </div>
    """


def _build_sms_body(projects: list) -> str:
    """Build SMS body for opportunity alerts."""
    lines = [f"SiteScan: {len(projects)} new opportunities"]
    for p in projects[:3]:  # max 3 in SMS
        lines.append(f"• {p.title[:60]} ({p.match_score}% match, {_format_currency(p.value)})")
    if len(projects) > 3:
        lines.append(f"+ {len(projects) - 3} more — check SiteScan")
    return "\n".join(lines)


async def send_email_alert(to_email: str, subject: str, html_body: str):
    """Send an email via SMTP."""
    settings = get_settings()
    
    if not settings.smtp_user or not settings.smtp_password:
        logger.warning("SMTP not configured, skipping email alert")
        return False
    
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = settings.notification_from_email
    msg["To"] = to_email
    msg.attach(MIMEText(html_body, "html"))
    
    try:
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as server:
            server.starttls()
            server.login(settings.smtp_user, settings.smtp_password)
            server.sendmail(settings.notification_from_email, to_email, msg.as_string())
        logger.info(f"Email alert sent to {to_email}")
        return True
    except Exception as e:
        logger.error(f"Email send failed: {e}")
        return False


async def send_sms_alert(to_phone: str, body: str):
    """Send an SMS via Twilio."""
    settings = get_settings()
    
    if not settings.twilio_account_sid or not settings.twilio_auth_token:
        logger.warning("Twilio not configured, skipping SMS alert")
        return False
    
    try:
        import httpx
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{settings.twilio_account_sid}/Messages.json",
                auth=(settings.twilio_account_sid, settings.twilio_auth_token),
                data={
                    "From": settings.twilio_from_number,
                    "To": to_phone,
                    "Body": body,
                },
            )
            resp.raise_for_status()
        logger.info(f"SMS alert sent to {to_phone}")
        return True
    except Exception as e:
        logger.error(f"SMS send failed: {e}")
        return False


async def process_alerts():
    """Check for new high-match projects and send alerts to users.
    
    Called after each scan run. Finds projects that:
    1. Were first seen in the last scan cycle
    2. Meet the user's minimum match score
    3. Haven't already been alerted for this user
    """
    settings = get_settings()
    session_factory = get_session_factory()
    
    async with session_factory() as session:
        try:
            # Get all users with alerts enabled
            result = await session.execute(
                select(User).where(
                    (User.email_alerts == True) | (User.sms_alerts == True)
                )
            )
            users = result.scalars().all()
            
            if not users:
                return
            
            # Get new projects from the last scan cycle
            cutoff = datetime.utcnow() - timedelta(hours=settings.scan_cron_hours + 1)
            
            for user in users:
                # Find new projects matching user's criteria
                query = select(Project).where(
                    and_(
                        Project.first_seen >= cutoff,
                        Project.is_active == True,
                        Project.match_score >= user.min_match_score,
                    )
                ).order_by(Project.match_score.desc())
                
                result = await session.execute(query)
                new_projects = result.scalars().all()
                
                if not new_projects:
                    continue
                
                # Filter out already-alerted projects
                already_alerted = set()
                alert_result = await session.execute(
                    select(AlertHistory.project_id).where(
                        AlertHistory.user_id == user.id
                    )
                )
                already_alerted = {row[0] for row in alert_result.all()}
                
                to_alert = [p for p in new_projects if p.id not in already_alerted]
                
                if not to_alert:
                    continue
                
                logger.info(f"Alerting user {user.email}: {len(to_alert)} new opportunities")
                
                # Send email
                if user.email_alerts and user.email:
                    subject = f"SiteScan: {len(to_alert)} new construction opportunit{'y' if len(to_alert) == 1 else 'ies'}"
                    html = _build_email_html(to_alert, user.full_name or user.email)
                    sent = await send_email_alert(user.email, subject, html)
                    
                    if sent:
                        for p in to_alert:
                            session.add(AlertHistory(
                                user_id=user.id,
                                project_id=p.id,
                                alert_type="email",
                            ))
                
                # Send SMS
                if user.sms_alerts and user.phone:
                    sms_body = _build_sms_body(to_alert)
                    sent = await send_sms_alert(user.phone, sms_body)
                    
                    if sent:
                        for p in to_alert:
                            session.add(AlertHistory(
                                user_id=user.id,
                                project_id=p.id,
                                alert_type="sms",
                            ))
                
            await session.commit()
            
        except Exception as e:
            await session.rollback()
            logger.error(f"Alert processing failed: {e}")
