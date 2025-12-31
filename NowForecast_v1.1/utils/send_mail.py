import json
import os
import smtplib
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from math import ceil

from dotenv import load_dotenv

load_dotenv()


SMTP_HOST = os.environ.get("SMTP_HOST")
SMTP_PORT = os.environ.get("SMTP_PORT")
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASS = os.environ.get("SMTP_PASS")
ALERT_FROM = os.environ.get("ALERT_FROM")
ALERT_TO = os.environ.get("ALERT_TO")
# Convert ALERT_TO to a clean list
ALERT_TO_LIST = json.loads(ALERT_TO)


# ======================================================================
# SEVERITY CONFIG
# ======================================================================

SEVERITY_MAP = {4: "Extreme", 3: "High"}

SEVERITY_TEXT_COLOR = {
    "Extreme": "#b30000",
    "High": "#ff8c00",
}

IST = timezone(timedelta(hours=5, minutes=30))


def classify_hazard(msg):
    if not msg:
        return "Unknown"
    m = msg.lower()
    for k, v in [
        ("fog", "Fog"),
        ("cold wave", "Cold Wave"),
        ("cold day", "Cold Day"),
        ("heat wave", "Heat Wave"),
        ("thunder", "Thunderstorm"),
        ("lightning", "Lightning"),
        ("hail", "Hailstorm"),
        ("rain", "Rainfall"),
        ("snow", "Snowfall"),
        ("dust", "Dust Storm"),
    ]:
        if k in m:
            return v
    return "Other"


def format_validity_duration(vupto):
    """
    IMD Nowcast validity logic (FINAL & CORRECT)

    - Uses CURRENT IST time
    - If vupto <= now → EXPIRED
    - No cross-midnight extension
    """

    try:
        vupto = int(vupto)

        now = datetime.now(IST)
        now_minutes = now.hour * 60 + now.minute

        vh, vm = divmod(vupto, 100)
        vupto_minutes = vh * 60 + vm

        # EXPIRED CASE
        if vupto_minutes <= now_minutes:
            return "Expired"

        remaining_minutes = vupto_minutes - now_minutes
        hours = ceil(remaining_minutes / 60)

        return f"Next {hours} hour" if hours == 1 else f"Next {hours} hours"

    except Exception:
        return "Validity not available"


def build_html(rows):
    grouped = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    )

    for c, d, col, msg, toi, vupto in rows:
        sev = SEVERITY_MAP[col]
        hz = classify_hazard(msg)
        val = format_validity_duration(vupto)
        grouped[c][sev][hz][val].add(d)

    body = ""
    for c in sorted(grouped):
        for sev in ("Extreme", "High"):
            for hz in grouped[c].get(sev, {}):
                for val, dists in grouped[c][sev][hz].items():
                    body += f"""
                    <tr>
                        <td>{c}</td>
                        <td style="color:{SEVERITY_TEXT_COLOR[sev]};font-weight:bold">{sev}</td>
                        <td><b>{hz}</b></td>
                        <td>{val}</td>
                        <td>{", ".join(sorted(dists))}</td>
                    </tr>
                    """

    return f"""
    <html><body style="font-family:Arial">
    <p>Dear Team,</p>
    <p>Based on the <b>nowcast</b>, the following severe weather alerts are identified:</p>
    <table border="1" cellpadding="8" cellspacing="0" width="100%">
      <tr style="background:#eaeaea">
        <th>Indus Circle</th><th>Severity</th><th>Hazard</th>
        <th>Validity</th><th>Affected Districts</th>
      </tr>{body}
    </table>
    <p>This is a system-generated alert for operational awareness.</p>
    <p><b>ML Infomap Weather Intelligence & Disaster Alert System</b></p>
    </body></html>
    """


def send_mail(html, subject_time, logger):
    try:
        logger.info("Preparing alert email")

        msg = MIMEMultipart("alternative")
        msg["From"] = ALERT_FROM

        # ✅ Convert list → comma-separated string for header
        msg["To"] = ", ".join(ALERT_TO_LIST)

        msg["Subject"] = f"Severe Real-Time Weather Alert – Nowcast ({subject_time})"

        msg.attach(MIMEText(html, "html"))

        logger.info("Connecting to SMTP server")
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
            logger.info("EHLO")
            s.ehlo()

            logger.info("Starting TLS")
            s.starttls()

            logger.info("EHLO after TLS")
            s.ehlo()

            logger.info("Logging in")
            s.login(SMTP_USER, SMTP_PASS)

            logger.info("Sending mail")
            response = s.sendmail(ALERT_FROM, ALERT_TO_LIST, msg.as_string())
            logger.info(f"Email is sent to {ALERT_TO_LIST}")

            if response:
                logger.error("Some recipients rejected: %s", response)
            else:
                logger.info("Email accepted for all recipients")

    except Exception:
        logger.exception("Failed to send alert email")
        raise


def send_no_alert_mail(subject_time, logger):
    html = """
    <html>
    <body style="font-family:Arial">
        <p>Dear Team,</p>

        <p>
            Based on the <b>nowcast</b>, no severe weather alerts are identified.
        </p>

        <p>This is a system-generated alert for operational awareness.</p>
        <p><b>ML Infomap Weather Intelligence & Disaster Alert System</b></p>
    </body>
    </html>
    """
    send_mail(html, subject_time, logger)
