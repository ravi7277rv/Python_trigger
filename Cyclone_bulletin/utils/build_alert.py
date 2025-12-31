import smtplib
from email.message import EmailMessage


def build_alert_message(pdf_ts_ist, bay_tokens, arab_tokens, bay_dates, arab_dates):
    non_nil = []
    for i, tok in enumerate(bay_tokens):
        if tok != "NIL":
            non_nil.append(
                ("Bay of Bengal", f"{(i*24)}-{(i+1)*24}h", tok, bay_dates[i])
            )
    for i, tok in enumerate(arab_tokens):
        if tok != "NIL":
            non_nil.append(("Arabian Sea", f"{(i*24)}-{(i+1)*24}h", tok, arab_dates[i]))
    if not non_nil:
        subject = f"[IMD] Cyclone Bulletin: No cyclogenesis (NIL) — {pdf_ts_ist.strftime('%d-%b-%Y %I:%M %p IST')}"
        body = f"No cyclogenesis probability reported (all NIL) in the latest IMD bulletin.\nBulletin timestamp (IST): {pdf_ts_ist.strftime('%Y-%m-%d %I:%M %p')}"
        return subject, body

    first = non_nil[0]
    subject = f"[CYCLONE ALERT] {first[0]} {first[1]} => {first[2]} ({pdf_ts_ist.strftime('%d-%b-%Y %I:%M %p IST')})"
    body_lines = [
        "Cyclogenesis probability detected in latest IMD bulletin:",
        f"Bulletin timestamp (IST): {pdf_ts_ist.strftime('%Y-%m-%d %I:%M %p')}",
        "",
        "Detected non-NIL probabilities:",
    ]
    for sea, window, tok, date in non_nil:
        body_lines.append(f"- {sea} | {window} | Date: {date} | Value: {tok}")
    body_lines.append("")
    body_lines.append("Please review the IMD bulletin and take necessary action.")
    return subject, "\n".join(body_lines)


def send_alert_email(subject, body, smtp_conf, logger):
    to_mail = ", ".join(smtp_conf["to_list"])
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_conf["from"]
    msg["To"] = to_mail
    msg.set_content(body)
    try:
        logger.info(
            "Connecting to SMTP host %s:%s", smtp_conf["host"], smtp_conf["port"]
        )
        server = smtplib.SMTP(smtp_conf["host"], smtp_conf["port"], timeout=30)
        server.ehlo()
        server.starttls()
        server.login(smtp_conf["user"], smtp_conf["pass"])
        server.send_message(msg)
        server.quit()
        logger.info("Alert email sent to %s", ", ".join(smtp_conf["to_list"]))
        return True
    except Exception as e:
        logger.exception("Failed to send alert email: %s", e)
        return False
