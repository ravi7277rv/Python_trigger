import json
import os
import re
from datetime import datetime, timedelta

from dotenv import load_dotenv

from utils.build_alert import build_alert_message, send_alert_email
from utils.cloud_summery import extract_cloud_summaries, parse_probabilities_from_text
from utils.find_latest_archive_row import (
    download_pdf_bytes,
    extract_text_from_pdf_bytes,
    find_latest_archive_row,
)
from utils.logger_config import setup_logger
from utils.table_creation import (
    bulletin_exists,
    create_table_if_not_exists,
    insert_bulletin_row,
)

load_dotenv()

# Setting UP logger
logger = setup_logger()

INSERT_MODE = "only_if_new"

# Behaviour

SMTP_HOST = os.environ.get("SMTP_HOST")
SMTP_PORT = os.environ.get("SMTP_PORT")
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASS = os.environ.get("SMTP_PASS")
ALERT_FROM = os.environ.get("ALERT_FROM")
ALERT_TO = os.environ.get("ALERT_TO")
# Convert ALERT_TO to a clean list
ALERT_TO_LIST = json.loads(ALERT_TO)


smtp_conf = {
    "host": SMTP_HOST,
    "port": SMTP_PORT,
    "user": SMTP_USER,
    "pass": SMTP_PASS,
    "from": ALERT_FROM,
    "to_list": ALERT_TO_LIST,  # store as list
}


def run_pipeline():
    logger.info("Starting cyclone pipeline (table-parser, no IST columns)")
    create_table_if_not_exists(logger)

    try:
        pdf_ts, pdf_url = find_latest_archive_row(logger)
        if pdf_ts is None:
            subj = "IMD Cyclone PDF Not Found"
            body = (
                "The IMD Tropical Weather Outlook PDF for today "
                "was not found at the expected location.\n\n"
                "Please verify if the document has been published."
            )

            logger.warning("No IMD cyclone PDF found for today")
            sent = send_alert_email(subj, body, smtp_conf, logger)
            return
        logger.info("Latest archive timestamp: %s ; url: %s", pdf_ts, pdf_url)
    except Exception as e:
        logger.exception("Failed to find latest archive row: %s", e)
        return

    pdf_ts_ist = pdf_ts

    if INSERT_MODE == "only_if_new":
        try:
            if bulletin_exists(pdf_ts_ist, logger):
                logger.info("Bulletin already exists, skipping insert.")
                return
        except Exception as e:
            logger.exception("Error checking bulletin existence: %s", e)

    try:
        pdf_bytes = download_pdf_bytes(pdf_url)
        if not pdf_bytes:
            logger.error("PDF download failed: %s", pdf_url)
            return
        text = extract_text_from_pdf_bytes(pdf_bytes)
    except Exception as e:
        logger.exception("Failed to download or extract PDF: %s", e)
        return

    bay_summary, arab_summary = extract_cloud_summaries(text)
    bay_tokens, arab_tokens = parse_probabilities_from_text(text, logger)

    # compute based_on_utc (attempt parse; fallback to pdf_ts date)
    based_on_date = None
    m = re.search(
        r"BASED\s+ON\s+0?3:00\s+UTC\s+OF\s+(\d{1,2}[.\-/]\d{1,2}[.\-/]\d{2,4})",
        text,
        re.IGNORECASE,
    )
    if m:
        ds = m.group(1).replace(".", "-").replace("/", "-")
        try:
            based_on_date = datetime.strptime(ds, "%d-%m-%Y")
        except:
            based_on_date = None
    if not based_on_date:
        based_on_date = datetime(
            pdf_ts_ist.year, pdf_ts_ist.month, pdf_ts_ist.day, 3, 0
        )

    based_on_utc = based_on_date.replace(hour=3, minute=0, second=0, microsecond=0)
    issued_at_utc = based_on_utc + timedelta(hours=3)
    T0 = based_on_utc + timedelta(hours=5, minutes=30)

    prob_keys = ["0_24", "24_48", "48_72", "72_96", "96_120", "120_144", "144_168"]
    data = {}
    data["bulletin_date"] = pdf_ts_ist.date()
    data["issued_at_utc"] = issued_at_utc
    data["based_on_utc"] = based_on_utc
    data["pdf_timestamp_ist"] = pdf_ts_ist

    data["bay_cloud_summary"] = bay_summary
    data["arabian_cloud_summary"] = arab_summary
    data["remarks"] = "Parsed from bulletin (table-parser, no IST columns)"

    prob_text_to_code = {"NIL": 0, "LOW": 1, "MID": 2, "HIGH": 3}

    bay_dates = []
    arab_dates = []
    for i, key in enumerate(prob_keys):
        forecast_date = (T0 + timedelta(hours=24 * (i + 1))).date()
        bay_dates.append(forecast_date)
        arab_dates.append(forecast_date)

        btoken = bay_tokens[i].upper() if bay_tokens and len(bay_tokens) > i else "NIL"
        atoken = (
            arab_tokens[i].upper() if arab_tokens and len(arab_tokens) > i else "NIL"
        )
        if btoken == "MODERATE":
            btoken = "MID"
        if atoken == "MODERATE":
            atoken = "MID"
        if btoken not in ("NIL", "LOW", "MID", "HIGH"):
            btoken = "NIL"
        if atoken not in ("NIL", "LOW", "MID", "HIGH"):
            atoken = "NIL"

        data[f"bay_{key}"] = prob_text_to_code.get(btoken, 0)
        data[f"bay_{key}_date"] = forecast_date

        data[f"arab_{key}"] = prob_text_to_code.get(atoken, 0)
        data[f"arab_{key}_date"] = forecast_date

    any_alert = any(tok != "NIL" for tok in bay_tokens + arab_tokens)
    if not ALERT_TO_LIST:
        raise ValueError("No valid recipient emails found in ALERT_TO")

    if any_alert:
        bay_texts = [tok.upper() if tok else "NIL" for tok in bay_tokens]
        arab_texts = [tok.upper() if tok else "NIL" for tok in arab_tokens]
        subj, body = build_alert_message(
            pdf_ts_ist, bay_texts, arab_texts, bay_dates, arab_dates
        )

        sent = send_alert_email(subj, body, smtp_conf, logger)
        if not sent:
            logger.error("Alert send FAILED; continuing to insert record.")
    else:
        subj, body = build_alert_message(
            pdf_ts_ist, bay_tokens, arab_tokens, bay_dates, arab_dates
        )
        try:
            send_alert_email(subj, body, smtp_conf, logger)
        except Exception:
            logger.exception("Failed to send informational NIL email.")

    try:
        insert_bulletin_row(data, logger)
        logger.info("Bulletin inserted successfully.")
    except Exception as e:
        logger.exception("Insertion failed: %s", e)
        return


if __name__ == "__main__":

    logger.info("Pipeline started executing")
    run_pipeline()
    logger.info("Pipeline finished successfully.")
