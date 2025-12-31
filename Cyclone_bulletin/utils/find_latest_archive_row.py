import os
from datetime import datetime
from io import BytesIO
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from PyPDF2 import PdfReader

load_dotenv()

# IMD Base URL
IMD_BASE_URL = "https://mausam.imd.gov.in"
IMD_PAGE_URL = "https://mausam.imd.gov.in/responsive/cyclone_bulletin_archive.php?id=1"
HEADERS = {"User-Agent": "Mozilla/5.0 (cyclone-pipeline/1.0)"}

session = requests.Session()
session.headers.update(HEADERS)


def scrape_imd_pdf_table():
    response = requests.get(IMD_PAGE_URL, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    rows_data = []

    table = soup.find("table")
    if not table:
        return rows_data

    for row in table.find_all("tr")[1:]:  # skip header
        cols = row.find_all("td")
        if len(cols) < 3:
            continue

        date_text = cols[1].get_text(strip=True)
        link_tag = cols[2].find("a")

        if not link_tag:
            continue

        try:
            timestamp = datetime.strptime(date_text, "%Y-%m-%d %I:%M %p")
        except ValueError:
            continue

        pdf_url = urljoin(IMD_BASE_URL, link_tag["href"])

        rows_data.append({"timestamp": timestamp, "url": pdf_url})

    return rows_data


def find_latest_pdf_row(rows):
    if not rows:
        return None, None

    # Sort by timestamp descending
    latest = max(rows, key=lambda r: r["timestamp"])
    return latest["timestamp"], latest["url"]


def find_latest_archive_row(logger):
    try:
        rows = scrape_imd_pdf_table()
    except Exception as e:
        logger.exception("Failed to scrape IMD PDF table: %s", e)
        return

    pdf_ts, pdf_url = find_latest_pdf_row(rows)

    if pdf_ts is None:
        return None, None

    logger.info("Latest available IMD PDF: %s ; %s", pdf_ts, pdf_url)

    # continue your processing here
    return pdf_ts, pdf_url


def download_pdf_bytes(url, session=None, timeout=30):
    s = session or requests  # <-- IMPORTANT FIX
    r = s.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return BytesIO(r.content)


def extract_text_from_pdf_bytes(pdf_bytes):
    reader = PdfReader(pdf_bytes)
    pages = []
    for p in reader.pages:
        pages.append(p.extract_text() or "")
    return "\n".join(pages)
    return "\n".join(pages)
