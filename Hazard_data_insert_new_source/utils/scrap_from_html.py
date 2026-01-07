import json
import re

import pandas as pd
import requests

# ------------ BASE URL FOR THE HTML PAGE -----------------------------
BASE_URL = "https://mausam.imd.gov.in/responsive/districtWiseWarning.php"


# ------------------- SEVERITY MAPPING ------------------------------
COLOR_SEVERITY = {
    "#7CFC00": "Low",  # No Warning
    "#FFFF00": "Moderate",  # Watch
    "#FFA500": "High",  # Alert
    "#FF0000": "Extreme",  # Warning
}

# ---------------- SEVERITY CODE MAPPING -----------------------------
SEVERITY_CODE = {
    "#7CFC00": 4,  # No Warning
    "#FFFF00": 3,  # Watch
    "#FFA500": 2,  # Alert
    "#FF0000": 1,  # Warning
}


# --------- SCRAPPING THE DATA DAY WISE 1 TO 5 -----------------------
def scrape_day(day):
    url = BASE_URL + f"?day=Day_{day}"
    print("Scraping:", url)

    r = requests.get(url, timeout=30)
    r.raise_for_status()

    html = r.text

    # Extract JS areas array
    match = re.search(r'"areas"\s*:\s*(\[\s*{.*?}\s*\])\s*,', html, re.S)
    if not match:
        raise Exception("Could not find areas JSON")

    raw_json = match.group(1)

    # Fix JS to JSON
    raw_json = raw_json.replace("\n", "")
    data = json.loads(raw_json)

    rows = []

    for item in data:
        title = item.get("title")
        dist_id = item.get("id")
        color = item.get("color")
        balloon = item.get("balloonText", "")

        # Extract date
        date_match = re.search(r"Date:\s*([0-9\-]+)", balloon)
        date = date_match.group(1) if date_match else None

        # Extract warning text
        if "No warning" in balloon:
            warning = "No warning"
        else:
            warnings = re.findall(r"<p>(.*?)</p>", balloon)
            warnings = [w.strip() for w in warnings if w.strip() and "Updated" not in w]
            warning = ", ".join(warnings)

        severity = COLOR_SEVERITY.get(color, "Unknown")
        color_code = SEVERITY_CODE.get(color, "Unknown")

        row = {
            "day": f"Day_{day}",
            "district": title,
            "district_id": dist_id,
            "date": date,
        }

        # Dynamic keys
        row[f"day_{day}"] = warning
        row[f"day{day}_text"] = warning
        row[f"day{day}_color"] = color_code
        row[f"day{day}_severity"] = severity

        rows.append(row)

    return rows


# -------- MERGING THE DATA DISTRICT WISE --------------------------
def merge_by_district(all_data):
    merged = {}

    for row in all_data:
        key = (row["district"], row["district_id"])

        if key not in merged:
            merged[key] = {
                "district": row["district"],
                "district_id": row["district_id"],
                "date": row.get("date"),
            }

        # Copy all day-related fields (skip "day", "district", "district_id", "date")
        for k, v in row.items():
            if k in ["day", "district", "district_id", "date"]:
                continue
            merged[key][k] = v

    return list(merged.values())


# --------- SCRAPPING THE HAZARD DATA DAY WISE FROM THE HTML --------
def scrape_all_days():
    all_data = []

    for day in range(1, 6):
        day_data = scrape_day(day)
        all_data.extend(day_data)

    # Merge day-wise rows into district-wise rows
    merged_data = merge_by_district(all_data)

    hazard_df = pd.DataFrame(merged_data)
    hazard_df.to_json("imd_5day_warning.json", orient="records", indent=2)

    return hazard_df
