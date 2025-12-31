from datetime import datetime, timedelta
from user_agents import parse
import re

def circle_name_cover_page(name):
    match = re.search(r'\((.*?)\)', name)
    if match:
        return match.group(1).strip()
    return name.replace("Upper North", "").strip()

def format_hazard_records(items):
    expanded = []
    today = datetime.now().date()   # Current date (YYYY-MM-DD)

    for item in items:
        # Extract numeric part from "Day 1", "Day 2", etc.
        day_str = item.get("day", "")
        day_num = int(day_str.replace("Day", "").strip())

        # Calculate date based on day number
        record_date = today + timedelta(days=day_num - 1)
        record_date_str = record_date.strftime("%d-%m-%Y") 

        # Convert list → comma-separated string
        district_str = ",".join(item.get("districts", []))

        expanded.append({
            "circle": item["circle"],
            "day": item["day"],
            "date": item["date"],
            "description": item["description"],
            "district": district_str,
            "hazardValue": item["hazardValue"],
            "severity": item["severity"]
        })

    return expanded


def format_device_name(device_str: str) -> str:
    if not device_str:
        return "an unknown device"

    parts = device_str.split("-")
    if len(parts) >= 4:
        device_type = parts[0].capitalize()
        browser = parts[2].capitalize()
        os_version = " ".join(parts[3:]).replace("windows", "Windows").replace("-", " ")
        return f"{device_type} through {browser}"
    else:
        return device_str
    

def get_device_label(user_agent_string: str) -> str:
    if not user_agent_string:
        return "Unknown Device"

    ua = parse(user_agent_string)

    # Device type
    if ua.is_mobile:
        device_type = "Mobile"
    elif ua.is_tablet:
        device_type = "Tablet"
    elif ua.is_pc:
        device_type = "PC"
    else:
        device_type = "Device"

    # OS name
    os_name = ua.os.family

    # Browser name + major version
    browser_name = ua.browser.family
    browser_version = ua.browser.version[0] if ua.browser.version else ""

    return f"{os_name} {device_type} | {browser_name} {browser_version}"




