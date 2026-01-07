# Condition Text mapping on the base of the number for url_2
CAT_MESSAGE_MAP_URL_2 = {
    "1": "No Warning",
    "2": "Heavy Rain",
    "3": "Heavy Snow",
    "4": "Thunderstorms & Lightning, Squall etc",
    "5": "Hailstorm",
    "6": "Dust Storm",
    "7": "Dust Raising Winds",
    "8": "Strong Surface Winds",
    "9": "Heat Wave",
    "10": "Hot Day",
    "11": "Warm Night",
    "12": "Cold Wave",
    "13": "Cold Day",
    "14": "Ground Frost",
    "15": "Fog",
    "16": "Very Heavy Rain",
    "17": "Extremely Heavy Rain",
}


IMD_COLOR_SEVERITY = {1: "Extreme", 2: "High", 3: "Moderate", 4: "Low"}


def clean_int(val):
    if val in ("", None):
        return None
    try:
        return int(val)
    except:
        return None


def clean_text(val):
    if val in ("", None):
        return None
    return str(val)


def cat_text(val):
    if val in ("", None):
        return None
    cats = [c.strip() for c in str(val).split(",") if c.strip()]
    texts = [CAT_MESSAGE_MAP_URL_2.get(c) for c in cats if c in CAT_MESSAGE_MAP_URL_2]
    return " + ".join(texts) if texts else None


def severity_from_color(color):
    try:
        return IMD_COLOR_SEVERITY.get(int(color))
    except:
        return None
