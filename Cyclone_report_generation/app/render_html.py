import base64
import os
from datetime import datetime, timedelta

import pytz
from jinja2 import Environment, FileSystemLoader

from app import fetch_data

static_dir = os.path.join(os.path.dirname(__file__), "..", "static")


def get_current_time_ampm():
    # return datetime.now().strftime("%#I:%M %p")
    now = datetime.now()
    return [
        f"{now.strftime('%d %B %Y')}, updated {now.strftime('%H:00')}",
        f"{now.strftime('%d %B, %Y')}",
    ]


def get_7dayforcast_day_date():
    today = datetime.today()
    seven_days = [
        {
            "day": (today + timedelta(days=i)).strftime("%A, "),
            "date": (today + timedelta(days=i)).strftime("%d %b, %Y"),
        }
        for i in range(7)
    ]
    return seven_days


def load_cyclone_map_image():
    base_dir = r"C:\inetpub\wwwroot\Weather\reports\img_cyclone"

    if not os.path.exists(base_dir):
        return None

    # List valid image files
    valid_ext = (".png", ".jpg", ".jpeg", ".svg", ".webp")
    files = [f for f in os.listdir(base_dir) if f.lower().endswith(valid_ext)]

    if not files:
        return None  # No image available

    # Pick the latest modified image file
    files.sort(key=lambda f: os.path.getmtime(os.path.join(base_dir, f)), reverse=True)
    image_path = os.path.join(base_dir, files[0])

    # Determine MIME type
    ext = files[0].split(".")[-1].lower()
    mime = "svg+xml" if ext == "svg" else ext

    with open(image_path, "rb") as img:
        encoded = base64.b64encode(img.read()).decode("utf-8")

    return f"data:image/{mime};base64,{encoded}"


def current_formatted_datetime():
    ist = pytz.timezone("Asia/Kolkata")
    dt = datetime.now(ist)

    date_str = f"{ordinal_date(dt.day)} {dt.strftime('%B %Y')}"
    time_str = dt.strftime("%I:%M %p IST")

    return date_str, time_str


def ordinal_date(day):
    suffix = "th"
    if 11 <= day % 100 <= 13:
        suffix = "th"
    else:
        if day % 10 == 1:
            suffix = "st"
        elif day % 10 == 2:
            suffix = "nd"
        elif day % 10 == 3:
            suffix = "rd"
    return f"{day}{suffix}"


def build(grouped_data):
    template_dir = os.path.join(os.path.dirname(__file__), "..", "templates")
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template("report_template.html")

    # ICONS
    flood = f"{static_dir}/icons/flood.svg".replace("\\", "/")
    cyclone = f"{static_dir}/icons/cyclone.svg".replace("\\", "/")
    lightning = f"{static_dir}/icons/lightning.svg".replace("\\", "/")
    landslide = f"{static_dir}/icons/landslide.svg".replace("\\", "/")
    avalanche = f"{static_dir}/icons/avalanche.svg".replace("\\", "/")
    snowfall = f"{static_dir}/icons/snowfall.svg".replace("\\", "/")

    fog = f"{static_dir}/icons/fog.svg".replace("\\", "/")
    rainfall = f"{static_dir}/icons/rainfall.svg".replace("\\", "/")
    temperature = f"{static_dir}/icons/temperature.svg".replace("\\", "/")
    wind = f"{static_dir}/icons/wind.svg".replace("\\", "/")
    solar_fog_bold = f"{static_dir}/icons/solar_fog_bold.svg".replace("\\", "/")
    vector_bg = f"{static_dir}/icons/Vector.svg".replace("\\", "/")
    map_png = f"{static_dir}/icons/map.png".replace("\\", "/")
    CircleRainfall = f"{static_dir}/icons/CircleRainfall.svg".replace("\\", "/")
    CircleWind = f"{static_dir}/icons/CircleWind.svg".replace("\\", "/")
    CircleFog = f"{static_dir}/icons/CircleFog.svg".replace("\\", "/")
    CircleTemp = f"{static_dir}/icons/temp_1.svg".replace("\\", "/")
    CircleHumidity = f"{static_dir}/icons/Humidity_circle.svg".replace("\\", "/")
    Humidity = f"{static_dir}/icons/Humidity.svg".replace("\\", "/")
    Visibility = f"{static_dir}/icons/visibility.svg".replace("\\", "/")

    # Cover Page
    headerlogo = f"{static_dir}/icons/headerlogo.svg".replace("\\", "/")
    tower_img = f"{static_dir}/icons/tower-img.jpg".replace("\\", "/")
    ml_logo = f"{static_dir}/icons/MLLogo.png".replace("\\", "/")
    indus_logo = f"{static_dir}/icons/induslogo.svg".replace("\\", "/")

    icons = {
        "flood": f"file:///{flood}",
        "cyclone": f"file:///{cyclone}",
        "landslide": f"file:///{landslide}",
        "lightening": f"file:///{lightning}",
        "avalanche": f"file:///{avalanche}",
        "snowfall": f"file:///{snowfall}",
        "fog": f"file:///{fog}",
        "rainfall": f"file:///{rainfall}",
        "temperature": f"file:///{temperature}",
        "wind": f"file:///{wind}",
        "solar_fog_bold": f"file:///{solar_fog_bold}",
        "vector_bg": f"file:///{vector_bg}",
        "map_png": f"file:///{map_png}",
        "CircleRainfall": f"file:///{CircleRainfall}",
        "CircleWind": f"file:///{CircleWind}",
        "CircleFog": f"file:///{CircleFog}",
        "CircleTemp": f"file:///{CircleTemp}",
        "CircleHumidity": f"file:///{CircleHumidity}",
        "Humidity": f"file:///{Humidity}",
        "Visibility": f"file:///{Visibility}",
        "header_logo": f"file:///{headerlogo}",
        "tower_img": f"file:///{tower_img}",
        "ml_logo": f"file:///{ml_logo}",
        "indus_logo": f"file:///{indus_logo}",
    }

    district_severity_count, kpi_severity_range_color = (
        fetch_data.fetch_district_count_saverity_wise("AP")
    )

    icons["cyclone_map"] = load_cyclone_map_image()

    date_str, time_str = current_formatted_datetime()

    rendered_html = template.render(
        grouped=grouped_data,
        generated_on=datetime.now().strftime("%Y-%m-%d %H:%M"),
        icons=icons,
        current_time=get_current_time_ampm(),
        kpi_severity_range_color=kpi_severity_range_color,
        date_string=date_str,
        time_string=time_str,
    )
    output_dir = os.path.join(os.path.dirname(__file__), "..", "output")
    output_path = f"{output_dir}/report.html"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(rendered_html)

    return output_path

    # def one_pager_build(circle, circle_name):
    template_dir = os.path.join(os.path.dirname(__file__), "..", "templates")
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template("one_pager.html")

    #  Map & Charts
    map_path = f"{static_dir}/maps/map.png".replace("\\", "/")
    chart_path = f"{static_dir}/charts/chart.png".replace("\\", "/")
    stacked_chart_zone = f"{static_dir}/charts/stacked-chart-zone.png".replace(
        "\\", "/"
    )
    stacked_chart_tower = f"{static_dir}/charts/stacked-chart-tower.png".replace(
        "\\", "/"
    )

    # ICONS
    flood = f"{static_dir}/icons/flood.svg".replace("\\", "/")
    cyclone = f"{static_dir}/icons/cyclone.svg".replace("\\", "/")
    lightening = f"{static_dir}/icons/lightening.svg".replace("\\", "/")
    landSlide = f"{static_dir}/icons/landSlide.svg".replace("\\", "/")
    fog = f"{static_dir}/icons/fog.svg".replace("\\", "/")
    avalanche = f"{static_dir}/icons/avalanche.svg".replace("\\", "/")
    rainfall = f"{static_dir}/icons/rainfall.svg".replace("\\", "/")
    temperature = f"{static_dir}/icons/temperature.svg".replace("\\", "/")
    wind = f"{static_dir}/icons/wind.svg".replace("\\", "/")
    solar_fog_bold = f"{static_dir}/icons/solar_fog_bold.svg".replace("\\", "/")
    vector_bg = f"{static_dir}/icons/Vector.svg".replace("\\", "/")
    map_png = f"{static_dir}/icons/map.png".replace("\\", "/")
    CircleRainfall = f"{static_dir}/icons/CircleRainfall.svg".replace("\\", "/")
    CircleWind = f"{static_dir}/icons/CircleWind.svg".replace("\\", "/")
    CircleFog = f"{static_dir}/icons/CircleFog.svg".replace("\\", "/")
    CircleTemp = f"{static_dir}/icons/temp_1.svg".replace("\\", "/")
    CircleHumidity = f"{static_dir}/icons/Humidity_circle.svg".replace("\\", "/")
    Humidity = f"{static_dir}/icons/Humidity.svg".replace("\\", "/")
    Visibility = f"{static_dir}/icons/visibility.svg".replace("\\", "/")

    # Cover Page
    headerlogo = f"{static_dir}/icons/headerlogo.svg".replace("\\", "/")
    tower_img = f"{static_dir}/icons/tower-img.jpg".replace("\\", "/")
    ml_logo = f"{static_dir}/icons/MLLogo.png".replace("\\", "/")
    indus_logo = f"{static_dir}/icons/induslogo.svg".replace("\\", "/")

    icons = {
        "flood": f"file:///{flood}",
        "cyclone": f"file:///{cyclone}",
        "landSlide": f"file:///{landSlide}",
        "lightening": f"file:///{lightening}",
        "fog": f"file:///{fog}",
        "avalanche": f"file:///{avalanche}",
        "rainfall": f"file:///{rainfall}",
        "temperature": f"file:///{temperature}",
        "wind": f"file:///{wind}",
        "solar_fog_bold": f"file:///{solar_fog_bold}",
        "vector_bg": f"file:///{vector_bg}",
        "map_png": f"file:///{map_png}",
        "CircleRainfall": f"file:///{CircleRainfall}",
        "CircleWind": f"file:///{CircleWind}",
        "CircleFog": f"file:///{CircleFog}",
        "CircleTemp": f"file:///{CircleTemp}",
        "CircleHumidity": f"file:///{CircleHumidity}",
        "Humidity": f"file:///{Humidity}",
        "Visibility": f"file:///{Visibility}",
        "header_logo": f"file:///{headerlogo}",
        "tower_img": f"file:///{tower_img}",
        "ml_logo": f"file:///{ml_logo}",
        "indus_logo": f"file:///{indus_logo}",
    }

    charts = {
        "bar_chart": f"file:///{chart_path}",
        "stacked_chart_zone": f"file:///{stacked_chart_zone}",
        "stacked_chart_tower": f"file:///{stacked_chart_tower}",
    }

    maps = {"district_map": f"file:///{map_path}"}

    district_severity_count, kpi_severity_range_color = (
        fetch_data.fetch_district_count_saverity_wise(circle)
    )
    hazard_list = ["Cyclone", "Lightning", "Flood", "Avalanche", "Snowfall"]

    rendered_html = template.render(
        generated_on=datetime.now().strftime("%Y-%m-%d %H:%M"),
        maps_path=maps,
        charts_path=charts,
        icons=icons,
        circle=circle,
        circle_name=circle_name,
        current_time=get_current_time_ampm(),
        forecast_day_date=get_7dayforcast_day_date(),
        district_severity_count=district_severity_count,
        kpi_severity_range_color=kpi_severity_range_color,
        severity_districts_list=fetch_data.fetch_district_names_saverity_wise_7days(
            circle
        ),
        hazard_districts=fetch_data.get_onepager_district_hazards(circle, hazard_list),
    )
    output_dir = os.path.join(os.path.dirname(__file__), "..", "output")
    output_path = f"{output_dir}/one_pager_report.html"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(rendered_html)

    return output_path
