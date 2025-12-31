from datetime import datetime, timezone

from shapely.geometry import shape
from sqlalchemy import text

from utils.helper import build_message, clean_int


def insert_nowcast(features, engine, INSERT_SQL, logger):
    logger.info(f"Insertion to the table for engine{engine} started")
    with engine.begin() as conn:
        for f in features:
            p = f["properties"]

            if clean_int(p.get("cat1")):
                continue

            color = clean_int(p.get("Color"))
            if color not in (3, 4):
                continue

            message = p.get("message") or build_message(p)
            conn.execute(
                text(INSERT_SQL),
                {
                    "id": f["id"],
                    "date": p.get("Date"),
                    "message": message,
                    "toi": clean_int(p.get("toi")),
                    "vupto": clean_int(p.get("vupto")),
                    "color": color,
                    "created_at": datetime.now(timezone.utc),
                    "geometry": shape(f["geometry"]).wkt,
                },
            )
