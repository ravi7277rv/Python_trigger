from datetime import timedelta, timezone

from sqlalchemy import text

IST = timezone(timedelta(hours=5, minutes=30))


def fetch_alerts(engine, schema, table):
    sql = text(
        f"""
        SELECT indus_circle, district, color, message, toi, vupto
        FROM {schema}.{table}
        WHERE color IN (3,4)
          AND update_time::date = CURRENT_DATE
        ORDER BY indus_circle, color DESC;
    """
    )

    with engine.connect() as conn:
        result = conn.execute(sql)
        return result.fetchall()


def get_subject_time(engine, schema, table):
    sql = text(
        f"""
        SELECT MAX(update_time)
        FROM {schema}.{table}
        WHERE color IN (3,4);
    """
    )

    with engine.connect() as conn:
        ts = conn.execute(sql).scalar()

    return ts.astimezone(IST).strftime("%d %b %Y | %H:%M IST") if ts else "N/A"
