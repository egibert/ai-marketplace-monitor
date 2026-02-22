"""
Load uszips.csv and populate:
  - zip_county (zip, county_id) for Pennsylvania
  - city_zip (city, state, zip) for city/state -> zip geolocation (all states in CSV or PA only)
Requires: counties table with id, county_name. Run sql/create_city_zip.sql first to create city_zip.
"""
import pandas as pd
import mysql.connector

CONFIG = {
    "host": "127.0.0.1",
    "user": "root",
    "password": "root",
    "database": "pa_mh",
}
CSV_PATH = "uszips.csv"
# Set to None to load all states into city_zip; set to "PA" to load only Pennsylvania
STATE_FILTER = "PA"


def main():
    conn = mysql.connector.connect(**CONFIG)
    cursor = conn.cursor(dictionary=True)

    print(f"Loading {CSV_PATH}...")
    df = pd.read_csv(CSV_PATH)

    # CSV must have: zip, state (or state_id), county_name, city
    state_col = "state_id" if "state_id" in df.columns else "state"
    if state_col not in df.columns:
        raise SystemExit("CSV must have 'state' or 'state_id' column")
    if "city" not in df.columns:
        raise SystemExit("CSV must have 'city' column")
    city_col = "city"

    # Filter to state if requested
    if STATE_FILTER:
        df = df[df[state_col] == STATE_FILTER]
    print(f"Rows to process: {len(df)} (state_filter={STATE_FILTER})")

    # --- 1. Populate zip_county (PA only when STATE_FILTER is PA) ---
    if "county_name" in df.columns and STATE_FILTER:
        inserted_zc = 0
        skipped_zc = 0
        for _, row in df.iterrows():
            zip_code = str(row["zip"]).zfill(5)
            county_name = row["county_name"]
            cursor.execute(
                "SELECT id FROM counties WHERE county_name = %s",
                (county_name,),
            )
            county = cursor.fetchone()
            if not county:
                skipped_zc += 1
                continue
            try:
                cursor.execute(
                    """
                    INSERT INTO zip_county (zip, county_id)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE county_id = VALUES(county_id)
                    """,
                    (zip_code, county["id"]),
                )
                inserted_zc += 1
            except Exception as e:
                print(f"zip_county Error {zip_code}: {e}")
                skipped_zc += 1
        conn.commit()
        print(f"zip_county: inserted/updated={inserted_zc}, skipped={skipped_zc}")
    else:
        print("zip_county: skipped (no county_name or no STATE_FILTER)")

    # --- 2. Populate city_zip (city, state, zip) for geolocation ---
    cursor.execute("DELETE FROM city_zip")
    inserted_cz = 0
    seen = set()
    for _, row in df.iterrows():
        zip_code = str(row["zip"]).zfill(5)
        state = str(row[state_col]).strip()[:10]
        city = str(row[city_col]).strip()[:255] if pd.notna(row[city_col]) else ""
        if not city or not state or len(zip_code) != 5:
            continue
        key = (city.lower(), state.upper(), zip_code)
        if key in seen:
            continue
        seen.add(key)
        try:
            cursor.execute(
                """
                INSERT INTO city_zip (city, state, zip) VALUES (%s, %s, %s)
                """,
                (city, state, zip_code),
            )
            inserted_cz += 1
        except Exception as e:
            print(f"city_zip Error {city},{state},{zip_code}: {e}")
    conn.commit()
    print(f"city_zip: inserted={inserted_cz}")

    cursor.close()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
