import os
import datetime
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# ==============================================================================
# ENVIRONMENT & CONFIGURATION
# ==============================================================================
load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "meteo-historical-project")
KEY_PATH = os.getenv("GCP_KEY_PATH")

DATASET_ID = "Meteo_Historical_Dataset"
TABLE_ID = "raw_weather_daily"

CITIES = [
    {"location_id": 1, "city_name": "Glasgow", "lat": 55.75, "lon": -4.25},
    {"location_id": 2, "city_name": "Dubai",   "lat": 25.00, "lon": 55.25},
    {"location_id": 3, "city_name": "Cairo",   "lat": 30.04, "lon": 31.23}
]

def fetch_weekly_weather(city: dict, start_date: str, end_date: str) -> list[dict]:
    """Fetches a rolling window of daily weather for a single city."""
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "start_date": start_date,
        "end_date": end_date,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "et0_fao_evapotranspiration",
            "shortwave_radiation_sum",
            "precipitation_sum"
        ],
        "timezone": "UTC"
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()
    daily = payload.get("daily", {})

    dates = daily.get("time", [])
    records = []
    for i, date_str in enumerate(dates):
        records.append({
            "location_id": city["location_id"],
            "date": date_str,
            "temp_max": daily["temperature_2m_max"][i],
            "temp_min": daily["temperature_2m_min"][i],
            "evapotranspiration_mm": daily["et0_fao_evapotranspiration"][i],
            "solar_radiation_mj": daily["shortwave_radiation_sum"][i],
            "precipitation_mm": daily["precipitation_sum"][i],
            "extracted_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
        })
    return records

def run_weekly_refresh():
    if not KEY_PATH:
        raise ValueError(
            "[!] GCP_KEY_PATH is not set. Ensure you have defined GCP_KEY_PATH in your .env file."
        )
    if not os.path.exists(KEY_PATH):
        raise FileNotFoundError(
            f"[!] Service account key file not found at: {KEY_PATH}"
        )

    # Rolling window: past 14 days up to yesterday
    today = datetime.date.today()
    start_date = (today - datetime.timedelta(days=14)).strftime("%Y-%m-%d")
    end_date = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"=== Starting Weekly Weather Refresh ({start_date} to {end_date}) ===")

    all_data = []
    for city in CITIES:
        print(f"[*] Fetching {city['city_name']}...", end=" ", flush=True)
        rows = fetch_weekly_weather(city, start_date, end_date)
        all_data.extend(rows)
        print(f"done ({len(rows)} records)")

    df = pd.DataFrame(all_data)

    # Type casting to match BigQuery schema
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["extracted_at"] = pd.to_datetime(df["extracted_at"])
    df["location_id"] = df["location_id"].astype(int)
    for col in ["temp_max", "temp_min", "evapotranspiration_mm", "solar_radiation_mj", "precipitation_mm"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    print(f"\n[+] Total rows extracted: {len(df)}")

    # BigQuery Client Initialization
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
    table_ref = f"{GCP_PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Append rows to raw landing table
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    print(f"[*] Appending batch to BigQuery: {table_ref}...")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()

    table = client.get_table(table_ref)
    print(f"[SUCCESS] Appended {len(df)} rows.")
    print(f"[SUCCESS] Total table size now in BigQuery: {table.num_rows:,} rows.")

    # ==========================================================================
    # SANDBOX WORKAROUND: Reset table expiration clock
    # ==========================================================================
    new_expiration = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=59)
    table.expires = new_expiration
    client.update_table(table, ["expires"])
    print(f"[SUCCESS] BigQuery Sandbox expiration extended to: {new_expiration.strftime('%Y-%m-%d %H:%M:%S UTC')}")

if __name__ == "__main__":
    run_weekly_refresh()