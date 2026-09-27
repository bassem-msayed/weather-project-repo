import os
import time
import datetime
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# ==============================================================================
# ENVIRONMENT & CONFIGURATION
# ==============================================================================
# Load environment variables from .env file located at repo root
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

def fetch_chunk_with_retry(city: dict, start_date: str, end_date: str, max_retries: int = 5) -> list[dict]:
    """Fetches a date slice with exponential backoff if a 429 rate limit is encountered."""
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
    
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, params=params, timeout=45)
            
            if response.status_code == 429:
                wait_time = attempt * 12
                print(f"[429 Rate Limit] Pausing {wait_time}s before retry {attempt}/{max_retries}...", flush=True)
                time.sleep(wait_time)
                continue
                
            response.raise_for_status()
            payload = response.json()
            daily = payload.get("daily", {})
            dates = daily.get("time", [])
            
            records = []
            for i, dt in enumerate(dates):
                records.append({
                    "location_id": city["location_id"],
                    "date": dt,
                    "temp_max": daily["temperature_2m_max"][i],
                    "temp_min": daily["temperature_2m_min"][i],
                    "evapotranspiration_mm": daily["et0_fao_evapotranspiration"][i],
                    "solar_radiation_mj": daily["shortwave_radiation_sum"][i],
                    "precipitation_mm": daily["precipitation_sum"][i],
                    "extracted_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
                })
            return records
            
        except requests.exceptions.RequestException as e:
            if attempt == max_retries:
                raise e
            time.sleep(5)
            
    return []

def run_backfill():
    # -------------------------------------------------------------------------
    # Authentication Setup: Supports both Local (.env) and CI/CD (GitHub Actions)
    # -------------------------------------------------------------------------
    if KEY_PATH and os.path.exists(KEY_PATH):
        print(f"[*] Authenticating using local service account key: {KEY_PATH}")
        credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
        client = bigquery.Client(project=GCP_PROJECT_ID, credentials=credentials)
    elif os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        print("[*] Authenticating via GOOGLE_APPLICATION_CREDENTIALS environment variable...")
        client = bigquery.Client(project=GCP_PROJECT_ID)
    else:
        try:
            print("[*] Attempting default application credentials...")
            client = bigquery.Client(project=GCP_PROJECT_ID)
        except Exception as e:
            raise ValueError(
                "[!] No valid Google Cloud credentials found. "
                "Set GCP_KEY_PATH in .env (local) or configure GOOGLE_APPLICATION_CREDENTIALS (CI/CD)."
            ) from e

    # 10-year chunking window
    end_date_dt = datetime.date.today() - datetime.timedelta(days=5)
    year_ranges = []
    current_year = 1940
    while current_year <= end_date_dt.year:
        chunk_start = f"{current_year}-01-01"
        chunk_end_year = min(current_year + 9, end_date_dt.year)
        chunk_end = end_date_dt.strftime("%Y-%m-%d") if chunk_end_year == end_date_dt.year else f"{chunk_end_year}-12-31"
        year_ranges.append((chunk_start, chunk_end))
        current_year += 10

    all_data = []
    print("=== Starting Resilient Weather Backfill ===")
    for city in CITIES:
        print(f"\n[*] Fetching data for {city['city_name']}...")
        city_count = 0
        for start_str, end_str in year_ranges:
            print(f"    -> Window: {start_str} to {end_str}...", end=" ", flush=True)
            records = fetch_chunk_with_retry(city, start_str, end_str)
            all_data.extend(records)
            city_count += len(records)
            print(f"done ({len(records):,} rows)", flush=True)
            time.sleep(2.5)
            
        print(f"[✓] {city['city_name']} complete: {city_count:,} records.")

    df = pd.DataFrame(all_data)
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["extracted_at"] = pd.to_datetime(df["extracted_at"])
    df["location_id"] = df["location_id"].astype(int)
    for col in ["temp_max", "temp_min", "evapotranspiration_mm", "solar_radiation_mj", "precipitation_mm"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    print(f"\n[+] Total rows prepared: {len(df):,}")
    
    table_ref = f"{GCP_PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    schema = [
        bigquery.SchemaField("location_id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("temp_max", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("temp_min", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("evapotranspiration_mm", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("solar_radiation_mj", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("precipitation_mm", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("extracted_at", "TIMESTAMP", mode="REQUIRED"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    print(f"[*] Submitting BigQuery Load Job to {table_ref}...")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    
    table = client.get_table(table_ref)
    print(f"\n[SUCCESS] Table {table_ref} populated!")
    print(f"[SUCCESS] Row Count in BigQuery: {table.num_rows:,} rows.")

    # ==========================================================================
    # SANDBOX WORKAROUND: Reset table expiration clock
    # ==========================================================================
    new_expiration = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=59)
    table.expires = new_expiration
    client.update_table(table, ["expires"])
    print(f"[SUCCESS] BigQuery Sandbox expiration extended to: {new_expiration.strftime('%Y-%m-%d %H:%M:%S UTC')}")

if __name__ == "__main__":
    run_backfill()