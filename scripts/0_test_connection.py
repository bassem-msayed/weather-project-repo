import datetime
import requests
import pandas as pd

# Coordinates matching your dim_city seed
CITIES = [
    {"location_id": 1, "city_name": "Glasgow", "lat": 55.75, "lon": -4.25},
    {"location_id": 2, "city_name": "Dubai",   "lat": 25.00, "lon": 55.25},
    {"location_id": 3, "city_name": "Cairo",   "lat": 30.04, "lon": 31.23}
]

def test_api_call():
    # Test a compact 5-day window from last month to verify response parsing
    start_date = "2024-01-01"
    end_date = "2024-01-05"
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    print(f"[*] Testing Open-Meteo Archive API ({start_date} to {end_date})...")
    
    all_rows = []
    for city in CITIES:
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
        
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        payload = response.json()
        daily = payload.get("daily", {})
        
        dates = daily.get("time", [])
        for i, dt in enumerate(dates):
            all_rows.append({
                "location_id": city["location_id"],
                "city_name": city["city_name"],
                "date": dt,
                "temp_max": daily["temperature_2m_max"][i],
                "temp_min": daily["temperature_2m_min"][i],
                "evapotranspiration_mm": daily["et0_fao_evapotranspiration"][i],
                "solar_radiation_mj": daily["shortwave_radiation_sum"][i],
                "precipitation_mm": daily["precipitation_sum"][i],
            })
        print(f" -> Fetched {len(dates)} days for {city['city_name']}")

    df = pd.DataFrame(all_rows)
    print("\n--- TEST RUN RESULTS ---")
    print(df.to_string(index=False))
    print(f"\n[+] Total rows extracted: {len(df)}")
    print("[+] Status: API connection and data unpacking verified successfully.")

if __name__ == "__main__":
    test_api_call()