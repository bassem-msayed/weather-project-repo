# Project Plan — Climate Drift

**Status:** Pre-development  
**Planned start:** April 19, 2025  
**Planned timeline:** 1 week  
**Author:** Bassem Sayed

---

## Primary Question

Has the temperature risen above the Paris Agreement threshold in the cities I've worked in and visited?

I want to extract real data — 80+ years of it, daily, for temperature and precipitation — and find out whether the places I've actually been to are already past the point the world agreed was dangerous.

---

## Why These Cities

Cairo, Dubai, and Glasgow are personal. I've lived/worked or least visited. I have memories in them. They also sit on three different continents and represent three very different climates — which makes comparison meaningful rather than arbitrary.

If all three have crossed 1.5°C above their historical baseline, that's not a regional story. That's a global one. And if they have, the next 0.5°C will likely come faster than the last.

---

## Data Source

**Open-Meteo Historical Weather API — ERA5 Reanalysis Model**  
https://open-meteo.com/

ERA5 reconstructs historical weather back to 1940 using observational data and atmospheric modeling. It's free, API-accessible, well-documented, and covers the full date range I need.

- **Date range:** 1940–2025 (80+ years of daily records)
- **Expected volume:** ~95,000 records across three cities

**Variables:**

| Variable | Unit | Why I'm including it |
|---|---|---|
| `temperature_2m_max` | °C | Primary metric — daily high |
| `temperature_2m_min` | °C | Primary metric — daily low |
| `precipitation_sum` | mm | Water balance |
| `et0_fao_evapotranspiration` | mm | Water balance |
| `shortwave_radiation_sum` | MJ/m² | Secondary heat context |

> Early ERA5 records may have NaN float values. Planning to handle these in staging via `SAFE_CAST`.

---

## Questions to Answer

**Q1 — Temperature trend**  
Has average annual temperature risen across the three cities between 1940 and 2025?

**Q2 — Paris Agreement threshold**  
Have any of the cities crossed the 1.5°C warming mark relative to their 1940s baseline, measured at the decade level?

**Q3 — Water balance**  
How has the gap between precipitation and evapotranspiration shifted over time — and does it look different across the three cities?

---

## Stack

| Layer | Tool |
|---|---|
| Source | Open-Meteo API → CSV → Google Drive |
| Warehouse | BigQuery External Table |
| Transformation | dbt Cloud + VS Code |
| Version control | Git + GitHub |
| Visualization | Tableau Desktop |

---

## Planned Architecture

```
sources (BigQuery External Tables)
    └── stg_meteo__daily       — cast types, handle nulls and NaN values
    └── stg_meteo__cities      — derive city name from coordinates
            │
            └── int_meteo__joined   — join daily records to city, extract year/month
                        │
                        └── mart_meteo__trends  — annual aggregations + window functions
```

**Grain:** One row per city per year.

**Planned aggregations:** Annual averages for temperature (max, min, mean), solar radiation, precipitation, evapotranspiration, and a derived water deficit column.

**Planned window functions:**

| Function | Purpose |
|---|---|
| 10-year rolling average | Smooth out annual noise to surface the real trend |
| Decade average per city | Measure warming at the scale the Paris Agreement actually uses |
| FIRST_VALUE for 1940s baseline | Anchor everything to a consistent starting point |
| RANK for hottest years | See which years are actually at the top |
| LAG for year-over-year delta | Capture how fast things are changing year to year |

---

## Planned Tests

**Generic:**
- `not_null` on key columns in staging and mart
- `unique` on `location_id` in cities staging
- `accepted_values` for city name

**Custom:**
- Assert that minimum temperature never exceeds maximum temperature for any record

---

## Definition of Done

- [ ] Raw data loaded to BigQuery as external table
- [ ] Staging models clean and tested
- [ ] Intermediate join validated
- [ ] Mart produces one row per city per year with all aggregations and window functions
- [ ] All dbt tests passing
- [ ] Column descriptions complete across all models
- [ ] Tableau dashboard built and ready to share
- [ ] README written with findings backed by model output
- [ ] Scheduled job configured and confirmed in dbt Cloud
- [ ] Repository pushed to GitHub with clean commit history

---

## Scope Boundaries

Keeping this tight. The following are out of scope:

- Forecasting or predictive modeling
- Cities beyond the three selected
- Monthly or seasonal granularity in the mart — annual only
- Statistical significance testing
- Python analysis layer