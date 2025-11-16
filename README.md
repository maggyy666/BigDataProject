# Min to run

- DATASET: `https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page`

- Go to 2025 > january > High Volume For-Hire Vehicle Trip Records (PARQUET)
- Create `data/raw`  
- Download `flhvhv_tripdata_2025-01.parquet` to `data/raw`

---

- `python -m venv venv`

- `pip install -r requirements.txt`

- `docker compose build --no-cache`

- `docker compose up`
---
## TO-DO

Some versions have issues with access to files. Spark-user related stuff.

