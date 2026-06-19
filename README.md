# Uber Data Engineering ETL Pipeline — GCP

Cloud-native ETL pipeline for NYC Uber taxi trip data built on the modern GCP data stack: Python data modelling, Mage.ai orchestration, BigQuery analytics warehouse, and Looker Studio dashboard.


---

## Architecture

```
Raw CSV (GCS Bucket)
    ↓
Python Transform (pandas — star schema modelling)
    ↓
Mage.ai Pipeline (orchestration on GCP Compute Engine VM)
    ↓
BigQuery (fact + dimension tables)
    ↓
BigQuery SQL (analytics table — multi-table JOIN)
    ↓
Looker Studio Dashboard
```

![Architecture Diagram](architecture.jpg)

---

## Dataset

**NYC TLC (Taxi & Limousine Commission) Trip Record Data**

Yellow taxi trip records with fields: pick-up/drop-off timestamps, geographic coordinates, trip distance, fare breakdown, passenger count, rate type, and payment type.

- Source: NYC Open Data / TLC Trip Record Data
- Format: CSV (`uber_data.csv`)
- Storage: Uploaded to Google Cloud Storage bucket as the pipeline source

---

## Data Model — Star Schema

The raw flat CSV is transformed into a **star schema** using pandas before loading to BigQuery:

![Data Model](data_model.jpeg)

### Dimension Tables (7)

| Dimension Table | Key Columns | Grain |
|---|---|---|
| `datetime_dim` | datetime_id, tpep_pickup_datetime, tpep_dropoff_datetime, pick_hour/day/month/year/weekday, drop_hour/day/month/year/weekday | one row per unique datetime combination |
| `passenger_count_dim` | passenger_count_id, passenger_count | unique passenger counts |
| `trip_distance_dim` | trip_distance_id, trip_distance | unique trip distances |
| `rate_code_dim` | rate_code_id, rate_code_id (raw), rate_code_name | maps rate code integers to names (Standard / JFK / Newark / etc.) |
| `pickup_location_dim` | pickup_location_id, pickup_latitude, pickup_longitude | unique pickup coordinates |
| `dropoff_location_dim` | dropoff_location_id, dropoff_latitude, dropoff_longitude | unique dropoff coordinates |
| `payment_type_dim` | payment_type_id, payment_type (raw), payment_type_name | maps codes to names (Credit card / Cash / No charge / Dispute) |

### Fact Table

`fact_table` — one row per trip, all foreign keys to dimension tables plus fare components:

| Column | Description |
|---|---|
| trip_id | Surrogate primary key |
| VendorID | Taxi vendor identifier |
| datetime_id | FK → datetime_dim |
| passenger_count_id | FK → passenger_count_dim |
| trip_distance_id | FK → trip_distance_dim |
| rate_code_id | FK → rate_code_dim |
| pickup_location_id | FK → pickup_location_dim |
| dropoff_location_id | FK → dropoff_location_dim |
| payment_type_id | FK → payment_type_dim |
| fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount | Fare components |

---

## Transformation Pipeline (Python)

`Uber Data Pipeline (Fixed Version).ipynb` — pandas transformations to build the star schema:

```python
# Example: datetime dimension table construction
datetime_dim = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)
datetime_dim['datetime_id'] = datetime_dim.index
datetime_dim['pick_hour']    = datetime_dim['tpep_pickup_datetime'].dt.hour
datetime_dim['pick_day']     = datetime_dim['tpep_pickup_datetime'].dt.day
datetime_dim['pick_month']   = datetime_dim['tpep_pickup_datetime'].dt.month
datetime_dim['pick_year']    = datetime_dim['tpep_pickup_datetime'].dt.year
datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

# Example: rate_code dimension with human-readable names
rate_code_type = {1:"Standard rate", 2:"JFK", 3:"Newark", 4:"Nassau or Westchester", 5:"Negotiated fare", 6:"Group ride"}
rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
```

---

## Orchestration — Mage.ai

Mage.ai is a modern, open-source data pipeline orchestration tool deployed on a **GCP Compute Engine VM**:

- **Extract block**: load raw CSV from GCS bucket
- **Transform block**: run pandas star schema transformation
- **Load block**: write fact and dimension tables to BigQuery dataset (`uber_data_engineering_yt`)

Pipeline is configured via `mage-files/` and `commands.txt` (startup and deploy commands).

---

## Analytics Query (BigQuery SQL)

A final aggregated analytics table is built by JOINing all dimension tables to the fact table:

```sql
CREATE OR REPLACE TABLE `project.uber_data_engineering_yt.tbl_analytics` AS (
SELECT
    f.trip_id,
    f.VendorID,
    d.tpep_pickup_datetime,
    d.tpep_dropoff_datetime,
    p.passenger_count,
    t.trip_distance,
    r.rate_code_name,
    pick.pickup_latitude,
    pick.pickup_longitude,
    drop.dropoff_latitude,
    drop.dropoff_longitude,
    pay.payment_type_name,
    f.fare_amount,
    f.extra,
    f.mta_tax,
    f.tip_amount,
    f.tolls_amount,
    f.improvement_surcharge,
    f.total_amount
FROM fact_table f
JOIN datetime_dim d         ON f.datetime_id = d.datetime_id
JOIN passenger_count_dim p  ON f.passenger_count_id = p.passenger_count_id
JOIN trip_distance_dim t    ON f.trip_distance_id = t.trip_distance_id
JOIN rate_code_dim r        ON f.rate_code_id = r.rate_code_id
JOIN pickup_location_dim pick ON f.pickup_location_id = pick.pickup_location_id
JOIN dropoff_location_dim drop ON f.dropoff_location_id = drop.dropoff_location_id
JOIN payment_type_dim pay   ON f.payment_type_id = pay.payment_type_id
);
```

This flat analytics table is then connected to **Looker Studio** for interactive dashboarding (trip heatmaps, fare distribution, payment type breakdown, peak hour analysis).

---

## File Structure

| File | Contents |
|---|---|
| `Uber Data Pipeline (Fixed Version).ipynb` | Final pandas star schema transformation notebook |
| `Uber Data Pipeline (Video Version).ipynb` | Tutorial-version notebook for reference |
| `analytics_query.sql` | BigQuery analytics table creation SQL |
| `architecture.jpg` | Full pipeline architecture diagram |
| `data_model.jpeg` | Star schema entity-relationship diagram |
| `data/uber_data.csv` | Raw NYC TLC trip record CSV (sample) |
| `mage-files/` | Mage.ai pipeline configuration files |
| `commands.txt` | GCP VM setup and Mage deployment commands |

---

## GCP Services Used

| Service | Role |
|---|---|
| **Cloud Storage (GCS)** | Source data bucket (raw CSV) |
| **Compute Engine VM** | Hosts the Mage.ai pipeline server |
| **BigQuery** | Data warehouse: stores dimension tables, fact table, analytics table |
| **Looker Studio** | Connected BI dashboard (connects directly to BigQuery) |

---

## Tech Stack

- **Language**: Python 3
- **Data processing**: pandas, NumPy
- **Orchestration**: Mage.ai (open-source)
- **Cloud**: Google Cloud Platform (GCS, Compute Engine, BigQuery, Looker Studio)
- **SQL**: BigQuery Standard SQL (CREATE OR REPLACE TABLE AS SELECT, multi-table JOINs)

---

## References

- Mage.ai open-source project: https://github.com/mage-ai/mage-ai
- NYC TLC Trip Record Data: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Data Dictionary: https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf
