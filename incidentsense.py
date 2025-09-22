import IncidentSense_helper as h
import IncidentSense_queries as q
import requests
import pandas as pd
from google.cloud import bigquery
import random
import time


ic = h.IncidentSense()

def get_openaq_data():
    import math
    import time
    import requests
    import pandas as pd
    from google.cloud import bigquery

    # =========================
    # You can request an API key from OpenAQ by going to https://explore.openaq.org/register or https://explore.openaq.org/account.
    # =========================
    API_KEY = 'your - api - key '  # YOUR API KEY
    BASE_URL = "https://api.openaq.org/v3/locations"

    BQ_DATASET = "bqai_disaster"
    INCIDENTS_TBL = f"{BQ_DATASET}.incidents_raw"
    OUT_TABLE = f"{BQ_DATASET}.openaq_locations_near_incidents"

    # DAYS_BACK      = 3650
    DAYS_BACK = 1825
    BBOX_RADIUS_KM = 100
    PAGE_LIMIT = 1000  # per OpenAQ docs, max 1000
    SLEEP_SEC = 0.10  # polite pause between pages
    MAX_PAGES = None  # set a small int while testing; None = no cap
    MAX_INCIDENTS = None  # set to e.g. 50 for quick tests

    # =========================
    # Helpers
    # =========================
    def bbox_from_point(lat, lon, radius_km=100):
        """Return bbox [minLon, minLat, maxLon, maxLat] for a given center lat/lon & radius_km."""
        dlat = radius_km / 111.0
        dlon = radius_km / (111.320 * max(math.cos(math.radians(lat)), 1e-6))
        min_lat = max(lat - dlat, -90.0)
        max_lat = min(lat + dlat, 90.0)
        min_lon = lon - dlon
        max_lon = lon + dlon
        if min_lon < -180: min_lon += 360
        if max_lon > 180: max_lon -= 360
        return [float(min_lon), float(min_lat), float(max_lon), float(max_lat)]

    def fetch_locations_for_bbox(bbox, page_limit=1000, max_pages=None, sleep_sec=0.1, headers=None):
        """Yield result dicts from OpenAQ /v3/locations for a given bbox, paging until empty."""
        page = 1
        while True:
            if max_pages is not None and page > max_pages:
                break
            params = {
                "limit": page_limit,
                "page": page,
                # bbox order: minLon,minLat,maxLon,maxLat (OpenAQ v3)
                "bbox": ",".join(str(x) for x in bbox),
            }
            r = requests.get(BASE_URL, params=params, headers=headers, timeout=60)
            if r.status_code == 429:
                # basic backoff on rate limiting
                time.sleep(2.0)
                continue
            r.raise_for_status()
            data = r.json()
            results = data.get("results", [])
            if not results:
                break
            for item in results:
                yield item
            page += 1
            time.sleep(sleep_sec)

    def norm_str(v, *, prefer_keys=("code", "name", "value", "label")):
        """Normalize scalars/dicts to a plain string (or None) for BQ STRING columns."""
        if v is None:
            return None
        if isinstance(v, str):
            return v
        if isinstance(v, (int, float)):
            return str(v)
        if isinstance(v, dict):
            for k in prefer_keys:
                if k in v and v[k] is not None:
                    return str(v[k])
            return None
        # lists/other -> None
        return None

    # =========================
    # Pull recent incidents from BigQuery
    # =========================
    bq = bigquery.Client()

    incidents_sql = f"""
    WITH recent AS (
      SELECT
        hazard_type,
        event_id,
        start_time,
        geom,
        ST_Y(geom) AS lat,
        ST_X(geom) AS lon
      FROM `{INCIDENTS_TBL}`
      WHERE geom IS NOT NULL
        AND start_time >= DATETIME(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {DAYS_BACK} DAY))
    )
    SELECT hazard_type, event_id, lat, lon
    FROM recent
    WHERE lat BETWEEN -90 AND 90
      AND lon BETWEEN -180 AND 180
    """

    incidents_df = bq.query(incidents_sql).result().to_dataframe()
    if MAX_INCIDENTS:
        incidents_df = incidents_df.head(MAX_INCIDENTS)

    print(f"Pulled {len(incidents_df)} incidents to query OpenAQ locations against.")

    # =========================
    # Query OpenAQ REST and collect rows
    # =========================
    headers = {"X-API-Key": API_KEY} if API_KEY else {}

    rows = []
    for _, inc in incidents_df.iterrows():
        lat, lon = float(inc["lat"]), float(inc["lon"])
        bbox = bbox_from_point(lat, lon, BBOX_RADIUS_KM)

        seen_ids = set()
        for loc in fetch_locations_for_bbox(
                bbox=bbox,
                page_limit=PAGE_LIMIT,
                max_pages=MAX_PAGES,
                sleep_sec=SLEEP_SEC,
                headers=headers
        ):
            loc_id = loc.get("id")
            if loc_id in seen_ids:
                continue
            seen_ids.add(loc_id)

            coords = loc.get("coordinates") or {}
            loc_lat = coords.get("latitude")
            loc_lon = coords.get("longitude")

            rows.append({
                "event_id": inc["event_id"],
                "hazard_type": inc["hazard_type"],
                "incident_lat": lat,
                "incident_lon": lon,
                "location_id": loc_id,
                "location_name": loc.get("name"),
                "latitude": loc_lat,
                "longitude": loc_lon,
                "country": loc.get("country"),
                "city": loc.get("city"),
            })

    print(f"Collected {len(rows)} location matches across incidents.")

    # =========================
    # Load to BigQuery
    # =========================
    out_df = pd.DataFrame(rows)
    if out_df.empty:
        print("No OpenAQ locations found for the selected incidents/time window.")
    else:
        # --- Clean/normalize fields that might be dict-shaped ---
        out_df["country"] = out_df["country"].apply(lambda x: norm_str(x, prefer_keys=("code", "name")))
        out_df["city"] = out_df["city"].apply(lambda x: norm_str(x, prefer_keys=("name", "label", "value")))
        out_df["location_name"] = out_df["location_name"].apply(
            lambda x: norm_str(x, prefer_keys=("name", "label", "value")))

        # --- Type coercions to match schema ---
        for c in ["incident_lat", "incident_lon", "latitude", "longitude"]:
            out_df[c] = pd.to_numeric(out_df[c], errors="coerce")
        # Nullable integer for location_id
        out_df["location_id"] = pd.to_numeric(out_df["location_id"], errors="coerce").astype("Int64")

        # --- Load to BigQuery ---
        schema = [
            bigquery.SchemaField("event_id", "STRING"),
            bigquery.SchemaField("hazard_type", "STRING"),
            bigquery.SchemaField("incident_lat", "FLOAT"),
            bigquery.SchemaField("incident_lon", "FLOAT"),
            bigquery.SchemaField("location_id", "INT64"),
            bigquery.SchemaField("location_name", "STRING"),
            bigquery.SchemaField("latitude", "FLOAT"),
            bigquery.SchemaField("longitude", "FLOAT"),
            bigquery.SchemaField("country", "STRING"),
            bigquery.SchemaField("city", "STRING"),
        ]
        job = bq.load_table_from_dataframe(
            out_df,
            OUT_TABLE,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", schema=schema),
        )
        job.result()
        print(f"Loaded {len(out_df)} rows into {OUT_TABLE}")
        return



def get_noaa_data():
    """
    Get Earthquakes data from NOAA website using API call
    :return:
    """

    # --- Step 1: Call the NOAA Earthquake API ---
    BASE_URL = ic.NOAA_API_URL

    # Your filters from the SQL WHERE
    COMMON_PARAMS = {
        "minLatitude": -90,
        "maxLatitude": 90,
        "minLongitude": -180,
        "maxLongitude": 180,
    }

    START_YEAR = 1901  # > 1900
    END_YEAR = 2025  # <= 2025
    REQUEST_TIMEOUT = 60
    SLEEP_BETWEEN_CALLS = 0.2  # be gentle to the API

    def fetch_year(year: int):
        """Fetch one calendar year's records."""
        params = {
            **COMMON_PARAMS,
            "minYear": year,
            "maxYear": year,
        }
        r = requests.get(BASE_URL, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        data = r.json()
        # HazEL returns either {"items":[...]} or a bare list
        items = data.get("items", data) if isinstance(data, dict) else data
        return items or []

    # ---- pull all years, with basic retry/backoff ----
    all_rows = []
    for yr in range(START_YEAR, END_YEAR + 1):
        for attempt in range(3):
            try:
                rows = fetch_year(yr)
                all_rows.extend(rows)
                break
            except requests.HTTPError as e:
                # crude backoff for transient 5xx/429s
                if attempt < 2 and (500 <= e.response.status_code < 600 or e.response.status_code == 429):
                    time.sleep(1.5 * (attempt + 1))
                    continue
                raise
            finally:
                time.sleep(SLEEP_BETWEEN_CALLS)

    # --- Step 2: Convert to Pandas DataFrame ---
    # ---- to DataFrame + de-dupe on 'id' ----
    df = pd.DataFrame(all_rows)

    if df.empty:
        raise SystemExit("No rows returned. Check filters or API availability.")

    if "id" in df.columns:
        df.drop_duplicates(subset=["id"], inplace=True)

    def coerce_int64_nullable(s: pd.Series) -> pd.Series:
        # If series holds non-scalars (lists/dicts), don't try to cast
        if s.apply(lambda x: isinstance(x, (list, dict))).any():
            return s  # leave as-is

        # Replace obvious empties with NA
        s = s.replace(r'^\s*$', pd.NA, regex=True)

        # Work as string, extract first integer if present (handles "1900?", "  1975 ", etc.)
        extracted = (
            s.astype(str)
            .str.strip()
            .str.extract(r'(-?\d+)', expand=False)  # first integer substring
        )

        # Convert to numeric (will be float if not cast), then to nullable Int64
        out = pd.to_numeric(extracted, errors="coerce").astype("Int64")
        return out

    def coerce_float_nullable(s: pd.Series) -> pd.Series:
        # Skip non-scalars
        if s.apply(lambda x: isinstance(x, (list, dict))).any():
            return s
        s = s.replace(r'^\s*$', pd.NA, regex=True)
        # Extract a float-like number (handles "-123.45", "12,345.6" → "12345.6")
        cleaned = (
            s.astype(str)
            .str.strip()
            .str.replace(",", "", regex=False)
            .str.extract(r'(-?\d+(?:\.\d+)?)', expand=False)
        )
        return pd.to_numeric(cleaned, errors="coerce")

    # ---- apply robust typing ----
    if not df.empty:
        int_cols = [c for c in ["year", "month", "day", "hour", "minute", "second"] if c in df.columns]
        float_cols = [c for c in ["latitude", "longitude", "eqMagnitude", "depth"] if c in df.columns]

        for c in int_cols:
            df[c] = coerce_int64_nullable(df[c])

        for c in float_cols:
            df[c] = coerce_float_nullable(df[c])

        # Optional: show which columns still couldn't be cast (useful for debugging)
        # print(df[int_cols + float_cols].dtypes)

    # ---- load to BigQuery ----
    client = bigquery.Client()
    table_id = ic.earthquakes_table

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,  # let BQ infer the schema
    )

    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()

    print(f"Loaded {len(df)} rows into {table_id}")


def get_ibtracs_data():

    # ---------- Inputs ----------

    # Generate a random integer between 1 and 100000 (inclusive)
    random_integer = random.randint(1, 100000)
    bucket_name = f'{ic.ibtracs_bucket_name_prefix}{str(random_integer)}'
    destination_blob_name = ic.ibtracs_destination_blob_name
    url_to_upload = (
        "https://www.ncei.noaa.gov/data/international-best-track-archive-for-climate-"
        "stewardship-ibtracs/v04r01/access/csv/ibtracs.ALL.list.v04r01.csv"
    )
    dataset_id = ic.dataset
    table_name = ic.ibtracs_table
    # ----------------------------

    # Upload the file to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    response = requests.get(url_to_upload, stream=True)
    response.raise_for_status()
    blob.upload_from_string(response.content)
    gcs_uri = f"gs://{bucket_name}/{destination_blob_name}"
    print(f"File from {url_to_upload} uploaded to {gcs_uri}")

    # Load into BigQuery
    bq_client = bigquery.Client()

    # Use your default project if not specified; build fully-qualified table ID
    full_table_id = f"{bq_client.project}.{dataset_id}.{table_name}"

    # Ensure dataset exists (no-op if it already does)
    try:
        bq_client.get_dataset(dataset_id)
    except Exception:
        bq_client.create_dataset(dataset_id)
        print(f"Created dataset: {dataset_id}")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,  # Let BQ infer schema
        skip_leading_rows=1,  # CSV has a header row
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # overwrite each run
        allow_quoted_newlines=True,  # safer for complex CSVs
    )

    load_job = bq_client.load_table_from_uri(
        gcs_uri,
        full_table_id,
        job_config=job_config,
    )
    print(f"Starting BigQuery load job {load_job.job_id} → {full_table_id} ...")
    load_job.result()  # Wait for the job to complete

    table = bq_client.get_table(full_table_id)
    print(
        f"Loaded {table.num_rows} rows and {len(table.schema)} columns into {full_table_id}."
    )


def main():
    ''' Step 0: Create dataset '''
    ic.run_bigquery(q.database_creation)

    '''Step 1 : Collect Data '''

    ''' A: IBTrACS DATA - hurricanes & cyclones '''
    get_ibtracs_data()

    ''' B: NOAA data - earthquakes '''
    get_noaa_data()

    ''' C: Create incidents table by combining all raw data files'''
    ic.run_bigquery(q.incidents_raw)

    ''' D: Get GDELT News Data '''
    ic.run_bigquery(q.gdelt_news)
    ic.run_bigquery(q.gdelt_news_compute_metrics)

    ''' E: Get Air Quality Data from OpenAQ '''
    get_openaq_data()


    ''' Step 2: Exposure and Infrastructure Enrichment '''

    ''' A: Population  '''
    ic.run_bigquery(q.population_with_geom)

    ''' B: Infrastructure  '''
    ic.run_bigquery(q.infrastructure_for_country)

    ''' Step 3: Enrich Incidents Raw table with all data '''
    ic.run_bigquery(q.incidents_enriched)

    ''' Step 4: Canonical text corpus for embeddings '''
    ic.run_bigquery(q.incident_corpus)

    ''' Step 5: Create embedding model '''
    ic.run_bigquery(q.embedding_model)

    ''' Step 6: Semantic Vector Creation '''
    ic.run_bigquery(q.semantic_vector_creation)

    ''' Step 7: Denormalized table with metadata and embeddings '''
    ic.run_bigquery(q.incident_embeddings_wide)

    ''' Step 8: Top 5 Similar Incidents '''
    ic.run_bigquery(q.similar_incidents_top5)

    ''' Step 9: Impact Score creation '''
    ic.run_bigquery(q.impact_score)

    ''' Step 10: Remote LLM model creation '''
    ic.run_bigquery(q.llm_model)

    ''' Step 11: AI Incident Brief Generation  '''
    ic.run_bigquery(q.incident_brief_ml_generate)
    ## optional
    ic.run_bigquery(q.incdent_brief_ai_generate)


    ''' Step 12: AI Generated Action Plan  '''
    ic.run_bigquery(q.action_plan)


    ''' Step 13: Serve Example Action plan  '''
    incident_action_plan = ic.run_bigquery(q.incident_plan_example)
    print(incident_action_plan)







if __name__ == '__main__':
    main()