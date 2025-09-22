import IncidentSense_helper as h
import IncidentSense_queries as q
import requests
import pandas as pd
from google.cloud import bigquery
import random
import time


ic = h.IncidentSense()


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

    ''' A: IBTrACS DATA '''
    get_ibtracs_data()

    ''' B: NOAA data - hurricanes & cyclones'''
    get_noaa_data()





if __name__ == '__main__':
    main()