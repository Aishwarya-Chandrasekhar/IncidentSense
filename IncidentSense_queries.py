"""
Author: Aishwarya Chandrasekhar
Version: 1
Date of version: Sep 2025

"""

import copy
from google.cloud import bigquery
import warnings
import pytz


warnings.filterwarnings("ignore")


sydney_tz = pytz.timezone("Australia/Sydney")



# %%%%%
DATASET_ID   = "bqai_disaster"
TARGET_TABLE = f"{DATASET_ID}.openaq_v3_measurements"

API_KEY      =  "f2e6e352d2e1ce7c7f295112b80c2c14b646fc92db195ef8d994cd99805f77db"  # put your real key or set env
POLLUTANTS   = ["pm25", "pm10"]
DAYS_BACK    = 30
PAGE_LIMIT   = 1000
SLEEP_SEC    = 0.15
BASE_URL     = "https://api.openaq.org/v3"
HEADERS      = {"X-API-Key": API_KEY, "User-Agent": "bq-notebook/1.0"}
# %%%%%


database_creation = '''CREATE SCHEMA IF NOT EXISTS `bqai_disaster`;'''

incidents_raw = '''
CREATE OR REPLACE TABLE `bqai_disaster.incidents_raw` AS

-- 1) NOAA Storms
SELECT
  'storm' AS hazard_type,
  event_id,
  event_begin_time AS start_time,
  event_end_time   AS end_time,
  ST_GEOGPOINT(event_longitude, event_latitude) AS geom,
  event_type,
  CAST(magnitude AS FLOAT64) AS severity,
  COALESCE(deaths_direct,0)+COALESCE(deaths_indirect,0) AS fatalities,
  COALESCE(injuries_direct,0)+COALESCE(injuries_indirect,0) AS injuries,
  COALESCE(damage_property,0)+COALESCE(damage_crops,0) AS damage_usd,
  "NOAA Storms" AS source
FROM `bigquery-public-data.noaa_historic_severe_storms.storms_*`

UNION ALL

-- 2) IBTrACS tropical cyclones
SELECT
  'hurricane' AS hazard_type,
  SID AS event_id,
  SAFE_CAST(ISO_TIME AS DATETIME) AS start_time,
  SAFE_CAST(ISO_TIME AS DATETIME) AS end_time,
  ST_GEOGPOINT(SAFE_CAST(LON AS FLOAT64), SAFE_CAST(LAT AS FLOAT64)) AS geom,
  "tropical cyclone" AS event_type,
  SAFE_CAST(WMO_WIND AS FLOAT64) AS severity,
  NULL AS fatalities,
  NULL AS injuries,
  NULL AS damage_usd,
  "IBTrACS v04r01" AS source
FROM `bqai_disaster.climate_stewardship_ibtracs`
WHERE SAFE_CAST(ISO_TIME AS DATETIME) IS NOT NULL
  AND EXTRACT(YEAR FROM SAFE_CAST(ISO_TIME AS DATETIME)) BETWEEN 1901 AND 2025

UNION ALL

-- 3) HazEL Earthquakes
(
  WITH src AS (
  SELECT
    CAST(id AS STRING) AS id,
    SAFE_CAST(year AS INT64)    AS y,
    SAFE_CAST(month AS INT64)   AS m,
    SAFE_CAST(day AS INT64)     AS d,
    SAFE_CAST(hour AS INT64)    AS hh,
    SAFE_CAST(minute AS INT64)  AS mm,
    COALESCE(SAFE_CAST(second AS FLOAT64), SAFE_CAST(second AS INT64)) AS ss,
    SAFE_CAST(latitude AS FLOAT64)  AS lat,
    SAFE_CAST(longitude AS FLOAT64) AS lon,
    SAFE_CAST(eqMagnitude AS FLOAT64) AS severity
  FROM `bqai_disaster.earthquakes_noaa_hazel`
)
SELECT
  'earthquake' AS hazard_type,
  id           AS event_id,
  CASE WHEN y>=1 AND m BETWEEN 1 AND 12 AND d BETWEEN 1 AND 31
         AND hh BETWEEN 0 AND 23 AND mm BETWEEN 0 AND 59
         AND COALESCE(ss,0) BETWEEN 0 AND 59.9999
       THEN DATETIME(y,m,d,hh,mm,CAST(FLOOR(COALESCE(ss,0)) AS INT64)) END AS start_time,
  CASE WHEN y>=1 AND m BETWEEN 1 AND 12 AND d BETWEEN 1 AND 31
         AND hh BETWEEN 0 AND 23 AND mm BETWEEN 0 AND 59
         AND COALESCE(ss,0) BETWEEN 0 AND 59.9999
       THEN DATETIME(y,m,d,hh,mm,CAST(FLOOR(COALESCE(ss,0)) AS INT64)) END AS end_time,
  CASE WHEN lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180
       THEN ST_GEOGPOINT(lon,lat) END AS geom,
  'earthquake' AS event_type,
  severity,
  NULL AS fatalities,
  NULL AS injuries,
  NULL AS damage_usd,
  'NOAA Significant Earthquakes (HazEL)' AS source
FROM src
WHERE lat BETWEEN -90 AND 90
  AND lon BETWEEN -180 AND 180
  AND y BETWEEN 1901 AND 2025
)

'''



gdelt_news = '''
CREATE OR REPLACE TABLE `bqai_disaster.gdelt_recent_sig`
PARTITION BY DATE(event_time)
CLUSTER BY lat_bin, lon_bin AS
SELECT
  SAFE.PARSE_TIMESTAMP('%Y%m%d', CAST(e.SQLDATE AS STRING)) AS event_time,  -- midnight UTC
  SAFE.ST_GEOGPOINT(e.ActionGeo_Long, e.ActionGeo_Lat) AS geom,
  e.AvgTone  AS tone,
  e.NumArticles AS num_articles,
  CAST(FLOOR(e.ActionGeo_Lat * 2) AS INT64)  AS lat_bin,   -- 0.5° bins
  CAST(FLOOR(e.ActionGeo_Long * 2) AS INT64) AS lon_bin
FROM `gdelt-bq.gdeltv2.events` e
WHERE e.SQLDATE BETWEEN
        CAST(FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)) AS INT64)
    AND CAST(FORMAT_DATE('%Y%m%d', CURRENT_DATE()) AS INT64)
  AND e.ActionGeo_Lat  BETWEEN -90  AND 90
  AND e.ActionGeo_Long BETWEEN -180 AND 180
  AND e.AvgTone IS NOT NULL
  AND e.NumArticles IS NOT NULL
  AND e.ActionGeo_Lat IS NOT NULL
  AND e.ActionGeo_Long IS NOT NULL;
'''


gdelt_news_compute_metrics = '''
CREATE OR REPLACE TABLE `bqai_disaster.news_metrics_recent` AS
WITH inc AS (
  SELECT
    event_id,
    geom,
    TIMESTAMP(start_time) AS start_ts,
    TIMESTAMP(COALESCE(end_time, start_time)) AS end_ts,
    CAST(FLOOR(ST_Y(geom) * 2) AS INT64)  AS lat_bin,
    CAST(FLOOR(ST_X(geom) * 2) AS INT64)  AS lon_bin
  FROM `bqai_disaster.incidents_raw`
  WHERE geom IS NOT NULL
),
-- join only nearby bins (±2 in each direction ≈ ±1°, ~111 km)
candidates AS (
  SELECT
    i.event_id,
    s.event_time,
    s.geom AS s_geom,
    s.tone
  FROM inc i
  JOIN UNNEST([-2,-1,0,1,2]) AS dy
  JOIN UNNEST([-2,-1,0,1,2]) AS dx
  JOIN `bqai_disaster.gdelt_recent_sig` s
    ON s.lat_bin = i.lat_bin + dy
   AND s.lon_bin = i.lon_bin + dx
  WHERE s.event_time BETWEEN TIMESTAMP_SUB(i.start_ts, INTERVAL 24 HOUR)
                         AND TIMESTAMP_ADD(i.end_ts,  INTERVAL 24 HOUR)
    AND ST_DWITHIN(i.geom, s.geom, 100000)     -- final precise filter (100 km)
)
SELECT
  event_id,
  COUNT(*)              AS news_articles_48h,
  AVG(tone)             AS news_tone_48h
FROM candidates
GROUP BY event_id;

'''



population_with_geom = '''
CREATE OR REPLACE TABLE `bqai_disaster.population_with_geom` AS
SELECT
  d.id,
  d.names.primary AS country_name,
  d.country       AS iso_code,
  d.geometry      AS geom,              -- keep raw geometry
  p.year,
  p.midyear_population
FROM `bigquery-public-data.overture_maps.division_area` d
JOIN `bigquery-public-data.census_bureau_international.midyear_population` p
  ON UPPER(d.country) = p.country_code
WHERE d.subtype = 'country'
  AND d.geometry IS NOT NULL;
'''


infrastructure_for_country = '''
# CREATE OR REPLACE TABLE `bqai_disaster.incidents_roads_90d` AS
CREATE OR REPLACE TABLE `bqai_disaster.incidents_roads_10d` AS --still takes 3 hrs
WITH incidents AS (
  SELECT
    event_id,
    hazard_type,
    start_time,
    end_time,
    geom
  FROM `bqai_disaster.incidents_raw`
  WHERE 
  # start_time >= DATETIME(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY))
 start_time >= DATETIME(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY))
    AND geom IS NOT NULL
),
countries AS (
  SELECT
    id,
    d.names.primary AS country_name,
    d.country       AS iso_code,
    d.geometry      AS geom
  FROM `bigquery-public-data.overture_maps.division_area` d
  WHERE d.subtype = 'country'
    AND d.geometry IS NOT NULL
),
roads AS (
  SELECT geometry
  FROM `bigquery-public-data.overture_maps.segment`
  WHERE geometry IS NOT NULL
),
roads_by_country AS (
  SELECT
    c.id AS country_id,
    ANY_VALUE(c.country_name) AS country_name,
    ANY_VALUE(c.iso_code)     AS iso_code,
    SUM(ST_LENGTH(ST_INTERSECTION(r.geometry, c.geom))) / 1000.0 AS road_km
  FROM countries c
  JOIN roads r
    ON ST_INTERSECTS(r.geometry, c.geom)
  GROUP BY c.id
)
SELECT
  i.event_id,
  i.hazard_type,
  i.start_time,
  i.end_time,
  c.country_name,
  c.iso_code,
  rbc.road_km
FROM incidents i
LEFT JOIN countries c
  ON ST_WITHIN(i.geom, c.geom)
LEFT JOIN roads_by_country rbc
  ON rbc.country_id = c.id;
'''

incidents_enriched = '''
CREATE OR REPLACE TABLE `bqai_disaster.incidents_enriched`
-- PARTITION BY DATE_TRUNC(DATE(start_time), MONTH)
CLUSTER BY hazard_type, iso_code AS
-- Countries reference (subset to valid country geoms)
WITH countries AS (
  SELECT
    d.id,
    d.country            AS iso_code,      -- ISO 3166-1 alpha-2
    d.names.primary      AS country_name,
    d.geometry           AS geom,          -- GEOGRAPHY
    SAFE_CAST(d.is_land AS BOOL)  AS is_land,
    SAFE_CAST(d.version AS INT64) AS version
  FROM `bigquery-public-data.overture_maps.division_area` d
  WHERE d.subtype = 'country'
    AND d.geometry IS NOT NULL
),

-- Attach country to each incident (prefer land polygons & latest version)
i_with_country AS (
  SELECT
    i.*,
    c.iso_code,
    c.country_name,
    ROW_NUMBER() OVER (
      PARTITION BY i.event_id
      ORDER BY c.is_land DESC, c.version DESC
    ) AS rn
  FROM `bqai_disaster.incidents_raw` AS i
  LEFT JOIN countries AS c
    ON SAFE.ST_WITHIN(i.geom, c.geom)     -- SAFE.* guards against rare invalid geoms
),
incidents AS (
  SELECT * EXCEPT(rn)
  FROM i_with_country
  WHERE rn = 1 OR rn IS NULL
),

# -- Incident-level roads (last 90d) already computed elsewhere
# roads_incident90 AS (
#   SELECT event_id, road_km
#   # FROM `bqai_disaster.incidents_roads_90d`
#   FROM `bqai_disaster.incidents_roads_10d`
# ),

-- Limit country fallback to countries that actually appear in these incidents
target_iso AS (
  SELECT DISTINCT iso_code
  FROM incidents
  WHERE iso_code IS NOT NULL
),

countries_needed AS (
  SELECT c.*
  FROM countries c
  JOIN target_iso t
    ON t.iso_code = c.iso_code
),

-- Heavy step (computed only for needed countries): total road length per country (km)
roads_by_country_fallback AS (
  SELECT
    c.iso_code,
    SUM(
      ST_LENGTH(
        -- SAFE.ST_INTERSECTION(r.geometry, c.geom)  -- SAFE.* to avoid geometry errors
        ST_INTERSECTION(r.geometry, c.geom)  -- SAFE.* to avoid geometry errors
      )
    ) / 1000.0 AS road_km
  FROM countries_needed c
  JOIN `bigquery-public-data.overture_maps.segment` r
    ON SAFE.ST_INTERSECTS(r.geometry, c.geom)
  GROUP BY c.iso_code
)

-- Final enrich
SELECT
  i.*,
  p.midyear_population AS population_estimate,
  -- COALESCE(ir.road_km, rb.road_km) AS road_km_country

  COALESCE( rb.road_km, 100) AS road_km_country --giving a default 100
FROM incidents AS i
LEFT JOIN `bqai_disaster.population_with_geom` AS p
  ON p.iso_code = i.iso_code
 AND p.year     = EXTRACT(YEAR FROM i.start_time)
-- LEFT JOIN roads_incident90 AS ir
--   ON ir.event_id = i.event_id
LEFT JOIN roads_by_country_fallback AS rb
  ON rb.iso_code = i.iso_code;
'''


incident_corpus = '''
CREATE OR REPLACE TABLE `bqai_disaster.incident_corpus` AS
WITH base AS (
  SELECT
    e.event_id,
    e.hazard_type,
    e.country_name,
    e.iso_code,
    e.severity,
    e.fatalities,
    e.injuries,
    e.damage_usd,
    e.source,
    e.start_time,
    e.population_estimate,
    e.road_km_country,
    aq.pm25_before,
    aq.pm25_after,
    aq.pm10_before,
    aq.pm10_after,
    nm.news_articles_48h,
    nm.news_tone_48h
  FROM `bqai_disaster.incidents_enriched` e
  -- LEFT JOIN `bqai_disaster.air_quality_metrics` aq USING (event_id)
  LEFT JOIN `bqai_disaster.openaq_locations_near_incidents` aq USING (event_id)
  LEFT JOIN `bqai_disaster.news_metrics_recent` nm USING (event_id)
)
SELECT
  event_id,
  CONCAT(
    'Type=', IFNULL(hazard_type, 'unknown'),
    '; Location=', IFNULL(country_name, 'unknown'),
    '; ISO=', IFNULL(iso_code, 'NA'),
    '; When=', IFNULL(FORMAT_TIMESTAMP('%Y-%m-%dT%H:%MZ', TIMESTAMP(start_time)), 'NA'),
    '; Severity=', IFNULL(CAST(severity AS STRING), 'NA'),
    '; Fatalities=', IFNULL(CAST(fatalities AS STRING), '0'),
    '; Injuries=', IFNULL(CAST(injuries AS STRING), '0'),
    '; DamageUSD=', IFNULL(CAST(damage_usd AS STRING), '0'),
    '; Pop=', IFNULL(CAST(CAST(population_estimate AS INT64) AS STRING), 'NA'),
    '; RoadsKM=', IFNULL(CAST(ROUND(road_km_country, 1) AS STRING), 'NA'),
    -- AQ deltas (only if we have data)
    '; PM25_before=', IFNULL(CAST(ROUND(pm25_before, 1) AS STRING), 'NA'),
    '; PM25_after=',  IFNULL(CAST(ROUND(pm25_after,  1) AS STRING), 'NA'),
    '; PM10_before=', IFNULL(CAST(ROUND(pm10_before, 1) AS STRING), 'NA'),
    '; PM10_after=',  IFNULL(CAST(ROUND(pm10_after,  1) AS STRING), 'NA'),
    -- News
    '; NewsArticles48h=', IFNULL(CAST(news_articles_48h AS STRING), '0'),
    '; NewsTone48h=',     IFNULL(CAST(ROUND(news_tone_48h, 3) AS STRING), 'NA'),
    '; Source=', IFNULL(source, 'NA')
  ) AS description
FROM base;
'''



embedding_model = '''
CREATE OR REPLACE MODEL `bqai_disaster.embedding_text`
REMOTE WITH CONNECTION `incidentsense.us.vertex_ai`
OPTIONS (
  endpoint = 'text-embedding-004'   -- or full URL; region must match the connection
);
'''


semantic_vector_creation = '''
CREATE `bqai_disaster.incident_embeddings_arr` AS
# CREATE OR REPLACE TABLE `bqai_disaster.incident_embeddings_arr` AS
SELECT
  event_id,
  ml_generate_embedding_result AS emb
FROM ML.GENERATE_EMBEDDING(
  MODEL `bqai_disaster.embedding_text`,
  (
    SELECT event_id, description AS content
    FROM `bqai_disaster.incident_corpus`
  ),
  -- Optional: set task type and vector size
  STRUCT(TRUE AS flatten_json_output, 'RETRIEVAL_DOCUMENT' AS task_type, 768 AS output_dimensionality)
);
'''


incident_embeddings_wide = '''
CREATE OR REPLACE TABLE `bqai_disaster.incident_embeddings_wide` AS
SELECT
  i.event_id,
  i.hazard_type,
  DATE(i.start_time) AS event_date,
  i.country_name,
  e.emb          -- FLOAT64[] (REPEATED) from your current table
FROM `bqai_disaster.incidents_enriched` i
JOIN `bqai_disaster.incident_embeddings_arr` e
  USING (event_id);
'''


similar_incidents_top5 = '''
CREATE OR REPLACE TABLE `bqai_disaster.similar_incidents_top5` AS
SELECT
  h.query.event_id AS query_event,
  h.base.event_id  AS neighbor_event,
  CAST(h.distance AS FLOAT64)       AS cosine_distance,
  1.0 - CAST(h.distance AS FLOAT64) AS cosine_similarity
FROM VECTOR_SEARCH(
  -- BASE with true prefiltering (needs columns present + in index STORING)
  (SELECT event_id, emb, hazard_type, event_date, country_name
   FROM `bqai_disaster.incident_embeddings_wide`
   WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)),
  'emb',
  -- QUERY set (batch recent incidents only)
  (SELECT event_id, emb
   FROM `bqai_disaster.incident_embeddings_wide`
   WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)),
  'emb',
  top_k         => 6,
  distance_type => 'COSINE',
  options       => '{"fraction_lists_to_search": 0.02}'
) AS h
WHERE h.base.event_id != h.query.event_id
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY h.query.event_id ORDER BY h.distance
) <= 5;
'''


impact_score = '''
CREATE OR REPLACE TABLE `bqai_disaster.incident_features_recent` AS
WITH base AS (
  SELECT
    i.event_id,
    i.hazard_type,
    i.start_time,
    i.country_name,
    i.severity,
    SAFE.LOG10(1 + COALESCE(i.population,0)) AS log_pop,
    COALESCE(n.news_articles_48h, 0) AS news_articles_48h,
    COALESCE(n.news_tone_48h, 0.0)   AS news_tone_48h
  FROM `bqai_disaster.incidents_enriched` i
  LEFT JOIN `bqai_disaster.news_metrics_recent` n USING(event_id)
),
by_hazard AS (
  SELECT
    hazard_type,
    AVG(severity) AS sev_mean,
    STDDEV_POP(severity) AS sev_std,
    AVG(log_pop) AS pop_mean,
    STDDEV_POP(log_pop) AS pop_std,
    AVG(news_articles_48h) AS news_mean,
    STDDEV_POP(news_articles_48h) AS news_std
  FROM base
  GROUP BY hazard_type
)
SELECT
  b.event_id, b.hazard_type, b.start_time, b.country_name,
  b.severity,
  b.log_pop,
  b.news_articles_48h, b.news_tone_48h,
  -- Z-scores with SAFE divide
  (b.severity - h.sev_mean) / NULLIF(h.sev_std,0)           AS z_severity,
  (b.log_pop - h.pop_mean) / NULLIF(h.pop_std,0)            AS z_log_pop,
  (b.news_articles_48h - h.news_mean) / NULLIF(h.news_std,0) AS z_news,
  -- Simple interpretable Impact Score (0–100-ish)
  LEAST(100,
    20 + 20*COALESCE((b.severity - h.sev_mean)/NULLIF(h.sev_std,0),0) +
         20*COALESCE((b.log_pop  - h.pop_mean)/NULLIF(h.pop_std,0),0) +
         10*COALESCE((b.news_articles_48h - h.news_mean)/NULLIF(h.news_std,0),0) +
          5*COALESCE(b.news_tone_48h/10.0, 0)      -- tone is roughly [-10,10]
  ) AS impact_score
FROM base b
JOIN by_hazard h USING(hazard_type);
'''


llm_model = '''
CREATE OR REPLACE MODEL `bqai_disaster.gemini_flash`
REMOTE WITH CONNECTION `incidentsense.us.vertex_ai`
OPTIONS (
  endpoint = 'gemini-2.0-flash'  -- any supported Gemini model is fine
);
'''

incident_brief_ml_generate = '''
CREATE OR REPLACE TABLE `bqai_disaster.incident_briefs_ml_generate_text` AS
WITH
-- 1) Classify hazard buckets for every event (scalar AI.GENERATE)
tags AS (
  SELECT
    f.event_id,
    AI.GENERATE(
      prompt => CONCAT(
        'Classify into exactly one of [wind, flood, seismic, cyclone, winter, wildfire, other] ',
        'based on the short description that follows. Return ONLY the label. ',
        'Text: ',
        f.hazard_type, ' ',
        IFNULL(f.country_name, ''),
        ' sev=', CAST(f.severity AS STRING)
      ),
      connection_id => 'us.vertex-ai',
      endpoint      => 'gemini-2.0-flash'
    ).result AS hazard_bucket
  FROM `bqai_disaster.incident_features_recent` f
),

-- 2) Build all query–neighbor pairs with features 
joined AS (
  SELECT
    s.query_event,
    s.neighbor_event,
    s.cosine_similarity,
    q.hazard_type           AS query_type,
    q.country_name          AS query_country,
    q.start_time            AS query_start,
    q.severity              AS query_severity,
    q.impact_score          AS query_impact,
    n.hazard_type           AS neigh_type,
    n.country_name          AS neigh_country,
    n.start_time            AS neigh_start,
    n.severity              AS neigh_severity,
    n.impact_score          AS neigh_impact
  FROM `bqai_disaster.similar_incidents_top5` s
  JOIN `bqai_disaster.incident_features_recent` q
    ON q.event_id = s.query_event
  JOIN `bqai_disaster.incident_features_recent` n
    ON n.event_id = s.neighbor_event
),

-- 3) Attach hazard buckets and compute deltas 
enriched AS (
  SELECT
    j.*,
    tq.hazard_bucket AS query_bucket,
    tn.hazard_bucket AS neigh_bucket,
    (j.query_severity - j.neigh_severity)   AS delta_severity,
    (j.query_impact  - j.neigh_impact)      AS delta_impact
  FROM joined j
  LEFT JOIN tags tq ON tq.event_id = j.query_event
  LEFT JOIN tags tn ON tn.event_id = j.neighbor_event
)

-- 4) Call the remote LLM once per row with a rich prompt
SELECT
  query_event,
  neighbor_event,
  cosine_similarity,
  ml_generate_text_llm_result AS similarity_brief
FROM ML.GENERATE_TEXT(
  MODEL `bqai_disaster.gemini_flash`,
  (
    SELECT
      query_event,
      neighbor_event,
      cosine_similarity,
      CONCAT(
        'Write a concise (80–120 words) disaster similarity brief for an emergency manager.', '\n',
        'Query: type=', query_type,
          ', country=', IFNULL(query_country, 'Unknown'),
          ', start=', CAST(query_start AS STRING),
          ', severity=', CAST(query_severity AS STRING),
          ', impact_score=', CAST(query_impact AS STRING),
          ', bucket=', IFNULL(query_bucket, 'unknown'), '.', '\n',
        'Neighbor: type=', neigh_type,
          ', country=', IFNULL(neigh_country, 'Unknown'),
          ', start=', CAST(neigh_start AS STRING),
          ', severity=', CAST(neigh_severity AS STRING),
          ', impact_score=', CAST(neigh_impact AS STRING),
          ', bucket=', IFNULL(neigh_bucket, 'unknown'), '.', '\n',
        'Cosine similarity=', CAST(cosine_similarity AS STRING), '.', '\n',
        'Deltas (query - neighbor): severity=', CAST(delta_severity AS STRING),
          ', impact_score=', CAST(delta_impact AS STRING), '.', '\n',
        'Explain WHY they are similar (hazard type, region, population exposure, recency). ',
        'Then provide 3 actionable bullet points for the QUERY incident. ',
        'Keep actions concrete, short, and non-duplicative.'
      ) AS prompt
    FROM enriched
  ),
  STRUCT(
    512  AS max_output_tokens,
    0.2  AS temperature,
    TRUE AS flatten_json_output
  )
)
'''


incdent_brief_ai_generate = '''
CREATE OR REPLACE TABLE `bqai_disaster.incident_briefs_ai_generate` AS
WITH
-- 1) Classify hazard buckets for every event
tags AS (
  SELECT
    f.event_id,
    AI.GENERATE(
      prompt => CONCAT(
        'Classify into exactly one of [wind, flood, seismic, cyclone, winter, wildfire, other] ',
        'based on the short description that follows. Return ONLY the label. ',
        'Text: ',
        f.hazard_type, ' ',
        IFNULL(f.country_name, ''),
        ' sev=', CAST(f.severity AS STRING)
      ),
      connection_id => 'us.vertex-ai',
      endpoint      => 'gemini-2.0-flash'
    ).result AS hazard_bucket
  FROM `bqai_disaster.incident_features_recent` f
),

-- 2) Build all query–neighbor pairs with features
joined AS (
  SELECT
    s.query_event,
    s.neighbor_event,
    s.cosine_similarity,
    q.hazard_type  AS query_type,
    q.country_name AS query_country,
    q.start_time   AS query_start,
    q.severity     AS query_severity,
    q.impact_score AS query_impact,
    n.hazard_type  AS neigh_type,
    n.country_name AS neigh_country,
    n.start_time   AS neigh_start,
    n.severity     AS neigh_severity,
    n.impact_score AS neigh_impact
  FROM `bqai_disaster.similar_incidents_top5` s
  JOIN `bqai_disaster.incident_features_recent` q ON q.event_id = s.query_event
  JOIN `bqai_disaster.incident_features_recent` n ON n.event_id = s.neighbor_event
),

-- 3) Attach hazard buckets and compute deltas
enriched AS (
  SELECT
    j.*,
    tq.hazard_bucket AS query_bucket,
    tn.hazard_bucket AS neigh_bucket,
    (j.query_severity - j.neigh_severity) AS delta_severity,
    (j.query_impact  - j.neigh_impact)    AS delta_impact
  FROM joined j
  LEFT JOIN tags tq ON tq.event_id = j.query_event
  LEFT JOIN tags tn ON tn.event_id = j.neighbor_event
)

-- 4) Generate similarity brief with richer context
SELECT
  query_event,
  neighbor_event,
  cosine_similarity,
  AI.GENERATE(
    prompt => CONCAT(
      'Write a concise (80–120 words) disaster similarity brief for an emergency manager.', '\n',
      'Query: type=', query_type,
        ', country=', IFNULL(query_country, 'Unknown'),
        ', start=', CAST(query_start AS STRING),
        ', severity=', CAST(query_severity AS STRING),
        ', impact_score=', CAST(query_impact AS STRING),
        ', bucket=', IFNULL(query_bucket, 'unknown'), '.', '\n',
      'Neighbor: type=', neigh_type,
        ', country=', IFNULL(neigh_country, 'Unknown'),
        ', start=', CAST(neigh_start AS STRING),
        ', severity=', CAST(neigh_severity AS STRING),
        ', impact_score=', CAST(neigh_impact AS STRING),
        ', bucket=', IFNULL(neigh_bucket, 'unknown'), '.', '\n',
      'Cosine similarity=', CAST(cosine_similarity AS STRING), '.', '\n',
      'Deltas (query - neighbor): severity=', CAST(delta_severity AS STRING),
        ', impact_score=', CAST(delta_impact AS STRING), '.', '\n',
      'Explain WHY they are similar (hazard type, region, population exposure, recency). ',
      'Then provide 3 actionable bullet points for the QUERY incident. ',
      'Keep actions concrete, short, and non-duplicative.'
    ),
    connection_id => 'us.vertex-ai',
    endpoint      => 'gemini-2.0-flash'
  ).result AS similarity_brief
FROM enriched;
'''


action_plan = '''
CREATE OR REPLACE TABLE `bqai_disaster.incident_actions` AS
WITH ctx AS (
  SELECT
    b.query_event,
    STRING_AGG(
      CONCAT(n.neighbor_event, ' (sim=', FORMAT('%0.2f', n.cosine_similarity), ')'),
      ', '
      ORDER BY n.cosine_similarity DESC
    ) AS neighbor_list
  FROM `bqai_disaster.incident_briefs_ml_generate_text` b
  JOIN `bqai_disaster.similar_incidents_top5` n USING (query_event, neighbor_event)
  GROUP BY b.query_event
),
gen AS (
  SELECT
    query_event,
    -- Ask LLM to return only a JSON array of 5 objects
    ml_generate_text_llm_result AS json_actions
  FROM ML.GENERATE_TEXT(
    MODEL `bqai_disaster.gemini_flash`,
    (
      SELECT
        query_event,
        CONCAT(
          'You are assisting an emergency operations chief. ',
          'Return ONLY a JSON array with EXACTLY 5 objects. ',
          'Each object must have keys: "priority" (integer 1-5, where 1=highest), ',
          '"action" (string, <=90 chars), ',
          '"owner" (one of: "Ops","Logistics","Planning","Comms","Safety"), ',
          '"eta_hours" (integer). ',
          'No markdown, no extra prose, no code fences.', '\n',
          'Context: Query incident id=', query_event, '. ',
          'Top neighbors: ', neighbor_list, '. ',
          'Create concrete, short, non-duplicative actions tied to incident ops.'
        ) AS prompt
      FROM ctx
    ),
    STRUCT(
      256 AS max_output_tokens,
      0.2 AS temperature,
      TRUE AS flatten_json_output
    )
  )
)
SELECT
  query_event,
  SAFE_CAST(JSON_VALUE(item, '$.priority') AS INT64)   AS priority,
  JSON_VALUE(item, '$.action')                          AS action,
  JSON_VALUE(item, '$.owner')                           AS owner,
  SAFE_CAST(JSON_VALUE(item, '$.eta_hours') AS INT64)   AS eta_hours
FROM gen,
UNNEST(JSON_QUERY_ARRAY(json_actions)) AS item
ORDER BY query_event, priority;

'''


incident_plan_example = '''
Select * from `bqai_disaster.incident_actions` limit 10
'''