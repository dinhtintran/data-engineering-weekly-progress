-- Star schema DDL and query plan for Google Places reviews (PostgreSQL)
-- Usage: psql -f star_schema_reviews.sql

-- Clean slate for reruns
DROP TABLE IF EXISTS fact_reviews CASCADE;
DROP TABLE IF EXISTS dim_place CASCADE;

-- Dimension table: one row per place
CREATE TABLE dim_place (
    place_sk      BIGSERIAL PRIMARY KEY,
    place_id      TEXT        NOT NULL UNIQUE,
    name          TEXT        NOT NULL,
    address       TEXT,
    main_type     TEXT,
    types         TEXT[],
    latitude      NUMERIC(9,6),
    longitude     NUMERIC(9,6),
    source_system TEXT        NOT NULL DEFAULT 'google_places',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Fact table: one row per review
CREATE TABLE fact_reviews (
    review_sk      BIGSERIAL PRIMARY KEY,
    place_id       TEXT        NOT NULL REFERENCES dim_place(place_id),
    reviewer_name  TEXT,
    review_rating  SMALLINT    CHECK (review_rating BETWEEN 0 AND 5),
    review_text    TEXT,
    review_time    TIMESTAMPTZ,
    review_language TEXT,
    ingestion_ts   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexing & optimization
CREATE INDEX IF NOT EXISTS dim_place_place_id_idx ON dim_place USING BTREE (place_id);
CREATE INDEX IF NOT EXISTS fact_reviews_place_id_idx ON fact_reviews USING BTREE (place_id);

-- Average rating by place type, leveraging the place_id index during the join
EXPLAIN ANALYZE
SELECT
    COALESCE(dp.main_type, t.type) AS place_type,
    AVG(fr.review_rating)::NUMERIC(3,2) AS avg_rating,
    COUNT(*) AS review_count,
    COUNT(DISTINCT fr.place_id) AS place_count
FROM fact_reviews fr
JOIN dim_place dp ON fr.place_id = dp.place_id
LEFT JOIN LATERAL (
    SELECT unnest(dp.types) AS type
) AS t ON TRUE
GROUP BY COALESCE(dp.main_type, t.type)
ORDER BY avg_rating DESC NULLS LAST;

-- Expected plan notes (PostgreSQL):
--  * The join on fr.place_id = dp.place_id should choose "Index Scan using dim_place_place_id_idx on dim_place dp".
--  * If fact_reviews is small, a Nested Loop is typical; for larger fact sizes, a Hash Join will appear with Bitmap Index Scan on fact_reviews_place_id_idx to probe dim_place via place_id.
--  * Enable timing/BUFFERS for more detail: SET enable_seqscan = on; EXPLAIN (ANALYZE, BUFFERS, VERBOSE) <query>;.
