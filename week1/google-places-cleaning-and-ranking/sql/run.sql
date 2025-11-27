DROP TABLE IF EXISTS places;

.mode csv
.import /Users/tin/Desktop/data-engineer-weekly-progress/data-engineering-weekly-progress/week1/google-places-cleaning-and-ranking/data/clean/coffeeshop_clean_place.csv places

ALTER TABLE places ADD COLUMN main_type TEXT;

UPDATE places
SET main_type =
    TRIM(
        REPLACE(
            REPLACE(
                REPLACE(types, '[', ''),
                ']', ''),
            '"', '')
    )
WHERE main_type IS NULL OR main_type = '';

-- Create place_ranking table using DENSE_RANK() window function
DROP TABLE IF EXISTS place_ranking;

CREATE TABLE place_ranking AS
SELECT
    place_id,
    name,
    main_type,
    rating,
    user_ratings_total,
    latitude,
    longitude,
    address,
    DENSE_RANK() OVER (
        PARTITION BY main_type
        ORDER BY rating DESC, user_ratings_total DESC
    ) AS rating_rank
FROM places;

-- View top 20 ranked places
SELECT * FROM place_ranking ORDER BY main_type, rating_rank LIMIT 20;

.headers on
.mode csv
.output /Users/tin/Desktop/data-engineer-weekly-progress/data-engineering-weekly-progress/week1/google-places-cleaning-and-ranking/output/ranked_places.csv

SELECT
    place_id,
    name,
    rating,
    user_ratings_total,
    COALESCE(main_type, types) AS category,
    RANK() OVER (
        PARTITION BY COALESCE(main_type, types)
        ORDER BY rating DESC, user_ratings_total DESC
    ) AS rating_rank
FROM places
ORDER BY category, rating_rank;

.output stdout
