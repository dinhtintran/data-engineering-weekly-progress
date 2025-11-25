DROP TABLE IF EXISTS places;

.mode csv
.import /Users/tin/Desktop/data-engineer-weekly-progress/data-engineering-weekly-progress/week1/google-places-cleaning-and-ranking/data/clean/billiard_clean_place.csv places

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
