-- Row count and primary-key uniqueness must match.
SELECT COUNT(*) AS stored_rows, COUNT(DISTINCT release_id) AS distinct_release_ids
FROM releases;

-- Confirm every stored record is a physical Pink Floyd release.
SELECT artist_name, format_category, COUNT(*) AS release_count
FROM releases
GROUP BY artist_name, format_category
ORDER BY release_count DESC;

-- Distribution by decade.
SELECT release_decade, COUNT(*) AS release_count,
       ROUND(AVG(track_count), 2) AS average_tracks
FROM releases
GROUP BY release_decade
ORDER BY release_decade;

-- Multi-condition verification used by the cleaning stage.
SELECT release_id, title, release_year, format_category, media_count, track_count
FROM releases
WHERE release_year >= 2000 AND media_count > 1 AND track_count >= 10
ORDER BY release_year, title;

-- Duplicate check: this query must return zero rows.
SELECT release_id, COUNT(*) AS occurrences
FROM releases
GROUP BY release_id
HAVING COUNT(*) > 1;
