-- Seed data for demonstrating data-diff capabilities.
-- Auto-executed by PostgreSQL on first container startup.

CREATE TABLE ratings_source (
    id         INTEGER PRIMARY KEY,
    user_id    INTEGER NOT NULL,
    movie_id   INTEGER NOT NULL,
    rating     NUMERIC(2,1) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE ratings_target (
    id         INTEGER PRIMARY KEY,
    user_id    INTEGER NOT NULL,
    movie_id   INTEGER NOT NULL,
    rating     NUMERIC(2,1) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

-- Populate source with 1000 rows
INSERT INTO ratings_source (id, user_id, movie_id, rating, created_at)
SELECT
    g                                    AS id,
    1 + (g % 200)                        AS user_id,
    1 + (g % 50)                         AS movie_id,
    (1 + (g % 5))::NUMERIC(2,1)         AS rating,
    '2025-01-01'::TIMESTAMP + (g || ' minutes')::INTERVAL AS created_at
FROM generate_series(1, 1000) AS g;

-- Copy all rows into target
INSERT INTO ratings_target SELECT * FROM ratings_source;

-- Introduce diffs:
-- 5 deleted rows (IDs 10-14 missing from target)
DELETE FROM ratings_target WHERE id BETWEEN 10 AND 14;

-- 5 extra rows in target only (IDs 1001-1005)
INSERT INTO ratings_target (id, user_id, movie_id, rating, created_at) VALUES
    (1001, 201, 51, 4.0, '2025-06-01 00:00:00'),
    (1002, 202, 52, 3.0, '2025-06-02 00:00:00'),
    (1003, 203, 53, 5.0, '2025-06-03 00:00:00'),
    (1004, 204, 54, 2.0, '2025-06-04 00:00:00'),
    (1005, 205, 55, 1.0, '2025-06-05 00:00:00');

-- 10 updated ratings (IDs 100-109 have different ratings in target)
UPDATE ratings_target SET rating = rating + 0.5 WHERE id BETWEEN 100 AND 109 AND rating < 5.0;
UPDATE ratings_target SET rating = 1.0             WHERE id BETWEEN 100 AND 109 AND rating >= 5.0;
