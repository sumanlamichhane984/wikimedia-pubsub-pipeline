-- ================================================
-- Wikimedia Pub/Sub Pipeline - BigQuery Analytics
-- Dataset: wikimedia_pubsub
-- ================================================

-- 1. Total row counts across all tables
SELECT 'wiki_edits_stream' AS table_name, COUNT(*) AS row_count
FROM `jenish-my-first-dog.wikimedia_pubsub.wiki_edits_stream`
UNION ALL
SELECT 'wiki_bot_stream', COUNT(*)
FROM `jenish-my-first-dog.wikimedia_pubsub.wiki_bot_stream`
UNION ALL
SELECT 'wiki_dlq_stream', COUNT(*)
FROM `jenish-my-first-dog.wikimedia_pubsub.wiki_dlq_stream`;


-- 2. Top 20 most edited articles (human edits only)
SELECT
  title,
  COUNT(*)                        AS total_edits,
  COUNT(DISTINCT user)            AS unique_editors,
  SUM(byte_delta)                 AS total_bytes_changed,
  AVG(byte_delta)                 AS avg_bytes_per_edit
FROM `jenish-my-first-dog.wikimedia_pubsub.wiki_edits_stream`
WHERE title IS NOT NULL
GROUP BY title
ORDER BY total_edits DESC
LIMIT 20;


-- 3. Bot vs Human edit ratio
SELECT
  'human' AS edit_type,
  COUNT(*) AS total_edits,
  ROUND(COUNT(*) * 100.0 / (
    SELECT COUNT(*) FROM `jenish-my-first-dog.wikimedia_pubsub.wiki_edits_stream`
  ) + (
    SELECT COUNT(*) FROM `jenish-my-first-dog.wikimedia_pubsub.wiki_bot_stream`
  ), 2) AS percentage
FROM `jenish-my-first-dog.wikimedia_pubsub.wiki_edits_stream`
UNION ALL
SELECT
  'bot',
  COUNT(*),
  NULL
FROM `jenish-my-first-dog.wikimedia_pubsub.wiki_bot_stream`;


-- 4. Most active human editors
SELECT
  user,
  COUNT(*)           AS total_edits,
  SUM(byte_delta)    AS total_bytes_changed,
  MIN(event_timestamp) AS first_seen,
  MAX(event_timestamp) AS last_seen
FROM `jenish-my-first-dog.wikimedia_pubsub.wiki_edits_stream`
WHERE user IS NOT NULL
  AND user_is_anon = FALSE
GROUP BY user
ORDER BY total_edits DESC
LIMIT 20;


-- 5. Most active bot editors
SELECT
  user,
  COUNT(*)           AS total_edits,
  SUM(byte_delta)    AS total_bytes_changed
FROM `jenish-my-first-dog.wikimedia_pubsub.wiki_bot_stream`
WHERE user IS NOT NULL
GROUP BY user
ORDER BY total_edits DESC
LIMIT 20;


-- 6. Anonymous vs registered editor breakdown
SELECT
  user_is_anon,
  COUNT(*)                     AS total_edits,
  COUNT(DISTINCT user)         AS unique_users,
  ROUND(AVG(byte_delta), 2)    AS avg_bytes_changed
FROM `jenish-my-first-dog.wikimedia_pubsub.wiki_edits_stream`
GROUP BY user_is_anon;


-- 7. Edit volume by hour of day (UTC)
SELECT
  EXTRACT(HOUR FROM TIMESTAMP(event_timestamp)) AS hour_utc,
  COUNT(*) AS total_edits
FROM `jenish-my-first-dog.wikimedia_pubsub.wiki_edits_stream`
WHERE event_timestamp IS NOT NULL
GROUP BY hour_utc
ORDER BY hour_utc;


-- 8. Minor vs major edits breakdown
SELECT
  is_minor,
  COUNT(*)                  AS total_edits,
  ROUND(AVG(byte_delta), 2) AS avg_bytes_changed,
  SUM(byte_delta)           AS total_bytes_changed
FROM `jenish-my-first-dog.wikimedia_pubsub.wiki_edits_stream`
GROUP BY is_minor;


-- 9. Largest edits by byte delta (potential vandalism or major additions)
SELECT
  title,
  user,
  byte_delta,
  is_minor,
  event_timestamp,
  comment
FROM `jenish-my-first-dog.wikimedia_pubsub.wiki_edits_stream`
ORDER BY ABS(byte_delta) DESC
LIMIT 20;


-- 10. Namespace breakdown (0=article, 1=talk, 2=user, 4=project etc.)
SELECT
  namespace,
  CASE namespace
    WHEN 0  THEN 'Article'
    WHEN 1  THEN 'Talk'
    WHEN 2  THEN 'User'
    WHEN 3  THEN 'User Talk'
    WHEN 4  THEN 'Wikipedia'
    WHEN 10 THEN 'Template'
    WHEN 14 THEN 'Category'
    ELSE 'Other'
  END AS namespace_name,
  COUNT(*) AS total_edits
FROM `jenish-my-first-dog.wikimedia_pubsub.wiki_edits_stream`
GROUP BY namespace
ORDER BY total_edits DESC;
