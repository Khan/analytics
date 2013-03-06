-- Required script arguments:
--   dt: the day for which to compute stats in the format YYYY-MM-DD

-- Performance statistics for an individual URL taken from the request logs.
--
-- We limit to only 10,000 URLS because when we ran it on 20 July 2012 there
-- were 1.6 million unique URLs, 90% of which were hit only once. The 10,000th
-- URL had about 550 hits, which seemed reasonable. This cutoff was arbitrarily
-- picked.
INSERT OVERWRITE TABLE daily_request_log_url_stats
  PARTITION (dt = '${dt}')
SELECT
  stats.count,
  stats.url,
  ROUND(stats.avg_response_bytes),
  ROUND(stats.ms_pct[0]) as ms_pct5,
  ROUND(stats.ms_pct[1]) as ms_pct50,
  ROUND(stats.ms_pct[2]) as ms_pct95,
  ROUND(stats.cpu_ms_pct[0]) as cpu_ms_pct5,
  ROUND(stats.cpu_ms_pct[1]) as cpu_ms_pct50,
  ROUND(stats.cpu_ms_pct[2]) as cpu_ms_pct95,
  ROUND(stats.cpm_microcents_pct[0]) as cpm_microcents_pct5,
  ROUND(stats.cpm_microcents_pct[1]) as cpm_microcents_pct50,
  ROUND(stats.cpm_microcents_pct[2]) as cpm_microcents_pct95,
  -- The columns for the 25th, 75th percentiles were added late and are
  -- listed last to match the table's column ordering.
  ROUND(stats.ms_pct[3]) as ms_pct25,
  ROUND(stats.ms_pct[4]) as ms_pct75,
  ROUND(stats.cpu_ms_pct[3]) as cpu_ms_pct25,
  ROUND(stats.cpu_ms_pct[4]) as cpu_ms_pct75,
  ROUND(stats.cpm_microcents_pct[3]) as cpm_microcents_pct25,
  ROUND(stats.cpm_microcents_pct[4]) as cpm_microcents_pct75
FROM (
  SELECT
    COUNT(*) AS count,
    url,
    AVG(bytes) AS avg_response_bytes,
    PERCENTILE(ms, array(0.05, 0.50, 0.95, 0.25, 0.75)) AS ms_pct,
    PERCENTILE(cpu_ms, array(0.05, 0.50, 0.95, 0.25, 0.75)) AS cpu_ms_pct,
    PERCENTILE(cpm_usd * 100000000, array(0.05, 0.50, 0.95, 0.25, 0.75))
      AS cpm_microcents_pct
  FROM website_request_logs
  WHERE dt = '${dt}'
  GROUP BY url
  ORDER BY count DESC
  LIMIT 10000
) stats;


-- Performance statistics for an 'grouped' URLs taken from the request
-- logs.  We map each url to the wsgi handler that handles it
-- (identified by the regexp that maps to that handler in the wsgi
-- application), and collect information on a per-handler basis.
--
-- This is exactly the same as url_stats, except 'url' has been
-- replaced by 'url_route'.  Also, we got rid of the limit of 10,000
-- (not really necessary since we're doing all this grouping; there
-- are only a few hundred handlers).
INSERT OVERWRITE TABLE daily_request_log_urlroute_stats
  PARTITION (dt = '${dt}')
SELECT
  stats.count,
  stats.url_route,
  ROUND(stats.avg_response_bytes),
  ROUND(stats.ms_pct[0]) as ms_pct5,
  ROUND(stats.ms_pct[1]) as ms_pct50,
  ROUND(stats.ms_pct[2]) as ms_pct95,
  ROUND(stats.cpu_ms_pct[0]) as cpu_ms_pct5,
  ROUND(stats.cpu_ms_pct[1]) as cpu_ms_pct50,
  ROUND(stats.cpu_ms_pct[2]) as cpu_ms_pct95,
  ROUND(stats.cpm_microcents_pct[0]) as cpm_microcents_pct5,
  ROUND(stats.cpm_microcents_pct[1]) as cpm_microcents_pct50,
  ROUND(stats.cpm_microcents_pct[2]) as cpm_microcents_pct95,
  -- The columns for the 25th, 75th percentiles were added late and are
  -- listed last to match the table's column ordering.
  ROUND(stats.ms_pct[3]) as ms_pct25,
  ROUND(stats.ms_pct[4]) as ms_pct75,
  ROUND(stats.cpu_ms_pct[3]) as cpu_ms_pct25,
  ROUND(stats.cpu_ms_pct[4]) as cpu_ms_pct75,
  ROUND(stats.cpm_microcents_pct[3]) as cpm_microcents_pct25,
  ROUND(stats.cpm_microcents_pct[4]) as cpm_microcents_pct75
FROM (
  SELECT
    COUNT(*) AS count,
    url_route,
    AVG(bytes) AS avg_response_bytes,
    PERCENTILE(ms, array(0.05, 0.50, 0.95, 0.25, 0.75)) AS ms_pct,
    PERCENTILE(cpu_ms, array(0.05, 0.50, 0.95, 0.25, 0.75)) AS cpu_ms_pct,
    PERCENTILE(cpm_usd * 100000000, array(0.05, 0.50, 0.95, 0.25, 0.75))
      AS cpm_microcents_pct
  FROM website_request_logs
  WHERE dt = '${dt}'
  GROUP BY url_route
  ORDER BY count DESC
) stats;
