
-- Performance statistics for an individual URL taken from the request logs.
-- TODO(chris): should we use the array syntax of PERCENTILE instead?
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
  ROUND(stats.api_cpu_ms_pct[0]) as api_cpu_ms_pct5,
  ROUND(stats.api_cpu_ms_pct[1]) as api_cpu_ms_pct50,
  ROUND(stats.api_cpu_ms_pct[2]) as api_cpu_ms_pct95,
  ROUND(stats.cpm_microcents_pct[0]) as cpm_microcents_pct5,
  ROUND(stats.cpm_microcents_pct[1]) as cpm_microcents_pct50,
  ROUND(stats.cpm_microcents_pct[2]) as cpm_microcents_pct95
FROM (
  SELECT
    COUNT(*) AS count,
    url,
    AVG(bytes) AS avg_response_bytes,
    PERCENTILE(ms, array(0.05, 0.50, 0.95)) AS ms_pct,
    PERCENTILE(cpu_ms, array(0.05, 0.50, 0.95)) AS cpu_ms_pct,
    PERCENTILE(api_cpu_ms, array(0.05, 0.50, 0.95)) AS api_cpu_ms_pct,
    PERCENTILE(cpm_usd * 100000000, array(0.05, 0.50, 0.95)) AS cpm_microcents_pct
  FROM website_request_logs
  WHERE dt = '${dt}'
  GROUP BY url
) stats;
