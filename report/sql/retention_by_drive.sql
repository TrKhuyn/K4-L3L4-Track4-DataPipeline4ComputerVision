SELECT drive_id,
       SUM(frames) AS frames_total,
       SUM(frames_kept) AS frames_kept,
       SUM(frames) - SUM(frames_kept) AS frames_dropped,
       ROUND(100.0 * SUM(frames_kept) / NULLIF(SUM(frames), 0), 2) AS kept_pct
FROM marts.mart_trip_summary
GROUP BY drive_id
ORDER BY drive_id;
