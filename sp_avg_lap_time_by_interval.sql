
DROP PROCEDURE IF EXISTS sp_avg_lap_time_by_interval;
DELIMITER $$

CREATE PROCEDURE sp_avg_lap_time_by_interval(
  IN p_start_lap INT,
  IN p_end_lap   INT
)
BEGIN
  /* Validação de parâmetros */
  IF p_start_lap IS NULL OR p_end_lap IS NULL OR p_start_lap > p_end_lap THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Parâmetros inválidos: start_lap/end_lap';
  END IF;

  /*
    Regra:
    - Voltas de competitor_laps com lap_number ∈ [p_start_lap, p_end_lap]
    - Para cada volta, usa o registro de app_config_lap_interval que cobre o lap_number
    - Considera somente voltas com lap_seconds ∈ [min_time_seconds, max_time_seconds]
    - Agrega média por (race_id, racer_id)
  */
  SELECT
    l.race_id,
    l.racer_id,
    AVG(l.lap_seconds)                         AS avg_lap_seconds,        -- média em segundos (com ms)
    COUNT(*)                                   AS laps_count,             -- quantidade de voltas consideradas
    SEC_TO_TIME(ROUND(AVG(l.lap_seconds)))     AS avg_lap_time_hhmmss,    -- média HH:MM:SS (sem ms)
    MIN(l.lap_seconds)                         AS min_lap_seconds,        -- menor tempo dentro do intervalo
    MAX(l.lap_seconds)                         AS max_lap_seconds         -- maior tempo dentro do intervalo
  FROM (
    /* Parser robusto de lap_time → lap_seconds */
    SELECT
      cl.race_id,
      cl.racer_id,
      cl.lap_number,
      CASE
        /* HH:MM:SS(.mmm) */
        WHEN cl.lap_time REGEXP '^[0-9]+:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?$' THEN
          CAST(SUBSTRING_INDEX(cl.lap_time, ':', 1) AS UNSIGNED) * 3600
          + CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(cl.lap_time, ':', -2), ':', 1) AS UNSIGNED) * 60
          + CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(cl.lap_time, ':', -1), '.', 1) AS UNSIGNED)
          + IF(cl.lap_time LIKE '%.%',
               CAST(CONCAT('0.', SUBSTRING_INDEX(cl.lap_time, '.', -1)) AS DECIMAL(10,3)),
               0)

        /* MM:SS(.mmm) */
               WHEN cl.lap_time REGEXP '^[0-9]+:[0-9]{2}(\\.[0-9]+)?$' THEN
          CAST(SUBSTRING_INDEX(cl.lap_time, ':', 1) AS UNSIGNED) * 60
          + CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(cl.lap_time, ':', -1), '.', 1) AS UNSIGNED)
          + IF(cl.lap_time LIKE '%.%',
               CAST(CONCAT('0.', SUBSTRING_INDEX(cl.lap_time, '.', -1)) AS DECIMAL(10,3)),
               0)

        ELSE NULL
      END AS lap_seconds
    FROM competitor_laps cl
    WHERE cl.lap_number BETWEEN p_start_lap AND p_end_lap
  ) AS l
  JOIN app_config_lap_interval cfg
    ON l.lap_number BETWEEN cfg.start_lap AND cfg.end_lap
  WHERE l.lap_seconds IS NOT NULL
    AND l.lap_seconds BETWEEN cfg.min_time_seconds AND cfg.max_time_seconds
  GROUP BY l.race_id, l.racer_id
  ORDER BY l.race_id, l.racer_id;
END $$
