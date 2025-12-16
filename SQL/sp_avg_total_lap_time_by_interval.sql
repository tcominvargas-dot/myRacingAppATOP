
DROP PROCEDURE IF EXISTS sp_avg_total_lap_time_by_interval;
DELIMITER $$

CREATE PROCEDURE sp_avg_total_lap_time_by_interval(
  IN p_start_lap INT,
  IN p_end_lap   INT
)
BEGIN
  /* Validação básica */
  IF p_start_lap IS NULL OR p_end_lap IS NULL OR p_start_lap > p_end_lap THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Parâmetros inválidos: start_lap/end_lap';
  END IF;

  /*
    Lógica:
    - Considera voltas com lap_number ∈ [p_start_lap, p_end_lap]
    - Usa a config app_config_lap_interval que cobre a volta (lap_number BETWEEN start_lap AND end_lap)
    - Filtra por tempo: lap_seconds ∈ [min_time_seconds, max_time_seconds]
    - Calcula média total sobre TODAS as voltas qualificadas (independente de kart)
    - Formata média como HH:MM:SS.mmm preservando milissegundos
  */
  SELECT
    AVG(l.lap_seconds)                                    AS avg_total_seconds,
    /* HH:MM:SS (sem ms) para referência rápida */
    SEC_TO_TIME(FLOOR(AVG(l.lap_seconds)))                AS avg_total_hhmmss,
    /* HH:MM:SS.mmm preservando ms */
    CONCAT(
      LPAD(FLOOR(AVG(l.lap_seconds) / 3600), 2, '0'), ':',
      LPAD(FLOOR(MOD(AVG(l.lap_seconds), 3600) / 60), 2, '0'), ':',
      LPAD(FLOOR(MOD(AVG(l.lap_seconds), 60)), 2, '0'), '.',
      LPAD(ROUND(MOD(AVG(l.lap_seconds), 1) * 1000), 3, '0')
    )                                                      AS avg_total_hhmmss_mmm,
    COUNT(*)                                               AS laps_count,
    COUNT(DISTINCT CONCAT(l.race_id, ':', l.racer_id))     AS racers_distinct
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
       AND l.lap_seconds BETWEEN cfg.min_time_seconds AND cfg.max_time_seconds;
END $$
