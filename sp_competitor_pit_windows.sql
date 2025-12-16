
DROP PROCEDURE IF EXISTS sp_competitor_pit_windows;
DELIMITER $$

CREATE PROCEDURE sp_competitor_pit_windows(
  IN p_start_lap INT,
  IN p_end_lap   INT
)
BEGIN
  /* Validação básica dos parâmetros */
  IF p_start_lap IS NULL OR p_end_lap IS NULL OR p_start_lap > p_end_lap THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Parâmetros inválidos: start_lap/end_lap';
  END IF;

  /* Consulta principal: uma linha por volta marcada como pit */
  SELECT
    /* bucket dinâmico em passos de 5, sem limite superior */
    5 * CEIL( (p_end_lap - s.lap_number + 1) / 5 ) AS window_box_interval,

    /* demais colunas */
    s.race_id,
    s.racer_id,
    s.lap_number,
    s.lap_time,
    s.lap_seconds

  FROM (
    /* Parse de lap_time -> segundos (suporta HH:MM:SS(.mmm) e MM:SS(.mmm)) */
    SELECT
      cl.race_id,
      cl.racer_id,
      cl.lap_number,
      cl.lap_time,
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
  ) AS s

  /* Mantém somente voltas que se enquadram nos intervalos de pit configurados */
  WHERE s.lap_seconds IS NOT NULL
    AND EXISTS (
      SELECT 1
      FROM app_config_box_interval b
      WHERE s.lap_seconds BETWEEN b.low_time_seconds AND b.high_time_seconds
    )

  /* Ordenação: menor window_box_interval -> race/racer/lap */
  ORDER BY window_box_interval ASC, s.race_id ASC, s.racer_id ASC, s.lap_number ASC;

END $$
DELIMITER ;