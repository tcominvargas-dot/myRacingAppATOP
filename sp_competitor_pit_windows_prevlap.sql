
DROP PROCEDURE IF EXISTS sp_competitor_pit_windows_prevlap;
DELIMITER $$

CREATE PROCEDURE sp_competitor_pit_windows_prevlap(
  IN p_start_lap INT,
  IN p_end_lap   INT
)
BEGIN
  /* Validação básica dos parâmetros */
  IF p_start_lap IS NULL OR p_end_lap IS NULL OR p_start_lap > p_end_lap THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Parâmetros inválidos: start_lap/end_lap';
  END IF;

  /* Camada interna: calcula prev_pit_lap_number via LAG */
  WITH base AS (
    SELECT
      /* bucket dinâmico em passos de 5, sem limite superior */
      5 * CEIL( (p_end_lap - s.lap_number + 1) / 5 ) AS window_box_interval,

      /* demais colunas */
      s.race_id,
      s.racer_id,
      s.lap_number,
      s.lap_time,
      s.lap_seconds,

      /* último lap_number desse mesmo piloto (e corrida) no resultado */
      LAG(s.lap_number) OVER (
        PARTITION BY s.race_id, s.racer_id
        ORDER BY s.lap_number
      ) AS prev_pit_lap_number

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
  )

  /* Camada externa: adiciona a coluna com (lap_number - prev_pit_lap_number) */
  SELECT
    base.window_box_interval,
    base.race_id,
    base.racer_id,
    base.lap_number,
    base.lap_time,
    base.lap_seconds,
    base.prev_pit_lap_number,

    /* nova coluna: diferença entre a volta atual e a volta anterior (no resultado) */
    CASE
      WHEN base.prev_pit_lap_number IS NULL THEN NULL
      ELSE base.lap_number - base.prev_pit_lap_number
    END AS laps_between_pit

  FROM base
  ORDER BY base.window_box_interval ASC,
           base.race_id ASC,
           base.racer_id ASC,
           base.lap_number ASC;

END $$
