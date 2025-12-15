
DELIMITER ;;
DROP PROCEDURE IF EXISTS `sp_kart_box_ranking`;;
CREATE DEFINER=`admin`@`%` PROCEDURE `sp_kart_box_ranking`(
  IN p_race_id INT,
  IN p_pit_min_1 INT,
  IN p_pit_max_1 INT,
  IN p_pit_min_2 INT,
  IN p_pit_max_2 INT,
  IN p_min_self_laps INT,
  IN p_min_field_laps INT,
  IN p_override_now_hhmm VARCHAR(8),
  IN p_override_offset_before_end_min INT
)
SQL SECURITY INVOKER
BEGIN
  DECLARE v_override_sec INT DEFAULT NULL;
  DECLARE v_max_sec INT DEFAULT 0;
  DECLARE v_now_sec INT DEFAULT 0;
  DECLARE v_min_self_laps INT DEFAULT 1;
  DECLARE v_min_field_laps INT DEFAULT 1;

  IF p_min_self_laps IS NOT NULL AND p_min_self_laps >= 1 THEN
    SET v_min_self_laps = p_min_self_laps;
  END IF;
  IF p_min_field_laps IS NOT NULL AND p_min_field_laps >= 1 THEN
    SET v_min_field_laps = p_min_field_laps;
  END IF;

  IF p_pit_min_1 IS NULL OR p_pit_max_1 IS NULL OR p_pit_min_1 > p_pit_max_1 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Faixa de PIT 1 inválida: p_pit_min_1 e p_pit_max_1 devem existir e min <= max.';
  END IF;
  IF (p_pit_min_2 IS NOT NULL OR p_pit_max_2 IS NOT NULL)
     AND (p_pit_min_2 IS NULL OR p_pit_max_2 IS NULL OR p_pit_min_2 > p_pit_max_2) THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Faixa de PIT 2 inválida: ambos limites devem existir e min <= max, ou ambos NULL.';
  END IF;

  -- Harden: aceita apenas HH:MM ou HH:MM:SS
  IF p_override_now_hhmm IS NOT NULL AND p_override_now_hhmm <> '' THEN
    IF p_override_now_hhmm REGEXP '^[0-9]{1,2}:[0-9]{2}(:[0-9]{2})?$' THEN
      SELECT TIME_TO_SEC(
        COALESCE(
          STR_TO_DATE(p_override_now_hhmm, '%H:%i:%s'),
          STR_TO_DATE(p_override_now_hhmm, '%H:%i')
        )
      ) INTO v_override_sec;
    ELSE
      SET v_override_sec = NULL;
    END IF;
  END IF;

  DROP TEMPORARY TABLE IF EXISTS tmp_base;
  CREATE TEMPORARY TABLE tmp_base AS
  SELECT race_id, racer_id, lap_number,
         TIME_TO_SEC(lap_time) AS lt_sec,
         TIME_TO_SEC(total_time) AS tt_sec
  FROM my_karting_app.competitor_laps
  WHERE (p_race_id IS NULL OR race_id = p_race_id);

  SELECT COALESCE(MAX(tt_sec), 0) INTO v_max_sec FROM tmp_base;
  IF v_override_sec IS NOT NULL THEN
    SET v_now_sec = v_override_sec;
  ELSEIF p_override_offset_before_end_min IS NOT NULL THEN
    SET v_now_sec = GREATEST(v_max_sec - (p_override_offset_before_end_min * 60), 0);
  ELSE
    SET v_now_sec = v_max_sec;
  END IF;

  DROP TEMPORARY TABLE IF EXISTS tmp_pit_laps;
  CREATE TEMPORARY TABLE tmp_pit_laps AS
  SELECT b.race_id, b.racer_id, b.lap_number, b.lt_sec, b.tt_sec
  FROM tmp_base b
  WHERE (b.lt_sec BETWEEN p_pit_min_1 AND p_pit_max_1)
     OR (p_pit_min_2 IS NOT NULL AND b.lt_sec BETWEEN p_pit_min_2 AND p_pit_max_2);

  DROP TEMPORARY TABLE IF EXISTS tmp_pit_seq;
  CREATE TEMPORARY TABLE tmp_pit_seq AS
  SELECT pl.*,
         LAG(pl.tt_sec) OVER (PARTITION BY pl.race_id, pl.racer_id ORDER BY pl.tt_sec) AS prev_pit_tt_sec
  FROM tmp_pit_laps pl;

  DROP TEMPORARY TABLE IF EXISTS tmp_janelas;
  CREATE TEMPORARY TABLE tmp_janelas (win_min INT NOT NULL, win_sec INT NOT NULL);
  INSERT INTO tmp_janelas VALUES (5,300),(10,600),(15,900),(20,1200),(25,1500);

  DROP TEMPORARY TABLE IF EXISTS tmp_recent_pits;
  CREATE TEMPORARY TABLE tmp_recent_pits AS
  SELECT j.win_min, ps.race_id, ps.racer_id, ps.tt_sec AS pit_tt_sec,
         COALESCE(ps.prev_pit_tt_sec, 0) AS prev_pit_tt_sec
  FROM tmp_pit_seq ps
  JOIN tmp_janelas j
    ON (v_now_sec - ps.tt_sec) >= 0 AND (v_now_sec - ps.tt_sec) <= j.win_sec;

  DROP TEMPORARY TABLE IF EXISTS tmp_stint_avg;
  CREATE TEMPORARY TABLE tmp_stint_avg AS
  SELECT rp.win_min, rp.race_id, rp.racer_id, rp.prev_pit_tt_sec, rp.pit_tt_sec,
         AVG(b.lt_sec) AS self_avg_sec,
         COUNT(*) AS self_laps
  FROM tmp_recent_pits rp
  JOIN tmp_base b
    ON b.race_id = rp.race_id
   AND b.racer_id = rp.racer_id
   AND b.tt_sec > rp.prev_pit_tt_sec AND b.tt_sec < rp.pit_tt_sec
   AND NOT ((b.lt_sec BETWEEN p_pit_min_1 AND p_pit_max_1) OR (p_pit_min_2 IS NOT NULL AND b.lt_sec BETWEEN p_pit_min_2 AND p_pit_max_2))
  GROUP BY rp.win_min, rp.race_id, rp.racer_id, rp.prev_pit_tt_sec, rp.pit_tt_sec
  HAVING COUNT(*) >= v_min_self_laps;

  DROP TEMPORARY TABLE IF EXISTS tmp_field_avg;
  CREATE TEMPORARY TABLE tmp_field_avg AS
  SELECT rp.win_min, rp.race_id, rp.racer_id,
         AVG(b.lt_sec) AS field_avg_sec,
         COUNT(*) AS field_laps
  FROM tmp_recent_pits rp
  JOIN tmp_base b
    ON b.race_id = rp.race_id
   AND b.racer_id <> rp.racer_id
   AND b.tt_sec BETWEEN rp.prev_pit_tt_sec AND rp.pit_tt_sec
   AND NOT ((b.lt_sec BETWEEN p_pit_min_1 AND p_pit_max_1) OR (p_pit_min_2 IS NOT NULL AND b.lt_sec BETWEEN p_pit_min_2 AND p_pit_max_2))
  GROUP BY rp.win_min, rp.race_id, rp.racer_id
  HAVING COUNT(*) >= v_min_field_laps;

  DROP TEMPORARY TABLE IF EXISTS tmp_comparacao;
  CREATE TEMPORARY TABLE tmp_comparacao AS
  SELECT s.win_min, s.race_id, s.racer_id, s.self_avg_sec, f.field_avg_sec,
         (f.field_avg_sec - s.self_avg_sec) AS delta_sec
  FROM tmp_stint_avg s
  JOIN tmp_field_avg f
    ON f.win_min = s.win_min AND f.race_id = s.race_id AND f.racer_id = s.racer_id;

  SELECT c.win_min, c.racer_id,
         ROUND(c.self_avg_sec,3) AS self_avg_sec,
         ROUND(c.field_avg_sec,3) AS field_avg_sec,
         ROUND(c.delta_sec,3) AS delta_sec
  FROM tmp_comparacao c
  ORDER BY c.win_min, c.delta_sec DESC;

  DROP TEMPORARY TABLE IF EXISTS tmp_comparacao;
  DROP TEMPORARY TABLE IF EXISTS tmp_field_avg;
  DROP TEMPORARY TABLE IF EXISTS tmp_stint_avg;
  DROP TEMPORARY TABLE IF EXISTS tmp_recent_pits;
  DROP TEMPORARY TABLE IF EXISTS tmp_janelas;
  DROP TEMPORARY TABLE IF EXISTS tmp_pit_seq;
  DROP TEMPORARY TABLE IF EXISTS tmp_pit_laps;
  DROP TEMPORARY TABLE IF EXISTS tmp_base;
END;;
DELIMITER ;
