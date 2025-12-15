
-- ==============================
-- CONFIGURAÇÕES
-- ==============================
SET @race_id = NULL;           -- opcional: filtrar corrida específica
SET @pit_min_1 = 230;          -- faixa 1: limite inferior em segundos (3:50 = 230s)
SET @pit_max_1 = 250;          -- faixa 1: limite superior em segundos (4:10 = 250s)
SET @pit_min_2 = NULL;         -- faixa 2: se não houver, deixe NULL
SET @pit_max_2 = NULL;         -- faixa 2: idem
SET @override_now_sec = NULL;  -- simulação de "agora" (NULL = usa MAX(total_time))

-- ==============================
-- QUERY PRINCIPAL
-- ==============================
WITH
-- 1) Base em segundos
base AS (
  SELECT
    race_id,
    racer_id,
    lap_number,
    TIME_TO_SEC(lap_time)   AS lt_sec,
    TIME_TO_SEC(total_time) AS tt_sec
  FROM competitor_laps
  WHERE (@race_id IS NULL OR race_id = @race_id)
),

-- 2) Agora = maior total_time ou override
agora AS (
  SELECT COALESCE(@override_now_sec, MAX(tt_sec)) AS now_sec FROM base
),

-- 3) Voltas que indicam PIT (usando parâmetros)
pit_laps AS (
  SELECT b.race_id, b.racer_id, b.lap_number, b.lt_sec, b.tt_sec
  FROM base b
  WHERE (b.lt_sec BETWEEN @pit_min_1 AND @pit_max_1)
     OR (@pit_min_2 IS NOT NULL AND b.lt_sec BETWEEN @pit_min_2 AND @pit_max_2)
),

-- 4) Sequência de pits por piloto (para definir stint anterior)
pit_seq AS (
  SELECT
    pl.*,
    LAG(pl.tt_sec) OVER (PARTITION BY pl.race_id, pl.racer_id ORDER BY pl.tt_sec) AS prev_pit_tt_sec
  FROM pit_laps pl
),

-- 5) Janelas exclusivas (ajuste conforme necessidade)
janelas AS (
  SELECT  5 AS win_min,  5*60  AS win_sec UNION ALL
  SELECT 10 AS win_min, 10*60  AS win_sec UNION ALL
  SELECT 15 AS win_min, 15*60  AS win_sec UNION ALL
  SELECT 20 AS win_min, 20*60  AS win_sec UNION ALL
  SELECT 25 AS win_min, 25*60  AS win_sec
),
janelas_bounded AS (
  SELECT
    j.win_min,
    j.win_sec,
    COALESCE(LAG(j.win_sec) OVER (ORDER BY j.win_sec), 0) AS low_sec
  FROM janelas j
),

-- 6) Pits recentes classificados em UMA única janela
recent_pits AS (
  SELECT
    jb.win_min,
    ps.race_id,
    ps.racer_id,
    ps.tt_sec                       AS pit_tt_sec,
    COALESCE(ps.prev_pit_tt_sec, 0) AS prev_pit_tt_sec
  FROM pit_seq ps
  CROSS JOIN agora a
  JOIN janelas_bounded jb
    ON (a.now_sec - ps.tt_sec) >  jb.low_sec
   AND (a.now_sec - ps.tt_sec) <= jb.win_sec
),

-- 7) Média do próprio piloto no stint anterior
stint_avg AS (
  SELECT
    rp.win_min,
    rp.race_id,
    rp.racer_id,
    rp.prev_pit_tt_sec,
    rp.pit_tt_sec,
    AVG(b.lt_sec) AS self_avg_sec,
    COUNT(*)      AS self_laps
  FROM recent_pits rp
  JOIN base b
    ON b.race_id  = rp.race_id
   AND b.racer_id = rp.racer_id
   AND b.tt_sec   > rp.prev_pit_tt_sec
   AND b.tt_sec   < rp.pit_tt_sec
   AND NOT (b.lt_sec BETWEEN @pit_min_1 AND @pit_max_1)
   AND (@pit_min_2 IS NULL OR NOT (b.lt_sec BETWEEN @pit_min_2 AND @pit_max_2))
  GROUP BY rp.win_min, rp.race_id, rp.racer_id, rp.prev_pit_tt_sec, rp.pit_tt_sec
  HAVING COUNT(*) >= 1  -- relaxado para não eliminar tudo
),

-- 8) Média do restante do pelotão no mesmo intervalo
field_avg AS (
  SELECT
    rp.win_min,
    rp.race_id,
    rp.racer_id,
    AVG(b.lt_sec) AS field_avg_sec,
    COUNT(*)      AS field_laps
  FROM recent_pits rp
  JOIN base b
    ON b.race_id  = rp.race_id
   AND b.racer_id <> rp.racer_id
   AND b.tt_sec BETWEEN rp.prev_pit_tt_sec AND rp.pit_tt_sec
   AND NOT (b.lt_sec BETWEEN @pit_min_1 AND @pit_max_1)
   AND (@pit_min_2 IS NULL OR NOT (b.lt_sec BETWEEN @pit_min_2 AND @pit_max_2))
  GROUP BY rp.win_min, rp.race_id, rp.racer_id
  HAVING COUNT(*) >= 1
),

-- 9) Comparação kart vs pelotão
comparacao AS (
  SELECT
    s.win_min,
    s.race_id,
    s.racer_id,
    s.self_avg_sec,
    f.field_avg_sec,
    (f.field_avg_sec - s.self_avg_sec) AS delta_sec,
    s.self_laps,
    f.field_laps
  FROM stint_avg s
  JOIN field_avg f
    ON f.win_min = s.win_min
   AND f.race_id = s.race_id
   AND f.racer_id = s.racer_id
),

-- 10) Agregado por janela
por_janela AS (
  SELECT
    win_min,
    COUNT(*) AS pits_count,
    AVG(CASE WHEN delta_sec > 0 THEN 1.0 ELSE 0.0 END) AS prob_fast,
    AVG(delta_sec) AS avg_delta_sec
  FROM comparacao
  GROUP BY win_min
)

-- ==============================
-- SAÍDA FINAL
-- ==============================
SELECT
  jb.win_min                               AS janela_minutos,
  COALESCE(pj.pits_count, 0)               AS pits_count,
  COALESCE(ROUND(pj.prob_fast, 3), 0)      AS prob_fast,
  COALESCE(ROUND(pj.avg_delta_sec, 3), 0)  AS avg_delta_sec,
  CASE
    WHEN COALESCE(pj.prob_fast, 0) >= 0.80 THEN 5
    WHEN COALESCE(pj.prob_fast, 0) >= 0.60 THEN 4
    WHEN COALESCE(pj.prob_fast, 0) >= 0.40 THEN 3
    WHEN COALESCE(pj.prob_fast, 0) >= 0.20 THEN 2
    ELSE 1
  END                                      AS nivel
FROM janelas_bounded jb
LEFT JOIN por_janela pj
  ON pj.win_min = jb.win_min
ORDER BY janela_minutos;

-- ==============================
-- RANKING OPCIONAL DOS KARTS MAIS PROMISSORES
-- ==============================
SELECT
  c.win_min,
  c.racer_id,
  ROUND(c.self_avg_sec, 3)  AS self_avg_sec,
  ROUND(c.field_avg_sec, 3) AS field_avg_sec,
  ROUND(c.delta_sec, 3)     AS delta_sec
FROM comparacao c
