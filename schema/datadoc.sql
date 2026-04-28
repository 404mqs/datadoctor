-- Data Doctor — Delta schema DDL
-- Run this in a Databricks notebook cell (spark.sql or %sql) before the first run.
-- Replace 'datadoc' with the value of agent.delta_schema in your datadoctor_config.yml
-- if you changed it from the default.

-- ============================================================
-- Data Doctor — Schema de tracking (Delta)
-- Correr UNA sola vez en el workspace para inicializar.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS datadoc;

-- ------------------------------------------------------------
-- Tabla: proposals
-- Una fila por cada propuesta generada por el agente.
-- status: proposed → (approved | rejected) → applied
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS datadoc.proposals (
  proposal_id          STRING,                -- uuid
  created_ts           TIMESTAMP,
  run_id               BIGINT,                -- run del Orquestador analizado
  task_key             STRING,                -- nombre de la task en el job
  original_path        STRING,                -- path del notebook original
  v2_path              STRING,                -- path del notebook optimizado (sandbox)
  duration_original_s  DOUBLE,                -- duración en el run analizado

  tier                 STRING,                -- 'green' | 'yellow' | 'red'
  tier_reason          STRING,                -- por qué se clasificó así
  target_tables        ARRAY<STRING>,         -- tablas output detectadas
  side_effects         ARRAY<STRING>,         -- side effects detectados

  llm_model            STRING,                -- 'databricks-claude-opus-4-7'
  llm_analysis         STRING,                -- JSON string con el análisis completo
  llm_duration_s       DOUBLE,                -- cuánto tardó la llamada al LLM

  validation_status    STRING,                -- 'passed' | 'diffs_detected' | 'failed' | 'skipped'
  validation_details   STRING,                -- JSON con conteos/sumas/diffs
  validation_run_id    BIGINT,                -- run_id del test run si se corrió

  status               STRING,                -- 'proposed' | 'approved' | 'rejected' | 'applied' | 'expired'
  status_updated_ts    TIMESTAMP,
  status_updated_by    STRING,
  notes                STRING                 -- comentarios de Marcos al aprobar/rechazar
)
USING DELTA
PARTITIONED BY (status)
COMMENT 'Data Doctor — propuestas de optimización generadas por el agente';

-- ------------------------------------------------------------
-- Tabla: applied_changes
-- Una fila por cada swap efectivo a producción.
-- Solo se inserta cuando el status de la proposal pasa a 'applied'.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS datadoc.applied_changes (
  applied_ts         TIMESTAMP,
  proposal_id        STRING,
  notebook_path      STRING,                  -- path que se reemplazó en prod
  backup_path        STRING,                  -- dónde quedó el backup del original
  applied_by         STRING,                  -- user que corrió Data_Approve
  rollback_ts        TIMESTAMP,               -- null hasta que alguien revierta
  rollback_by        STRING
)
USING DELTA
COMMENT 'Data Doctor — historial de cambios aplicados a notebooks productivos';

-- ------------------------------------------------------------
-- Índices lógicos útiles (solo Z-ORDER, Delta no tiene índices tradicionales)
-- ------------------------------------------------------------
OPTIMIZE datadoc.proposals ZORDER BY (proposal_id, status);
OPTIMIZE datadoc.applied_changes ZORDER BY (proposal_id);
