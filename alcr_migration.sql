-- SmartFS / SmartFX ALCR v1.0 research schema
-- Safe/additive. No V2/V3 live signal tables are changed.

ALTER TABLE public.bot_status
  ADD COLUMN IF NOT EXISTS strategy_alcr_enabled BOOLEAN DEFAULT TRUE;

UPDATE public.bot_status
SET strategy_alcr_enabled = COALESCE(strategy_alcr_enabled, TRUE)
WHERE id = 1;

CREATE TABLE IF NOT EXISTS public.alcr_research_log (
  id BIGSERIAL PRIMARY KEY,
  pair TEXT NOT NULL,
  market_type TEXT NOT NULL,
  strategy_version TEXT NOT NULL DEFAULT 'ALCR_V1.0',
  setup_type TEXT,
  market_state TEXT,
  volatility_state TEXT,
  timeframe TEXT NOT NULL DEFAULT '5m',
  direction TEXT,
  score NUMERIC,
  environment_score NUMERIC,
  location_score NUMERIC,
  setup_score NUMERIC,
  momentum_score NUMERIC,
  structure_score NUMERIC,
  risk_score NUMERIC,
  target_score NUMERIC,
  entry DOUBLE PRECISION,
  sl DOUBLE PRECISION,
  tp1 DOUBLE PRECISION,
  tp2 DOUBLE PRECISION,
  risk_reward DOUBLE PRECISION,
  candidate_detected BOOLEAN NOT NULL DEFAULT FALSE,
  status TEXT NOT NULL,
  outcome TEXT,
  reason TEXT,
  failed_gate TEXT,
  detected_at TIMESTAMPTZ,
  triggered_at TIMESTAMPTZ,
  entry_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  mfe DOUBLE PRECISION,
  mae DOUBLE PRECISION,
  r_multiple DOUBLE PRECISION,
  session TEXT,
  correlation_group TEXT,
  analysis_details JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alcr_research_pair_created
  ON public.alcr_research_log (pair, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alcr_research_status_created
  ON public.alcr_research_log (status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alcr_research_strategy_created
  ON public.alcr_research_log (strategy_version, created_at DESC);

CREATE TABLE IF NOT EXISTS public.alcr_research_trades (
  id BIGSERIAL PRIMARY KEY,
  research_log_id BIGINT REFERENCES public.alcr_research_log(id) ON DELETE SET NULL,
  pair TEXT NOT NULL,
  market_type TEXT NOT NULL,
  setup_type TEXT NOT NULL,
  direction TEXT NOT NULL,
  entry DOUBLE PRECISION NOT NULL,
  sl DOUBLE PRECISION NOT NULL,
  tp1 DOUBLE PRECISION NOT NULL,
  tp2 DOUBLE PRECISION,
  score NUMERIC,
  opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMPTZ,
  status TEXT NOT NULL DEFAULT 'ACTIVE',
  outcome TEXT,
  exit_price DOUBLE PRECISION,
  completed_at TIMESTAMPTZ,
  mfe DOUBLE PRECISION NOT NULL DEFAULT 0,
  mae DOUBLE PRECISION NOT NULL DEFAULT 0,
  r_multiple DOUBLE PRECISION,
  session TEXT,
  strategy_version TEXT NOT NULL DEFAULT 'ALCR_V1.0'
);

CREATE INDEX IF NOT EXISTS idx_alcr_research_trades_pair_status
  ON public.alcr_research_trades (pair, status);
CREATE UNIQUE INDEX IF NOT EXISTS idx_alcr_one_active_pair
  ON public.alcr_research_trades (pair) WHERE status = 'ACTIVE';

ALTER TABLE public.alcr_research_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.alcr_research_trades ENABLE ROW LEVEL SECURITY;

GRANT SELECT ON public.alcr_research_log TO authenticated;
GRANT SELECT ON public.alcr_research_trades TO authenticated;

DO $$
BEGIN
  CREATE POLICY alcr_research_select_authenticated
    ON public.alcr_research_log
    FOR SELECT TO authenticated USING (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  CREATE POLICY alcr_research_trades_select_authenticated
    ON public.alcr_research_trades
    FOR SELECT TO authenticated USING (true);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'supabase_realtime')
     AND NOT EXISTS (
       SELECT 1 FROM pg_publication_tables
       WHERE pubname = 'supabase_realtime'
         AND schemaname = 'public'
         AND tablename = 'alcr_research_log'
     ) THEN
    ALTER PUBLICATION supabase_realtime ADD TABLE public.alcr_research_log;
  END IF;

  IF EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'supabase_realtime')
     AND NOT EXISTS (
       SELECT 1 FROM pg_publication_tables
       WHERE pubname = 'supabase_realtime'
         AND schemaname = 'public'
         AND tablename = 'alcr_research_trades'
     ) THEN
    ALTER PUBLICATION supabase_realtime ADD TABLE public.alcr_research_trades;
  END IF;
EXCEPTION WHEN undefined_object THEN NULL;
END $$;
