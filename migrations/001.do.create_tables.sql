CREATE TABLE apy (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ts TIMESTAMP WITH TIME ZONE NOT NULL,
    chain TEXT NOT NULL,
    pool_id INTEGER NOT NULL,
    collateral_type TEXT NOT NULL,
    collateral_value NUMERIC NOT NULL,
    apy_24h NUMERIC NOT NULL,
    apy_7d NUMERIC,
    apy_28d NUMERIC,
    UNIQUE (chain, ts, pool_id, collateral_type)
);

CREATE INDEX idx_apy_chain_ts ON apy(chain, ts);
CREATE INDEX idx_apy_pool_id ON apy(pool_id);

CREATE TABLE tvl (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ts TIMESTAMP WITH TIME ZONE NOT NULL,
    chain TEXT NOT NULL,
    pool_id INTEGER NOT NULL,
    collateral_type TEXT NOT NULL,
    amount NUMERIC(30, 10) NOT NULL,
    collateral_value NUMERIC(30, 10) NOT NULL,
    block_ts TIMESTAMP WITH TIME ZONE NOT NULL,
    block_number INTEGER NOT NULL,
    contract_address TEXT NOT NULL,
    UNIQUE (chain, ts, pool_id, collateral_type)
);

CREATE INDEX idx_tvl_chain_ts ON tvl(chain, ts);
CREATE INDEX idx_tvl_pool_id ON tvl(pool_id);

CREATE TABLE core_delegations (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ts TIMESTAMP WITH TIME ZONE NOT NULL,
    chain TEXT NOT NULL,
    pool_id INTEGER NOT NULL,
    collateral_type TEXT NOT NULL,
    amount_delegated NUMERIC NOT NULL,
    block_ts TIMESTAMP WITH TIME ZONE NOT NULL,
    UNIQUE (chain, ts, pool_id, collateral_type)
);

CREATE INDEX idx_core_delegations_chain_ts ON core_delegations(chain, ts);
CREATE INDEX idx_core_delegations_pool_id ON core_delegations(pool_id);

CREATE TABLE core_account_delegations (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ts TIMESTAMP WITH TIME ZONE NOT NULL,
    chain TEXT NOT NULL,
    account_id TEXT NOT NULL,
    pool_id INTEGER NOT NULL,
    collateral_type TEXT NOT NULL,
    amount_delegated NUMERIC(30, 10) NOT NULL,
    UNIQUE (chain, account_id, pool_id, collateral_type)
);

CREATE INDEX idx_core_account_delegations_chain_account_id ON core_account_delegations(chain, account_id);
CREATE INDEX idx_core_account_delegations_pool_id ON core_account_delegations(pool_id);

CREATE TABLE pool_rewards (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ts TIMESTAMP WITH TIME ZONE NOT NULL,
    chain TEXT NOT NULL,
    pool_id INTEGER NOT NULL,
    collateral_type TEXT NOT NULL,
    rewards_usd NUMERIC NOT NULL,
    UNIQUE (chain, ts, pool_id, collateral_type)
);

CREATE INDEX idx_pool_rewards_chain_ts ON pool_rewards(chain, ts);
CREATE INDEX idx_pool_rewards_pool_id ON pool_rewards(pool_id);

CREATE TABLE perp_stats (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ts TIMESTAMP WITH TIME ZONE NOT NULL,
    chain TEXT NOT NULL,
    cumulative_volume NUMERIC NOT NULL,
    cumulative_collected_fees NUMERIC NOT NULL,
    cumulative_exchange_fees NUMERIC NOT NULL,
    UNIQUE (chain, ts)
);

CREATE INDEX idx_perp_stats_chain_ts ON perp_stats(chain, ts);

CREATE TABLE perp_account_stats (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ts TIMESTAMP WITH TIME ZONE NOT NULL,
    chain TEXT NOT NULL,
    account_id TEXT NOT NULL,
    fees NUMERIC NOT NULL,
    volume NUMERIC NOT NULL,
    amount_liquidated NUMERIC NOT NULL,
    liquidations INTEGER NOT NULL,
    cumulative_fees NUMERIC NOT NULL,
    cumulative_volume NUMERIC NOT NULL,
    UNIQUE (chain, ts, account_id)
);

CREATE INDEX idx_perp_account_stats_chain_ts ON perp_account_stats(chain, ts);
CREATE INDEX idx_perp_account_stats_account_id ON perp_account_stats(account_id);

CREATE TABLE perp_market_history (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    ts TIMESTAMP WITH TIME ZONE NOT NULL,
    chain TEXT NOT NULL,
    block_number INTEGER NOT NULL,
    market_id INTEGER NOT NULL,
    market_symbol TEXT NOT NULL,
    price NUMERIC NOT NULL,
    size NUMERIC NOT NULL,
    funding_rate NUMERIC NOT NULL,
    long_rate_apr NUMERIC NOT NULL,
    short_rate_apr NUMERIC NOT NULL,
    size_usd NUMERIC NOT NULL,
    long_oi NUMERIC NOT NULL,
    short_oi NUMERIC NOT NULL,
    long_oi_pct NUMERIC,
    short_oi_pct NUMERIC,
    UNIQUE (chain, ts, market_id)
);

CREATE INDEX idx_perp_market_history_ts ON perp_market_history(chain, ts);
CREATE INDEX idx_perp_market_history_market_id ON perp_market_history(market_id);

-- update_at column triggers
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
   NEW.updated_at = NOW();
   RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_apy_updated_at
BEFORE UPDATE ON apy
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER update_tvl_updated_at
BEFORE UPDATE ON tvl
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER update_core_delegations_updated_at
BEFORE UPDATE ON core_delegations
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER update_core_account_delegations_updated_at
BEFORE UPDATE ON core_account_delegations
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER update_pool_rewards_updated_at
BEFORE UPDATE ON pool_rewards
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER update_perp_stats_updated_at
BEFORE UPDATE ON perp_stats
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER update_perp_account_stats_updated_at
BEFORE UPDATE ON perp_account_stats
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();

CREATE TRIGGER update_perp_market_history
BEFORE UPDATE ON perp_market_history
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();