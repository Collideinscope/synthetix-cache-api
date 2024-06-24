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