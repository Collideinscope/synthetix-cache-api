const { knex, troyDBKnex } = require('../config/db');
const { CHAINS } = require('../helpers');

const {
  calculateDelta,
  calculatePercentage,
  calculateStandardDeviation,
  smoothData
} = require('../helpers');

const getLatestTVLData = async (chain) => {
  try {
    if (chain && !CHAINS.includes(chain)) {
      throw new Error('Invalid chain parameter');
    }

    const fetchLatest = async (chainToFetch) => {
      const result = await knex('tvl')
        .where('chain', chainToFetch)
        .orderBy('ts', 'desc')
        .limit(1);
      return { [chainToFetch]: result };
    };

    if (chain) {
      return await fetchLatest(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchLatest));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching latest TVL data: ' + error.message);
  }
};

const getCumulativeTVLData = async (chain) => {
  try {
    if (chain && !CHAINS.includes(chain)) {
      throw new Error('Invalid chain parameter');
    }

    const baseQuery = (chainToQuery) => knex('tvl')
      .where({
        chain: chainToQuery,
        pool_id: 1,
        collateral_type: '0xc74ea762cf06c9151ce074e6a569a5945b6302e7'
      })
      .orderBy('ts', 'asc');

    const fetchAll = async (chainToFetch) => {
      const result = await baseQuery(chainToFetch);
      return { [chainToFetch]: result };
    };

    if (chain) {
      return await fetchAll(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchAll));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching all TVL data: ' + error.message);
  }
};

const getTVLSummaryStats = async (chain) => {
  try {
    if (chain && !CHAINS.includes(chain)) {
      throw new Error('Invalid chain parameter');
    }

    const processChainData = async (chainToProcess) => {
      const baseQuery = () => knex('tvl')
        .where({
          chain: chainToProcess,
          pool_id: 1,
          collateral_type: '0xc74ea762cf06c9151ce074e6a569a5945b6302e7'
        });

      const allData = await baseQuery().orderBy('ts', 'asc');
      if (allData.length === 0) {
        return null; // No data for this chain
      }

      const smoothedData = smoothData(allData, 'collateral_value');
      const reversedSmoothedData = [...smoothedData].reverse();

      const latestData = reversedSmoothedData[0];
      const latestTs = new Date(latestData.ts);

      const getDateFromLatest = (days) => new Date(latestTs.getTime() - days * 24 * 60 * 60 * 1000);

      const value24h = reversedSmoothedData.find(item => new Date(item.ts) <= getDateFromLatest(1));
      const value7d = reversedSmoothedData.find(item => new Date(item.ts) <= getDateFromLatest(7));
      const value28d = reversedSmoothedData.find(item => new Date(item.ts) <= getDateFromLatest(28));

      let valueYtd = smoothedData.find(item => new Date(item.ts) >= new Date(latestTs.getFullYear(), 0, 1));

      if (!valueYtd) {
        valueYtd = reversedSmoothedData[reversedSmoothedData.length - 1];
      }

      const tvlValues = smoothedData.map(item => parseFloat(item.collateral_value));
      const standardDeviation = calculateStandardDeviation(tvlValues);

      const current = parseFloat(allData[allData.length - 1].collateral_value);
      const ath = Math.max(...tvlValues, current);
      const atl = Math.min(...tvlValues, current);

      return {
        current,
        delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.collateral_value) : null),
        delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.collateral_value) : null),
        delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.collateral_value) : null),
        delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.collateral_value) : null),
        ath,
        atl,
        ath_percentage: calculatePercentage(current, ath),
        atl_percentage: atl === 0 ? 100 : calculatePercentage(current, atl),
        standard_deviation: standardDeviation
      };
    };

    if (chain) {
      const result = await processChainData(chain);
      return result ? { [chain]: result } : {};
    } else {
      const results = await Promise.all(CHAINS.map(processChainData));
      return CHAINS.reduce((acc, chain, index) => {
        acc[chain] = results[index] || {};
        return acc;
      }, {});
    }
  } catch (error) {
    throw new Error('Error fetching TVL summary stats: ' + error.message);
  }
};

const fetchAndInsertTVLData = async (chain) => {
  if (!chain) {
    throw new Error('Chain must be provided for data updates.');
  }

  if (!CHAINS.includes(chain)) {
    throw new Error(`Chain ${chain} not recognized.`);
  }

  try {
    const tableName = chain === 'base'
      ? `prod_${chain}_mainnet.fct_core_vault_collateral_${chain}_mainnet`
      : `${chain}_mainnet.core_vault_collateral`;

    const rows = await troyDBKnex.raw(`
      SELECT ts, block_number, pool_id, collateral_type, contract_address, amount, collateral_value
      FROM ${tableName}
      ORDER BY ts DESC;
    `);

    const rowsAggregatedByHour = rows.rows.reduce((acc, row) => {
      const hourKey = `${row.ts.toISOString().slice(0, 13)}_${row.pool_id}_${row.collateral_type}_${chain}`;

      if (!acc[hourKey] || row.amount > acc[hourKey].amount) {
        acc[hourKey] = {
          ...row,
          block_ts: row.ts,
          chain,
          ts: new Date(row.ts.toISOString().slice(0, 13) + ':00:00Z')
        };
      }
    
      return acc;
    }, {});

    const dataToInsert = Object.values(rowsAggregatedByHour);

    await knex('tvl')
      .insert(dataToInsert)
      .onConflict(['chain', 'ts', 'pool_id', 'collateral_type'])
      .merge({
        amount: knex.raw('GREATEST(tvl.amount, excluded.amount)'),
        block_ts: knex.raw('CASE WHEN tvl.amount <= excluded.amount THEN excluded.block_ts ELSE tvl.block_ts END'),
      });

    console.log(`TVL data seeded successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error seeding TVL data for ${chain} chain:`, error);
  }
};

const fetchAndUpdateLatestTVLData = async (chain) => {
  if (!chain) {
    throw new Error('Chain must be provided for data updates.');
  }

  if (!CHAINS.includes(chain)) {
    throw new Error(`Chain ${chain} not recognized.`);
  }

  try {
    const tableName = chain === 'base'
      ? `prod_${chain}_mainnet.fct_core_vault_collateral_${chain}_mainnet`
      : `${chain}_mainnet.core_vault_collateral`;

    const lastTimestampResult = await knex('tvl')
      .where('chain', chain)
      .orderBy('block_ts', 'desc')
      .first();

    const lastTimestamp = lastTimestampResult.block_ts;

    const newRows = await troyDBKnex.raw(`
      SELECT ts, block_number, pool_id, collateral_type, contract_address, amount, collateral_value
      FROM ${tableName}
      WHERE ts > ?
      ORDER BY ts DESC;
    `, [lastTimestamp]);

    if (newRows.rows.length === 0) {
      console.log(`No new TVL data to update for ${chain} chain.`);
      return;
    }

    const rowsAggregatedByHour = newRows.rows.reduce((acc, row) => {
      const hourKey = `${row.ts.toISOString().slice(0, 13)}_${row.pool_id}_${row.collateral_type}_${chain}`;

      if (!acc[hourKey] || row.amount > acc[hourKey].amount) {
        acc[hourKey] = {
          ...row,
          block_ts: row.ts,
          chain,
          ts: new Date(row.ts.toISOString().slice(0, 13) + ':00:00Z')
        };
      }
    
      return acc;
    }, {});

    const dataToInsert = Object.values(rowsAggregatedByHour);

    if (dataToInsert.length > 0) {
      await knex('tvl')
        .insert(dataToInsert)
        .onConflict(['chain', 'ts', 'pool_id', 'collateral_type'])
        .merge({
          amount: knex.raw('GREATEST(tvl.amount, excluded.amount)'),
          block_ts: knex.raw('CASE WHEN tvl.amount <= excluded.amount THEN excluded.block_ts ELSE tvl.block_ts END'),
        });
    }

    console.log(`TVL data updated successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error updating TVL data for ${chain} chain:`, error);
  }
};

const getDailyTVLData = async (chain) => {
  try {
    if (chain && !CHAINS.includes(chain)) {
      throw new Error('Invalid chain parameter');
    }

    const fetchDaily = async (chainToFetch) => {
      const result = await knex.raw(`
        WITH daily_data AS (
          SELECT
            DATE_TRUNC('day', ts) AS date,
            FIRST_VALUE(SUM(collateral_value)) OVER (PARTITION BY DATE_TRUNC('day', ts) ORDER BY ts ASC) AS start_of_day_tvl,
            LAST_VALUE(SUM(collateral_value)) OVER (PARTITION BY DATE_TRUNC('day', ts) ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_of_day_tvl
          FROM tvl
          WHERE chain = ?
            AND pool_id = 1
            AND collateral_type = '0xc74ea762cf06c9151ce074e6a569a5945b6302e7'
          GROUP BY DATE_TRUNC('day', ts), ts
        )
        SELECT DISTINCT
          date,
          end_of_day_tvl - start_of_day_tvl AS daily_tvl_change
        FROM daily_data
        ORDER BY date;
      `, [chainToFetch]);

      const data = result.rows.map(row => ({
        ts: row.date,
        daily_tvl_change: parseFloat(row.daily_tvl_change)
      }));

      return { [chainToFetch]: data };
    };

    if (chain) {
      return await fetchDaily(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchDaily));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching daily TVL data: ' + error.message);
  }
};

module.exports = {
  getLatestTVLData,
  getCumulativeTVLData,
  fetchAndInsertTVLData,
  fetchAndUpdateLatestTVLData,
  getTVLSummaryStats,
  getDailyTVLData,
};
