const { knex, troyDBKnex } = require('../config/db');
const { CHAINS } = require('../helpers');

const {
  calculateDelta,
  calculatePercentage,
  calculateStandardDeviation,
  smoothData
} = require('../helpers');

const getLatestCoreDelegationsData = async (chain) => {
  try {
    if (chain && !CHAINS.includes(chain)) {
      throw new Error('Invalid chain parameter');
    }

    const fetchLatest = async (chainToFetch) => {
      const result = await knex('core_delegations')
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
    throw new Error('Error fetching latest core delegations data: ' + error.message);
  }
};

const getCumulativeCoreDelegationsData = async (chain) => {
  try {
    if (chain && !CHAINS.includes(chain)) {
      throw new Error('Invalid chain parameter');
    }

    const fetchCumulative = async (chainToFetch) => {
      const result = await knex('core_delegations')
        .where('chain', chainToFetch)
        .orderBy('ts', 'asc');

      return { [chainToFetch]: result };
    };

    if (chain) {
      return await fetchCumulative(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchCumulative));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching cumulative core delegations data: ' + error.message);
  }
};

const getCoreDelegationsSummaryStats = async (chain) => {
  try {
    if (chain && !CHAINS.includes(chain)) {
      throw new Error('Invalid chain parameter');
    }

    const processChainData = async (chainToProcess) => {
      const baseQuery = () => knex('core_delegations').where('chain', chainToProcess);

      const allData = await baseQuery().orderBy('ts', 'asc');
      if (allData.length === 0) {
        return null; // No data for this chain
      }

      const smoothedData = smoothData(allData, 'amount_delegated'); 
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

      const delegationsValues = smoothedData.map(item => parseFloat(item.amount_delegated));
      const standardDeviation = calculateStandardDeviation(delegationsValues);

      const current = parseFloat(allData[allData.length - 1].amount_delegated);
      const ath = Math.max(...delegationsValues, current);
      const atl = Math.min(...delegationsValues, current);

      return {
        current,
        delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.amount_delegated) : null),
        delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.amount_delegated) : null),
        delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.amount_delegated) : null),
        delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.amount_delegated) : null),
        ath,
        atl,
        ath_percentage: calculatePercentage(current, ath),
        atl_percentage: atl === 0 ? 100 : calculatePercentage(current, atl),
        standard_deviation: standardDeviation
      };
    }

    if (chain) {
      const result = await processChainData(chain);
      return result ? { [chain]: result } : {};
    } else {
      const results = await Promise.all(CHAINS.map(processChainData));
      return CHAINS.reduce((acc, chain, index) => {
        if (results[index]) {
          acc[chain] = results[index];
        }
        return acc;
      }, {});
    }
  } catch (error) {
    throw new Error('Error fetching Core Delegations summary stats: ' + error.message);
  }
};

// Initial seed
const fetchAndInsertAllCoreDelegationsData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
    return;
  }

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = chain === 'base'
      ? `prod_${chain}_mainnet.fct_core_pool_delegation_${chain}_mainnet`
      : `${chain}_mainnet.fct_core_pool_delegation`;

    const rows = await troyDBKnex.raw(`
      SELECT ts, pool_id, collateral_type, amount_delegated
      FROM ${tableName}
      ORDER BY ts DESC;
    `);

    // Aggregate and transform the data to keep only the highest value per hour
    const rowsAggregatedByHour = rows.rows.reduce((acc, row) => {
      // soft contraints chain, ts, pool_id, collateral_type
      const hourKey = `${row.ts.toISOString().slice(0, 13)}_${row.pool_id}_${row.collateral_type}_${chain}`;

      if (!acc[hourKey] || row.amount_delegated > acc[hourKey].amount_delegated) {
        acc[hourKey] = {
          ...row,
          block_ts: row.ts,
          chain, // chain added
          ts: new Date(row.ts.toISOString().slice(0, 13) + ':00:00Z') // Set the timestamp to the start of the hour
        };
      }

      return acc;
    }, {});

    const dataToInsert = Object.values(rowsAggregatedByHour);

    await knex('core_delegations')
      .insert(dataToInsert)
      .onConflict(['chain', 'ts', 'pool_id', 'collateral_type'])
      .merge({
        amount_delegated: knex.raw('GREATEST(core_delegations.amount_delegated, excluded.amount_delegated)'),
        block_ts: knex.raw('CASE WHEN core_delegations.amount_delegated <= excluded.amount_delegated THEN excluded.block_ts ELSE core_delegations.block_ts END'),
      });

    console.log(`Core delegations data seeded successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error seeding core delegations data for ${chain} chain:`, error);
  }
};

const fetchAndUpdateLatestCoreDelegationsData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
    return;
  }

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = chain === 'base'
      ? `prod_${chain}_mainnet.fct_core_pool_delegation_${chain}_mainnet`
      : `${chain}_mainnet.fct_core_pool_delegation`;

    // Fetch the last timestamp from the cache
    const lastTimestampResult = await knex('core_delegations')
      .where('chain', chain)
      .orderBy('block_ts', 'desc')
      .first();

    const lastTimestamp = lastTimestampResult.block_ts;

    // Fetch new data starting from the last timestamp
    const newRows = await troyDBKnex.raw(`
      SELECT ts, pool_id, collateral_type, amount_delegated
      FROM ${tableName}
      WHERE ts > ?
      ORDER BY ts DESC;
    `, [lastTimestamp]);

    if (newRows.rows.length === 0) {
      console.log(`No new core delegations data to update for ${chain} chain.`);
      return;
    }

    // Transform the data to keep only the highest value per hour
    const rowsAggregatedByHour = newRows.rows.reduce((acc, row) => {
      // soft contraints chain, ts, pool_id, collateral_type
      const hourKey = `${row.ts.toISOString().slice(0, 13)}_${row.pool_id}_${row.collateral_type}_${chain}`;

      if (!acc[hourKey] || row.amount_delegated > acc[hourKey].amount_delegated) {
        acc[hourKey] = {
          ...row,
          block_ts: row.ts,
          chain, // chain added
          ts: new Date(row.ts.toISOString().slice(0, 13) + ':00:00Z') // Set the timestamp to the start of the hour
        };
      }

      return acc;
    }, {});

    const dataToInsert = Object.values(rowsAggregatedByHour);

    if (dataToInsert.length > 0) {
      await knex('core_delegations')
        .insert(dataToInsert)
        .onConflict(['chain', 'ts', 'pool_id', 'collateral_type'])
        .merge({
          amount_delegated: knex.raw('GREATEST(core_delegations.amount_delegated, excluded.amount_delegated)'),
          block_ts: knex.raw('CASE WHEN core_delegations.amount_delegated <= excluded.amount_delegated THEN excluded.block_ts ELSE core_delegations.block_ts END'),
        });
    }

    console.log(`Core delegations data updated successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error updating core delegations data for ${chain} chain:`, error);
  }
};

const getDailyCoreDelegationsData = async (chain) => {
  try {
    if (chain && !CHAINS.includes(chain)) {
      throw new Error('Invalid chain parameter');
    }

    const fetchDaily = async (chainToFetch) => {
      const result = await knex.raw(`
        WITH daily_data AS (
          SELECT
            DATE_TRUNC('day', ts) AS date,
            FIRST_VALUE(SUM(amount_delegated)) OVER (PARTITION BY DATE_TRUNC('day', ts) ORDER BY ts ASC) AS start_of_day_delegations,
            LAST_VALUE(SUM(amount_delegated)) OVER (PARTITION BY DATE_TRUNC('day', ts) ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_of_day_delegations
          FROM core_delegations
          WHERE chain = ?
            AND pool_id = 1
            AND collateral_type = '0xC74eA762cF06c9151cE074E6a569a5945b6302E7'
          GROUP BY DATE_TRUNC('day', ts), ts
        )
        SELECT DISTINCT
          date,
          end_of_day_delegations - start_of_day_delegations AS daily_delegations_change
        FROM daily_data
        ORDER BY date;
      `, [chainToFetch]);

      return { [chainToFetch]: result.rows.map(row => ({
        ts: row.date,
        daily_delegations_change: parseFloat(row.daily_delegations_change)
      })) };
    };

    if (chain) {
      return await fetchDaily(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchDaily));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching daily core delegations data: ' + error.message);
  }
};

module.exports = {
  getLatestCoreDelegationsData,
  getCumulativeCoreDelegationsData,
  fetchAndInsertAllCoreDelegationsData,
  fetchAndUpdateLatestCoreDelegationsData,
  getCoreDelegationsSummaryStats,
  getDailyCoreDelegationsData,
};
