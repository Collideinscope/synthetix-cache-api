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
    if (chain && CHAINS.includes(chain)) {
      // Fetch the latest value for the specific chain
      const result = await knex('core_delegations')
        .where('chain', chain)
        .orderBy('ts', 'desc')
        .limit(1);

      return result;
    }

    // Fetch the latest value for each chain otherwise
    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const result = await knex('core_delegations')
          .where('chain', chain)
          .orderBy('ts', 'desc')
          .limit(1);

        return result[0];
      })
    );

    return results.filter(Boolean); // Filter out any undefined results
  } catch (error) {
    throw new Error('Error fetching latest core delegations data: ' + error.message);
  }
};

const getAllCoreDelegationsData = async (chain) => {
  try {
    let query = knex('core_delegations').orderBy('ts', 'asc');

    if (chain && CHAINS.includes(chain)) {
      query = query.where('chain', chain);
    }

    const result = await query;

    return result;
  } catch (error) {
    throw new Error('Error fetching all core delegations data: ' + error.message);
  }
};

const getCoreDelegationsSummaryStats = async (chain) => {
  try {
    const baseQuery = () => knex('core_delegations').where('chain', chain);

    const allData = await baseQuery().orderBy('ts', 'asc');
    if (allData.length === 0) {
      throw new Error('No data found for the specified chain');
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

const fetchDailyCoreDelegationsData = async (chain) => {
  const result = await knex.raw(`
    WITH daily_data AS (
      SELECT
        DATE_TRUNC('day', ts) AS date,
        SUM(amount_delegated) AS total_amount_delegated,
        LAG(SUM(amount_delegated)) OVER (ORDER BY DATE_TRUNC('day', ts)) AS prev_total_amount_delegated
      FROM core_delegations
      WHERE chain = ?
      GROUP BY DATE_TRUNC('day', ts)
      ORDER BY DATE_TRUNC('day', ts)
    )
    SELECT
      date,
      COALESCE(total_amount_delegated - prev_total_amount_delegated, total_amount_delegated) AS daily_delegations_change
    FROM daily_data
    WHERE prev_total_amount_delegated IS NOT NULL OR date = (SELECT MIN(date) FROM daily_data)
    ORDER BY date;
  `, [chain]);

  return result.rows.map(row => ({
    ts: row.date,
    daily_delegations_change: parseFloat(row.daily_delegations_change)
  }));
};

const getDailyCoreDelegationsData = async (chain) => {
  try {
    if (chain && CHAINS.includes(chain)) {
      const data = await fetchDailyCoreDelegationsData(chain);
      return { [chain]: data };
    }

    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const data = await fetchDailyCoreDelegationsData(chain);
        return { [chain]: data };
      })
    );

    return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
  } catch (error) {
    throw new Error('Error fetching daily core delegations data: ' + error.message);
  }
};

const getDailyCoreDelegationsSummaryStats = async (chain) => {
  try {
    const data = await getDailyCoreDelegationsData(chain);
    const dailyValues = data[chain].map(item => item.daily_delegations_change);

    if (dailyValues.length === 0) {
      throw new Error('No data found for the specified chain');
    }

    const smoothedData = smoothData(data[chain], 'daily_delegations_change');
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

    const standardDeviation = calculateStandardDeviation(dailyValues);

    const current = latestData.daily_delegations_change;
    const ath = Math.max(...dailyValues);
    const atl = Math.min(...dailyValues);

    return {
      current,
      delta_24h: calculateDelta(current, value24h ? value24h.daily_delegations_change : null),
      delta_7d: calculateDelta(current, value7d ? value7d.daily_delegations_change : null),
      delta_28d: calculateDelta(current, value28d ? value28d.daily_delegations_change : null),
      delta_ytd: calculateDelta(current, valueYtd ? valueYtd.daily_delegations_change : null),
      ath,
      atl,
      ath_percentage: calculatePercentage(current, ath),
      atl_percentage: atl === 0 ? 100 : calculatePercentage(current, atl),
      standard_deviation: standardDeviation
    };
  } catch (error) {
    throw new Error('Error fetching daily core delegations summary stats: ' + error.message);
  }
};

module.exports = {
  getLatestCoreDelegationsData,
  getAllCoreDelegationsData,
  fetchAndInsertAllCoreDelegationsData,
  fetchAndUpdateLatestCoreDelegationsData,
  getCoreDelegationsSummaryStats,
  getDailyCoreDelegationsData,
  getDailyCoreDelegationsSummaryStats,
};
