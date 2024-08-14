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
    if (chain && CHAINS.includes(chain)) {
      // Fetch the latest value for the specific chain
      const result = await knex('tvl')
        .where('chain', chain)
        .orderBy('ts', 'desc')
        .limit(1);

      return { [chain]: result };
    }

    // Fetch the latest value for each chain otherwise
    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const result = await knex('tvl')
          .where('chain', chain)
          .orderBy('ts', 'desc')
          .limit(1);

        return { [chain]: result };
      })
    );

    return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
  } catch (error) {
    throw new Error('Error fetching latest TVL data: ' + error.message);
  }
};

const getAllTVLData = async (chain) => {
  try {
    const baseQuery = (chain) => knex('tvl')
      .where({
        chain: chain,
        pool_id: 1,
        collateral_type: '0xc74ea762cf06c9151ce074e6a569a5945b6302e7'
      })
      .orderBy('ts', 'asc');

    if (chain && CHAINS.includes(chain)) {
      const result = await baseQuery(chain);
      return { [chain]: result };
    }

    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const result = await baseQuery(chain);
        return { [chain]: result };
      })
    );

    return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
  } catch (error) {
    throw new Error('Error fetching all TVL data: ' + error.message);
  }
};

const getTVLSummaryStats = async (chain) => {
  try {
    const baseQuery = () => knex('tvl')
      .where({
        chain: chain,
        pool_id: 1,
        collateral_type: '0xc74ea762cf06c9151ce074e6a569a5945b6302e7'
      });

    const allData = await baseQuery().orderBy('ts', 'asc');
    if (allData.length === 0) {
      throw new Error('No data found for the specified chain');
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
  } catch (error) {
    throw new Error('Error fetching TVL summary stats: ' + error.message);
  }
};

// initial seed
const fetchAndInsertAllTVLData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
  };

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = `${chain}_mainnet.core_vault_collateral`;

    const rows = await troyDBKnex.raw(`
      SELECT ts, block_number, pool_id, collateral_type, contract_address, amount, collateral_value
      FROM ${tableName}
      ORDER BY ts DESC;
    `);

    // Aggregate and transform the data to keep only the highest value per hour
    const rowsAggregatedByHour = rows.rows.reduce((acc, row) => {
      // soft contraints chain, ts, pool_id, collateral_type
      const hourKey = `${row.ts.toISOString().slice(0, 13)}_${row.pool_id}_${row.collateral_type}_${chain}`; // Format as 'YYYY-MM-DDTHH_poolId_collateralType_chain'

      if (!acc[hourKey] || row.amount > acc[hourKey].amount) {
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
    console.error(`Chain must be provided for data updates.`);
    return;
  }

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = `${chain}_mainnet.core_vault_collateral`;

    // Fetch the last timestamp from the cache
    const lastTimestampResult = await knex('tvl')
      .where('chain', chain)
      .orderBy('block_ts', 'desc')
      .first();

    const lastTimestamp = lastTimestampResult.block_ts;

    // Fetch new data starting from the last timestamp
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

    // Transform the data to keep only the highest value per hour
    const rowsAggregatedByHour = newRows.rows.reduce((acc, row) => {
      // soft contraints chain, ts, pool_id, collateral_type
      const hourKey = `${row.ts.toISOString().slice(0, 13)}_${row.pool_id}_${row.collateral_type}_${chain}`; // Format as 'YYYY-MM-DDTHH_poolId_collateralType_chain'

      if (!acc[hourKey] || row.amount > acc[hourKey].amount) {
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

const fetchDailyTVLData = async (chain) => {
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
  `, [chain]);

  return result.rows.map(row => ({
    ts: row.date,
    daily_tvl_change: parseFloat(row.daily_tvl_change)
  }));
};

const getDailyTVLData = async (chain) => {
  try {
    if (chain && CHAINS.includes(chain)) {
      const data = await fetchDailyTVLData(chain);
      return { [chain]: data };
    }

    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const data = await fetchDailyTVLData(chain);
        return { [chain]: data };
      })
    );

    return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
  } catch (error) {
    throw new Error('Error fetching daily TVL data: ' + error.message);
  }
};

const getDailyTVLSummaryStats = async (chain) => {
  try {
    const data = await getDailyTVLData(chain);
    const dailyValues = data[chain].map(item => item.daily_tvl_change);

    if (dailyValues.length === 0) {
      throw new Error('No data found for the specified chain');
    }

    const smoothedData = smoothData(data[chain], 'daily_tvl_change');
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

    const current = latestData.daily_tvl_change;
    const ath = Math.max(...dailyValues);
    const atl = Math.min(...dailyValues);

    return {
      current,
      delta_24h: calculateDelta(current, value24h ? value24h.daily_tvl_change : null),
      delta_7d: calculateDelta(current, value7d ? value7d.daily_tvl_change : null),
      delta_28d: calculateDelta(current, value28d ? value28d.daily_tvl_change : null),
      delta_ytd: calculateDelta(current, valueYtd ? valueYtd.daily_tvl_change : null),
      ath,
      atl,
      ath_percentage: calculatePercentage(current, ath),
      atl_percentage: atl === 0 ? 100 : calculatePercentage(current, atl),
      standard_deviation: standardDeviation
    };
  } catch (error) {
    throw new Error('Error fetching daily TVL summary stats: ' + error.message);
  }
};

module.exports = {
  getLatestTVLData,
  getAllTVLData,
  fetchAndInsertAllTVLData,
  fetchAndUpdateLatestTVLData,
  getTVLSummaryStats,
  getDailyTVLData,
  getDailyTVLSummaryStats,
};
