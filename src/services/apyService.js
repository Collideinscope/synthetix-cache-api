const { knex, troyDBKnex } = require('../config/db');
const { CHAINS } = require('../helpers');

const {
  calculateDelta,
  calculatePercentage,
  calculateStandardDeviation,
  smoothData
} = require('../helpers');

const getLatestAPYData = async (chain) => {
  try {
    if (chain && CHAINS.includes(chain)) {
      // Fetch the latest value for the specified chain
      const result = await knex('apy')
        .where('chain', chain)
        .orderBy('ts', 'desc')
        .limit(1);

      return result;
    } 

    // Fetch the latest value for each chain otherwise
    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const result = await knex('apy')
          .where('chain', chain)
          .orderBy('ts', 'desc')
          .limit(1);

        return result[0];
      })
    );

    return results.filter(Boolean); // Filter out any undefined results
  } catch (error) {
    throw new Error('Error fetching latest APY data: ' + error.message);
  }
};

const getAllAPYData = async (chain) => {
  try {
    let query = knex('apy').orderBy('ts', 'asc');

    if (chain && CHAINS.includes(chain)) {
      query = query.where('chain', chain);
    }

    const result = await query;

    return result;
  } catch (error) {
    throw new Error('Error fetching all APY data: ' + error.message);
  }
};

const getAPYSummaryStats = async (chain) => {
  try {
    const startDate = new Date('2024-05-01');
    const baseQuery = () => knex('apy').where('chain', chain).andWhere('ts', '>=', startDate);

    const allData = await baseQuery().orderBy('ts', 'asc');
    if (allData.length === 0) {
      throw new Error('No data found for the specified chain');
    }

    const smoothedData = smoothData(allData);
    const reversedSmoothedData = [...smoothedData].reverse();

    const latestData = reversedSmoothedData[0];
    const latestTs = new Date(latestData.ts);

    const getDateFromLatest = (days) => new Date(latestTs.getTime() - days * 24 * 60 * 60 * 1000);

    const value24h = reversedSmoothedData.find(item => new Date(item.ts) <= getDateFromLatest(1));
    const value7d = reversedSmoothedData.find(item => new Date(item.ts) <= getDateFromLatest(7));
    const value28d = reversedSmoothedData.find(item => new Date(item.ts) <= getDateFromLatest(28));

    let valueYtd = smoothedData.find(item => new Date(item.ts) >= new Date(latestTs.getFullYear(), 0, 1));

    if (!valueYtd) {
      // use oldest date if no YTD data available or used
      valueYtd = reversedSmoothedData[reversedSmoothedData.length - 1]; 
    }

    const apyValues = smoothedData.map(item => parseFloat(item.apy_7d));
    const standardDeviation = calculateStandardDeviation(apyValues);

    const current = parseFloat(allData[allData.length - 1].apy_7d);
    // include comparison to current (unsmoothed) value 
    const ath = Math.max(...apyValues, current);
    const atl = Math.min(...apyValues, current);

    return {
      current: parseFloat(allData[allData.length - 1].apy_7d),
      delta_24h: calculateDelta(parseFloat(current), value24h ? parseFloat(value24h.apy_7d) : null),
      delta_7d: calculateDelta(parseFloat(current), value7d ? parseFloat(value7d.apy_7d) : null),
      delta_28d: calculateDelta(parseFloat(current), value28d ? parseFloat(value28d.apy_7d) : null),
      delta_ytd: calculateDelta(parseFloat(current), valueYtd ? parseFloat(valueYtd.apy_7d) : null),
      ath,
      atl,
      ath_percentage: calculatePercentage(parseFloat(current), ath),
      atl_percentage: calculatePercentage(parseFloat(current), atl),
      standard_deviation: standardDeviation
    };
  } catch (error) {
    throw new Error('Error fetching APY summary stats: ' + error.message);
  }
};

// initial seed
const fetchAndInsertAllAPYData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
  };

  try {
    const tableName = `prod_${chain}_mainnet.fct_core_apr_${chain}_mainnet`;

    // Fetch initial 
    const rows = await troyDBKnex.raw(`
      SELECT ts, pool_id, collateral_type, collateral_value, apy_24h, apy_7d, apy_28d
      FROM ${tableName}
      ORDER BY ts DESC;
    `);

    const dataWithChainAdded = rows.rows.map(row => {
      row.chain = chain;
      return row;
    });
    
    // Insert and handle conflicts
    await knex('apy')
      .insert(dataWithChainAdded)
      .onConflict(['ts', 'pool_id', 'collateral_type', 'chain'])
      .merge({
        collateral_value: knex.raw('excluded.collateral_value'),
        apy_24h: knex.raw('excluded.apy_24h'),
        apy_7d: knex.raw('excluded.apy_7d'),
        apy_28d: knex.raw('excluded.apy_28d'),
        ts: knex.raw('excluded.ts'), 
      });

    console.log(`APY data seeded successfully for ${chain}.`);
  } catch (error) {
    console.error('Error seeding APY data:', error);
  }
};

const fetchAndUpdateLatestAPYData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
  };

  try {
    const tableName = `prod_${chain}_mainnet.fct_core_apr_${chain}_mainnet`;

    // Fetch the last timestamp from the cache
    const lastTimestampResult = await knex('apy').where('chain', chain).max('ts as last_ts').first();
    const lastTimestamp = lastTimestampResult.last_ts || new Date(0);

    // Fetch new data starting from last ts
    const newRows = await troyDBKnex.raw(`
      SELECT ts, pool_id, collateral_type, collateral_value, apy_24h, apy_7d, apy_28d
      FROM ${tableName}
      WHERE ts > ?
      ORDER BY ts DESC;
    `, [lastTimestamp]);

    if (newRows.rows.length === 0) {
      console.log(`No new APY data to update for ${chain}.`);
      return;
    }

    const dataWithChainAdded = newRows.rows.map(row => {
      row.chain = chain;
      return row;
    });

    // Insert and handle conflicts
    await knex('apy')
      .insert(dataWithChainAdded)
      .onConflict(['ts', 'pool_id', 'collateral_type', 'chain'])
      .merge({
        collateral_value: knex.raw('excluded.collateral_value'),
        apy_24h: knex.raw('excluded.apy_24h'),
        apy_7d: knex.raw('excluded.apy_7d'),
        apy_28d: knex.raw('excluded.apy_28d'),
        ts: knex.raw('excluded.ts'), // Keep the newer value
      });

    console.log(`APY data updated successfully for ${chain}.`);  
  } catch (error) {
    console.error('Error updating APY data:', error);
  }
};

module.exports = {
  getLatestAPYData,
  getAllAPYData,
  fetchAndInsertAllAPYData,
  fetchAndUpdateLatestAPYData,
  getAPYSummaryStats,
};
