const { knex, troyDBKnex } = require('../config/db');
// const { CHAINS } = require('../helpers');
const {
  calculateDelta,
  calculatePercentage,
  calculateStandardDeviation,
  smoothData
} = require('../helpers');

// base only for now 
const CHAINS = ['base'];

const getLatestPerpStatsData = async (chain) => {
  try {
    if (chain && CHAINS.includes(chain)) {
      const result = await knex('perp_stats')
        .where('chain', chain)
        .orderBy('ts', 'desc')
        .limit(1);

      return result;
    }

    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const result = await knex('perp_stats')
          .where('chain', chain)
          .orderBy('ts', 'desc')
          .limit(1);

        return result[0];
      })
    );

    return results.filter(Boolean);
  } catch (error) {
    throw new Error('Error fetching latest perp stats data: ' + error.message);
  }
};

const getAllPerpStatsData = async (chain) => {
  try {
    let query = knex('perp_stats').orderBy('ts', 'asc');

    if (chain && CHAINS.includes(chain)) {
      query = query.where('chain', chain);
    }

    const result = await query;

    return result;
  } catch (error) {
    throw new Error('Error fetching all perp stats data: ' + error.message);
  }
};

const getCumulativeVolumeSummarystats = async (chain) => {
  try {
    const baseQuery = () => knex('perp_stats').where('chain', chain);

    const allData = await baseQuery().orderBy('ts', 'asc');
    if (allData.length === 0) {
      throw new Error('No data found for the specified chain');
    }

    const smoothedData = smoothData(allData, 'cumulative_volume');  
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

    const volumeValues = smoothedData.map(item => parseFloat(item.cumulative_volume));
    const standardDeviation = calculateStandardDeviation(volumeValues);

    const current = parseFloat(allData[allData.length - 1].cumulative_volume);
    const ath = Math.max(...volumeValues, current);
    const atl = Math.min(...volumeValues, current);

    return {
      current,
      delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.cumulative_volume) : null),
      delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.cumulative_volume) : null),
      delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.cumulative_volume) : null),
      delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.cumulative_volume) : null),
      ath,
      atl,
      ath_percentage: calculatePercentage(current, ath),
      atl_percentage: atl === 0 ? 100 : calculatePercentage(current, atl),
      standard_deviation: standardDeviation
    };
  } catch (error) {
    throw new Error('Error fetching perp stats summary stats: ' + error.message);
  }
};

const getCumulativeExchangeFeesSummaryData = async (chain) => {
  try {
    const baseQuery = () => knex('perp_stats').where('chain', chain);

    const allData = await baseQuery().orderBy('ts', 'asc');
    if (allData.length === 0) {
      throw new Error('No data found for the specified chain');
    }

    const smoothedData = smoothData(allData, 'cumulative_exchange_fees');  // Smooth perp stats data
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

    const perpValues = smoothedData.map(item => parseFloat(item.cumulative_exchange_fees));
    const standardDeviation = calculateStandardDeviation(perpValues);

    const current = parseFloat(allData[allData.length - 1].cumulative_exchange_fees);
    const ath = Math.max(...perpValues, current);
    const atl = Math.min(...perpValues, current);

    return {
      current,
      delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.cumulative_exchange_fees) : null),
      delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.cumulative_exchange_fees) : null),
      delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.cumulative_exchange_fees) : null),
      delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.cumulative_exchange_fees) : null),
      ath,
      atl,
      ath_percentage: calculatePercentage(current, ath),
      atl_percentage: atl === 0 ? 100 : calculatePercentage(current, atl),
      standard_deviation: standardDeviation
    };
  } catch (error) {
    throw new Error('Error fetching perp stats summary stats: ' + error.message);
  }
};

const getCumulativeCollectedFeesSummaryData = async (chain) => {
  try {
    const baseQuery = () => knex('perp_stats').where('chain', chain);

    const allData = await baseQuery().orderBy('ts', 'asc');
    if (allData.length === 0) {
      throw new Error('No data found for the specified chain');
    }

    const smoothedData = smoothData(allData, 'cumulative_collected_fees');  // Smooth perp stats data
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

    const perpValues = smoothedData.map(item => parseFloat(item.cumulative_exchange_fees));
    const standardDeviation = calculateStandardDeviation(perpValues);

    const current = parseFloat(allData[allData.length - 1].cumulative_exchange_fees);
    const ath = Math.max(...perpValues, current);
    const atl = Math.min(...perpValues, current);

    return {
      current,
      delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.cumulative_exchange_fees) : null),
      delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.cumulative_exchange_fees) : null),
      delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.cumulative_exchange_fees) : null),
      delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.cumulative_exchange_fees) : null),
      ath,
      atl,
      ath_percentage: calculatePercentage(current, ath),
      atl_percentage: atl === 0 ? 100 : calculatePercentage(current, atl),
      standard_deviation: standardDeviation
    };
  } catch (error) {
    throw new Error('Error fetching perp stats summary stats: ' + error.message);
  }
};

const fetchAndInsertAllPerpStatsData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
  }

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = `prod_${chain}_mainnet.fct_perp_stats_daily_${chain}_mainnet`;

    const rows = await troyDBKnex.raw(`
      SELECT ts, cumulative_volume, cumulative_collected_fees, cumulative_exchange_fees
      FROM ${tableName}
      ORDER BY ts DESC;
    `);

    const dataToInsert = rows.rows.map(row => ({
      ...row,
      chain,
    }));

    await knex('perp_stats')
      .insert(dataToInsert)
      .onConflict(['chain', 'ts'])
      .merge({
        cumulative_volume: knex.raw('GREATEST(perp_stats.cumulative_volume, excluded.cumulative_volume)'),
        cumulative_collected_fees: knex.raw('EXCLUDED.cumulative_collected_fees'),
        cumulative_exchange_fees: knex.raw('EXCLUDED.cumulative_exchange_fees')
      });

    console.log(`Perp stats data seeded successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error seeding perp stats data for ${chain} chain:`, error);
  }
};

const fetchAndUpdateLatestPerpStatsData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
    return;
  }

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = `prod_${chain}_mainnet.fct_perp_stats_daily_${chain}_mainnet`;

    const lastTimestampResult = await knex('perp_stats')
      .where('chain', chain)
      .orderBy('ts', 'desc')
      .first();

    const lastTimestamp = lastTimestampResult.ts;

    const newRows = await troyDBKnex.raw(`
      SELECT ts, cumulative_volume, cumulative_collected_fees, cumulative_exchange_fees
      FROM ${tableName}
      WHERE ts > ?
      ORDER BY ts DESC;
    `, [lastTimestamp]);

    if (newRows.rows.length === 0) {
      console.log(`No new perp stats data to update for ${chain} chain.`);
      return;
    }

    const dataToInsert = newRows.rows.map(row => ({
      ...row,
      chain,
    }));

    if (dataToInsert.length > 0) {
      await knex('perp_stats')
        .insert(dataToInsert)
        .onConflict(['chain', 'ts'])
        .merge({
          cumulative_volume: knex.raw('EXCLUDED.cumulative_volume'),
          cumulative_collected_fees: knex.raw('EXCLUDED.cumulative_collected_fees'),
          cumulative_exchange_fees: knex.raw('EXCLUDED.cumulative_exchange_fees')        });
    }

    console.log(`Perp stats data updated successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error updating perp stats data for ${chain} chain:`, error);
  }
};

module.exports = {
  getLatestPerpStatsData,
  getAllPerpStatsData,
  fetchAndInsertAllPerpStatsData,
  fetchAndUpdateLatestPerpStatsData,
  getCumulativeVolumeSummarystats,
  getCumulativeExchangeFeesSummaryData,
  getCumulativeCollectedFeesSummaryData,
};
