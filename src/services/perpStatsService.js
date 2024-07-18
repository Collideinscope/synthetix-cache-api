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

const getCumulativeVolumeSummaryStats = async (chain) => {
  return getSummaryStats(chain, 'cumulative_volume');
};

const getCumulativeExchangeFeesSummaryStats = async (chain) => {
  return getSummaryStats(chain, 'cumulative_exchange_fees');
};

const getCumulativeCollectedFeesSummaryStats = async (chain) => {
  return getSummaryStats(chain, 'cumulative_collected_fees');
};

const getSummaryStats = async (chain, column) => {
  try {
    const baseQuery = () => knex('perp_stats').where('chain', chain);

    const allData = await baseQuery().orderBy('ts', 'asc');
    if (allData.length === 0) {
      throw new Error('No data found for the specified chain');
    }

    const smoothedData = smoothData(allData, column);  
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

    const columnValues = smoothedData.map(item => parseFloat(item[column]));
    const standardDeviation = calculateStandardDeviation(columnValues);

    const current = parseFloat(allData[allData.length - 1][column]);
    const ath = Math.max(...columnValues, current);
    const atl = Math.min(...columnValues, current);

    return {
      current,
      delta_24h: calculateDelta(current, value24h ? parseFloat(value24h[column]) : null),
      delta_7d: calculateDelta(current, value7d ? parseFloat(value7d[column]) : null),
      delta_28d: calculateDelta(current, value28d ? parseFloat(value28d[column]) : null),
      delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd[column]) : null),
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

const fetchCumulativeData = async (chain, dataType) => {
  try {
    const result = await knex.raw(`
      SELECT 
        ts,
        ${dataType}
      FROM 
        perp_stats
      WHERE
        chain = ?
      ORDER BY 
        ts;
    `, [chain]);

    return result.rows.map(row => ({
      ts: row.ts,
      [dataType]: row[dataType],
    }));
  } catch (error) {
    throw new Error(`Error fetching cumulative ${dataType} data: ` + error.message);
  }
};

const getCumulativeVolumeData = async (chain) => {
  try {
    if (chain && CHAINS.includes(chain)) {
      const data = await fetchCumulativeData(chain, 'cumulative_volume');
      return { [chain]: data };
    }

    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const data = await fetchCumulativeData(chain, 'cumulative_volume');
        return { [chain]: data };
      })
    );

    return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
  } catch (error) {
    throw new Error('Error fetching cumulative volume data: ' + error.message);
  }
};

const getCumulativeExchangeFeesData = async (chain) => {
  try {
    if (chain && CHAINS.includes(chain)) {
      const data = await fetchCumulativeData(chain, 'cumulative_exchange_fees');
      return { [chain]: data };
    }

    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const data = await fetchCumulativeData(chain, 'cumulative_exchange_fees');
        return { [chain]: data };
      })
    );

    return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
  } catch (error) {
    throw new Error('Error fetching cumulative exchange fees data: ' + error.message);
  }
};

const getCumulativeCollectedFeesData = async (chain) => {
  try {
    if (chain && CHAINS.includes(chain)) {
      const data = await fetchCumulativeData(chain, 'cumulative_collected_fees');
      return { [chain]: data };
    }

    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const data = await fetchCumulativeData(chain, 'cumulative_collected_fees');
        return { [chain]: data };
      })
    );

    return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
  } catch (error) {
    throw new Error('Error fetching cumulative collected fees data: ' + error.message);
  }
};

module.exports = {
  getLatestPerpStatsData,
  getAllPerpStatsData,
  fetchAndInsertAllPerpStatsData,
  fetchAndUpdateLatestPerpStatsData,
  getCumulativeVolumeSummaryStats,
  getCumulativeExchangeFeesSummaryStats,
  getCumulativeCollectedFeesSummaryStats,
  getCumulativeVolumeData,
  getCumulativeExchangeFeesData,
  getCumulativeCollectedFeesData,
};
