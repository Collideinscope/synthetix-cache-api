const { knex, troyDBKnex } = require('../config/db');
const { CHAINS } = require('../helpers');
const {
  calculateDelta,
  calculatePercentage,
  calculateStandardDeviation,
  smoothData
} = require('../helpers');

const getLatestPerpStatsData = async (chain) => {
  try {
    if (chain && !CHAINS.includes(chain)) {
      throw new Error('Invalid chain parameter');
    }

    const fetchLatest = async (chainToFetch) => {
      const result = await knex('perp_stats')
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
    throw new Error('Error fetching latest perp stats data: ' + error.message);
  }
};

const getSummaryStats = async (chain, column) => {
  try {
    if (chain && !CHAINS.includes(chain)) {
      throw new Error('Invalid chain parameter');
    }

    const processChainData = async (chainToProcess) => {
      const baseQuery = () => knex('perp_stats').where('chain', chainToProcess);

      const allData = await baseQuery().orderBy('ts', 'desc');
      
      if (allData.length === 0) {
        return null; // No data for this chain
      }

      const latestData = allData[0];
      const latestTs = new Date(latestData.ts);

      const getClosestDataPoint = (targetDate) => {
        return allData.reduce((closest, current) => {
          const currentDate = new Date(current.ts);
          const closestDate = new Date(closest.ts);
          return Math.abs(currentDate - targetDate) < Math.abs(closestDate - targetDate) ? current : closest;
        });
      };

      const value24h = getClosestDataPoint(new Date(latestTs.getTime() - 24 * 60 * 60 * 1000));
      const value7d = getClosestDataPoint(new Date(latestTs.getTime() - 7 * 24 * 60 * 60 * 1000));
      const value28d = getClosestDataPoint(new Date(latestTs.getTime() - 28 * 24 * 60 * 60 * 1000));
      const valueYtd = getClosestDataPoint(new Date(latestTs.getFullYear(), 0, 1));

      const columnValues = allData.map(item => parseFloat(item[column]));
      const standardDeviation = calculateStandardDeviation(columnValues);
      const current = parseFloat(latestData[column]);
      const ath = Math.max(...columnValues);
      const atl = Math.min(...columnValues);

      return {
        current,
        delta_24h: calculateDelta(current, parseFloat(value24h[column])),
        delta_7d: calculateDelta(current, parseFloat(value7d[column])),
        delta_28d: calculateDelta(current, parseFloat(value28d[column])),
        delta_ytd: calculateDelta(current, parseFloat(valueYtd[column])),
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
    throw new Error('Error fetching perp stats summary stats: ' + error.message);
  }
};

const getCumulativeVolumeSummaryStats = async (chain) => {
  return getSummaryStats(chain, 'cumulative_volume');
};

const getCumulativeExchangeFeesSummaryStats = async (chain) => {
  return getSummaryStats(chain, 'cumulative_exchange_fees');
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
    if (chain && !CHAINS.includes(chain)) {
      throw new Error('Invalid chain parameter');
    }

    if (chain) {
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
    if (chain && !CHAINS.includes(chain)) {
      throw new Error('Invalid chain parameter');
    }

    if (chain) {
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

const fetchDailyData = async (chain, dataType) => {
  const result = await knex.raw(`
    WITH daily_data AS (
      SELECT 
        DATE_TRUNC('day', ts) AS date,
        ${dataType},
        LAG(${dataType}) OVER (ORDER BY ts) AS prev_${dataType}
      FROM 
        perp_stats
      WHERE
        chain = ?
      ORDER BY 
        ts
    )
    SELECT 
      date,
      COALESCE(${dataType} - prev_${dataType}, ${dataType}) AS daily_${dataType}
    FROM 
      daily_data
    WHERE
      prev_${dataType} IS NOT NULL OR date = (SELECT MIN(date) FROM daily_data)
    ORDER BY 
      date;
  `, [chain]);

  return result.rows.map(row => ({
    ts: row.date,
    [`daily_${dataType}`]: row[`daily_${dataType}`],
  }));
};

const getDailyVolumeData = async (chain) => {
  try {
    if (chain && !CHAINS.includes(chain)) {
      throw new Error('Invalid chain parameter');
    }

    if (chain) {
      const data = await fetchDailyData(chain, 'cumulative_volume');
      return { [chain]: data };
    }

    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const data = await fetchDailyData(chain, 'cumulative_volume');
        return { [chain]: data };
      })
    );

    return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
  } catch (error) {
    throw new Error('Error fetching daily volume data: ' + error.message);
  }
};

const getDailyExchangeFeesData = async (chain) => {
  try {
    if (chain && !CHAINS.includes(chain)) {
      throw new Error('Invalid chain parameter');
    }

    if (chain) {
      const data = await fetchDailyData(chain, 'cumulative_exchange_fees');
      return { [chain]: data };
    }

    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const data = await fetchDailyData(chain, 'cumulative_exchange_fees');
        return { [chain]: data };
      })
    );

    return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
  } catch (error) {
    throw new Error('Error fetching daily exchange fees data: ' + error.message);
  }
};

const fetchAndInsertAllPerpStatsData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
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
  getCumulativeVolumeSummaryStats,
  getCumulativeExchangeFeesSummaryStats,
  getCumulativeVolumeData,
  getCumulativeExchangeFeesData,
  getDailyVolumeData,
  getDailyExchangeFeesData,
  fetchAndInsertAllPerpStatsData,
  fetchAndUpdateLatestPerpStatsData,
};