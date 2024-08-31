const { knex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');

const {
  calculateDelta,
  calculatePercentage,
  smoothData
} = require('../helpers');

const CACHE_TTL = 3600; // 1 hour

const getLatestPerpStatsData = async (chain) => {
  try {
    const fetchLatest = async (chainToFetch) => {
      const cacheKey = `latestPerpStats:${chainToFetch}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        result = await knex('perp_stats')
          .where('chain', chainToFetch)
          .orderBy('ts', 'desc')
          .limit(1);
        await redisService.set(cacheKey, result, CACHE_TTL);
      }

      return { [chainToFetch]: result };
    };

    if (chain) {
      return await fetchLatest(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchLatest));
      return CHAINS.reduce((acc, chain, index) => {
        acc[chain] = results[index][chain] || [];
        return acc;
      }, {});
    }
  } catch (error) {
    throw new Error('Error fetching latest perp stats data: ' + error.message);
  }
};

const getSummaryStats = async (chain, column) => {
  try {
    const processChainData = async (chainToProcess) => {
      const cacheKey = `perpStatsSummary:${chainToProcess}:${column}`;
      let result = null //await redisService.get(cacheKey);

      if (!result) {
        console.log('Processing perp stats summary');
        
        const data = await fetchCumulativeData(chainToProcess, column); 

        if (data.length === 0) {
          result = {};
        } else {
          const smoothedData = smoothData(data, column);
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
          const current = parseFloat(data[data.length - 1][column]);
          const ath = Math.max(...columnValues, current);
          const atl = Math.min(...columnValues, current);

          result = {
            current,
            delta_24h: calculateDelta(current, value24h ? parseFloat(value24h[column]) : null),
            delta_7d: calculateDelta(current, value7d ? parseFloat(value7d[column]) : null),
            delta_28d: calculateDelta(current, value28d ? parseFloat(value28d[column]) : null),
            delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd[column]) : null),
            ath,
            atl,
            ath_percentage: calculatePercentage(current, ath),
            atl_percentage: atl === 0 ? 100 : calculatePercentage(current, atl),
          };
        }

        await redisService.set(cacheKey, result, CACHE_TTL);
      }

      return result;
    };

    if (chain) {
      const result = await processChainData(chain);
      return { [chain]: result };
    } else {
      const results = await Promise.all(CHAINS.map(processChainData));
      return CHAINS.reduce((acc, chain, index) => {
        acc[chain] = results[index] || {};
        return acc;
      }, {});
    }
  } catch (error) {
    throw new Error(`Error fetching perp stats summary stats for ${column}: ` + error.message);
  }
};

const getCumulativeVolumeSummaryStats = async (chain) => {
  return getSummaryStats(chain, 'cumulative_volume');
};

const getCumulativeExchangeFeesSummaryStats = async (chain) => {
  return getSummaryStats(chain, 'cumulative_exchange_fees');
};

const fetchCumulativeData = async (chain, dataType) => {
  const cacheKey = `cumulativePerpStats:${chain}:${dataType}`;
  let result = await redisService.get(cacheKey);

  if (!result) {
    console.log('not from cache');
    result = await knex.raw(`
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

    result = result.rows.map(row => ({
      ts: row.ts,
      [dataType]: parseFloat(row[dataType]),
    }));

    await redisService.set(cacheKey, result, CACHE_TTL);
  }

  return result;
};

const getCumulativeVolumeData = async (chain) => {
  try {
    if (chain) {
      const data = await fetchCumulativeData(chain, 'cumulative_volume');
      return { [chain]: data };
    } else {
      const results = await Promise.all(CHAINS.map(async (chain) => {
        const data = await fetchCumulativeData(chain, 'cumulative_volume');
        return { [chain]: data };
      }));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching cumulative volume data: ' + error.message);
  }
};

const getCumulativeExchangeFeesData = async (chain) => {
  try {
    if (chain) {
      const data = await fetchCumulativeData(chain, 'cumulative_exchange_fees');
      return { [chain]: data };
    } else {
      const results = await Promise.all(CHAINS.map(async (chain) => {
        const data = await fetchCumulativeData(chain, 'cumulative_exchange_fees');
        return { [chain]: data };
      }));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching cumulative exchange fees data: ' + error.message);
  }
};

const fetchDailyData = async (chain, dataType) => {
  const cacheKey = `dailyPerpStats:${chain}:${dataType}`;
  let result = await redisService.get(cacheKey);

  if (!result) {
    console.log('not from cache');
    result = await knex.raw(`
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

    result = result.rows.map(row => ({
      ts: row.date,
      [`daily_${dataType}`]: parseFloat(row[`daily_${dataType}`]),
    }));

    await redisService.set(cacheKey, result, CACHE_TTL);
  }

  return result;
};

const getDailyVolumeData = async (chain) => {
  try {
    if (chain) {
      const data = await fetchDailyData(chain, 'cumulative_volume');
      return { [chain]: data };
    } else {
      const results = await Promise.all(CHAINS.map(async (chain) => {
        const data = await fetchDailyData(chain, 'cumulative_volume');
        return { [chain]: data };
      }));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching daily volume data: ' + error.message);
  }
};

const getDailyExchangeFeesData = async (chain) => {
  try {
    if (chain) {
      const data = await fetchDailyData(chain, 'cumulative_exchange_fees');
      return { [chain]: data };
    } else {
      const results = await Promise.all(CHAINS.map(async (chain) => {
        const data = await fetchDailyData(chain, 'cumulative_exchange_fees');
        return { [chain]: data };
      }));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching daily exchange fees data: ' + error.message);
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
};