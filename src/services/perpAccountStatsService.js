const { knex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');

const {
  calculateDelta,
  calculatePercentage,
  smoothData
} = require('../helpers');

const CACHE_TTL = 3600; // 1 hour

const getCumulativeUniqueTraders = async (chain) => {
  try {
    const fetchCumulativeData = async (chainToFetch) => {
      const cacheKey = `cumulativeUniqueTraders:${chainToFetch}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        result = await knex.raw(`
          WITH first_trade_day AS (
            SELECT
                account_id,
                MIN(date_trunc('day', ts)) AS first_day
            FROM
                perp_account_stats
            WHERE
                chain = ?
            GROUP BY
                account_id
          ),
          cumulative_trader_counts AS (
              SELECT
                  first_day AS ts,
                  COUNT(*) OVER (ORDER BY first_day) AS cumulative_trader_count
              FROM
                  first_trade_day
          )
          SELECT
              ts,
              cumulative_trader_count
          FROM
              cumulative_trader_counts
          ORDER BY
              ts;
        `, [chainToFetch]);

        result = result.rows.map(row => ({
          ts: row.ts,
          cumulative_trader_count: row.cumulative_trader_count,
        }));

        await redisService.set(cacheKey, result, CACHE_TTL);
      }

      return { [chainToFetch]: result };
    };

    if (chain) {
      return await fetchCumulativeData(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchCumulativeData));
      return CHAINS.reduce((acc, chain, index) => {
        acc[chain] = results[index][chain] || [];
        return acc;
      }, {});
    }
  } catch (error) {
    throw new Error('Error fetching cumulative unique trader data: ' + error.message);
  }
};

const getUniqueTradersSummaryStats = async (chain) => {
  try {
    const processChainData = async (chainToProcess) => {
      const cacheKey = `uniqueTradersSummary:${chainToProcess}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('Processing unique traders summary');
        const cumulativeData = await getCumulativeUniqueTraders(chainToProcess);
        const allData = cumulativeData[chainToProcess] || [];

        if (allData.length === 0) {
          result = {};
        } else {
          const smoothedData = smoothData(allData, 'cumulative_trader_count');
          const latestData = smoothedData[smoothedData.length - 1];
          const latestTs = new Date(latestData.ts);
          
          const findValueAtDate = (days) => {
            const targetDate = new Date(latestTs.getTime() - days * 24 * 60 * 60 * 1000);
            return smoothedData.findLast(item => new Date(item.ts) <= targetDate);
          };
          
          const value24h = findValueAtDate(1);
          const value7d = findValueAtDate(7);
          const value28d = findValueAtDate(28);
          const valueYtd = smoothedData.find(item => new Date(item.ts) >= new Date(latestTs.getFullYear(), 0, 1)) || smoothedData[0];
          
          const current = parseFloat(latestData.cumulative_trader_count);
          const traderValues = smoothedData.map(item => parseFloat(item.cumulative_trader_count));
          
          result = {
            current,
            delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.cumulative_trader_count) : null),
            delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.cumulative_trader_count) : null),
            delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.cumulative_trader_count) : null),
            delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.cumulative_trader_count) : null),
            ath: Math.max(...traderValues),
            atl: Math.min(...traderValues),
          };
          
          result.ath_percentage = calculatePercentage(current, result.ath);
          result.atl_percentage = result.atl === 0 ? 100 : calculatePercentage(current, result.atl);
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
      return Object.fromEntries(CHAINS.map((chain, index) => [chain, results[index] || {}]));
    }
  } catch (error) {
    throw new Error(`Error fetching unique traders summary stats: ${error.message}`);
  }
};

const getDailyNewUniqueTraders = async (chain) => {
  try {
    const fetchDailyData = async (chainToFetch) => {
      const cacheKey = `dailyNewUniqueTraders:${chainToFetch}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        result = await knex.raw(`
          WITH first_trading_day AS (
            SELECT
              account_id,
              MIN(DATE_TRUNC('day', ts)) AS first_day
            FROM
              perp_account_stats
            WHERE
              chain = ?
            GROUP BY
              account_id
          )
          SELECT
            first_day AS date,
            COUNT(*) AS daily_new_unique_traders
          FROM
            first_trading_day
          GROUP BY
            first_day
          ORDER BY
            first_day;
        `, [chainToFetch]);

        result = result.rows.map(row => ({
          ts: row.date,
          daily_new_unique_traders: parseInt(row.daily_new_unique_traders),
        }));

        await redisService.set(cacheKey, result, CACHE_TTL);
      }

      return { [chainToFetch]: result };
    };

    if (chain) {
      return await fetchDailyData(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchDailyData));
      return results.reduce((acc, result) => ({ ...acc, ...result }), {});
    }
  } catch (error) {
    throw new Error('Error fetching daily new unique traders: ' + error.message);
  }
};

module.exports = {
  getCumulativeUniqueTraders,
  getUniqueTradersSummaryStats,
  getDailyNewUniqueTraders,
};