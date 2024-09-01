const { knex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');

const {
  calculateDelta,
  calculatePercentage,
  smoothData
} = require('../helpers');

const CACHE_TTL = 3600; // 1 hour

const getOpenInterestData = async (chain) => {
  try {
    const fetchDataForChain = async (chainToFetch) => {
      const cacheKey = `openInterestData:${chainToFetch}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        result = await knex.raw(`
          WITH daily_market_oi AS (
            SELECT
              date_trunc('day', ts) AS day,
              market_symbol,
              AVG(size * price) AS daily_market_oi,
              chain
            FROM
              perp_market_history
            WHERE
              chain = ?
            GROUP BY
              date_trunc('day', ts),
              market_symbol,
              chain
          ),
          daily_oi AS (
            SELECT
              day,
              SUM(daily_market_oi) AS daily_oi,
              chain
            FROM
              daily_market_oi
            GROUP BY
              day,
              chain
          )
          SELECT
            day AS ts,
            daily_oi,
            chain
          FROM
            daily_oi
          ORDER BY
            ts ASC;
        `, [chainToFetch]);

        result = result.rows.map(row => ({
          ts: row.ts,
          daily_oi: parseFloat(row.daily_oi),
        }));

        await redisService.set(cacheKey, result, CACHE_TTL);
      }

      return { [chainToFetch]: result };
    };

    if (chain) {
      return await fetchDataForChain(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchDataForChain));
      return CHAINS.reduce((acc, chain, index) => {
        acc[chain] = results[index][chain] || [];
        return acc;
      }, {});
    }
  } catch (error) {
    throw new Error('Error fetching daily OI data: ' + error.message);
  }
};

const getDailyOpenInterestChangeData = async (chain) => {
  try {
    const fetchDataForChain = async (chainToFetch) => {
      const cacheKey = `dailyOpenInterestChangeData:${chainToFetch}`;
      let result = await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        result = await knex.raw(`
          WITH daily_market_oi AS (
            SELECT
              date_trunc('day', ts) AS day,
              market_symbol,
              AVG(size * price) AS daily_market_oi,
              chain
            FROM
              perp_market_history
            WHERE
              chain = ?
            GROUP BY
              date_trunc('day', ts),
              market_symbol,
              chain
          ),
          daily_oi AS (
            SELECT
              day,
              SUM(daily_market_oi) AS daily_oi,
              chain
            FROM
              daily_market_oi
            GROUP BY
              day,
              chain
          ),
          daily_oi_change AS (
            SELECT
              day AS ts,
              daily_oi,
              daily_oi - LAG(daily_oi) OVER (ORDER BY day) AS daily_oi_change,
              chain
            FROM
              daily_oi
          )
          SELECT
            ts,
            daily_oi,
            daily_oi_change,
            chain
          FROM
            daily_oi_change
          WHERE
            daily_oi_change IS NOT NULL
          ORDER BY
            ts ASC;
        `, [chainToFetch]);

        result = result.rows.map(row => ({
          ts: row.ts,
          daily_oi_change: parseFloat(row.daily_oi_change)
        }));

        await redisService.set(cacheKey, result, CACHE_TTL);
      }

      return { [chainToFetch]: result };
    };

    if (chain) {
      return await fetchDataForChain(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchDataForChain));
      return results.reduce((acc, obj, index) => {
        const chain = Object.keys(obj)[0];
        acc[chain] = obj[chain] || [];
        return acc;
      }, {});
    }
  } catch (error) {
    throw new Error('Error fetching daily open interest change data: ' + error.message);
  }
};

const getOpenInterestSummaryStats = async (chain) => {
  try {
    const processChainData = async (chainToProcess) => {
      const cacheKey = `openInterestSummaryStats:${chainToProcess}`;
      let result = null//await redisService.get(cacheKey);

      if (!result) {
        console.log('Processing open interest summary stats');
        const data = await getOpenInterestData(chainToProcess);
        const chainData = data[chainToProcess];
        
        if (chainData.length === 0) {
          result = {};
        } else {
          const smoothedData = smoothData(chainData, 'daily_oi');
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
          
          const current = parseFloat(latestData.daily_oi);
          const oiValues = smoothedData.map(item => parseFloat(item.daily_oi));
          
          result = {
            current,
            delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.daily_oi) : null),
            delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.daily_oi) : null),
            delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.daily_oi) : null),
            delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.daily_oi) : null),
            ath: Math.max(...oiValues),
            atl: Math.min(...oiValues),
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
    throw new Error(`Error fetching open interest summary stats: ${error.message}`);
  }
};

module.exports = {
  getOpenInterestData,
  getDailyOpenInterestChangeData,
  getOpenInterestSummaryStats,
};