const { troyDBKnex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');

const {
  calculateDelta,
  calculatePercentage,
  smoothData
} = require('../helpers');

const CACHE_TTL = 3600; // 1 hour

const getOpenInterestData = async (chain, bypassCache = false, trx = troyDBKnex) => {
  const fetchDataForChain = async (chainToFetch) => {
    const cacheKey = `openInterestData:${chainToFetch}`;
    let result = bypassCache ? null : await redisService.get(cacheKey);

    if (!result) {
      console.log('not from cache');
      const tableName = `prod_${chainToFetch}_mainnet.fct_perp_market_history_${chainToFetch}_mainnet`;
      try {
        const queryResult = await trx.raw(`
          WITH daily_market_oi AS (
            SELECT
              DATE_TRUNC('day', ts) AS day,
              market_symbol,
              AVG(size_usd) AS daily_market_oi
            FROM
              ${tableName}
            GROUP BY
              DATE_TRUNC('day', ts),
              market_symbol
          ),
          daily_oi AS (
            SELECT
              day,
              SUM(daily_market_oi) AS daily_oi
            FROM
              daily_market_oi
            GROUP BY
              day
          )
          SELECT
            day AS ts,
            daily_oi
          FROM
            daily_oi
          ORDER BY
            ts ASC;
        `);

        result = queryResult.rows.map(row => ({
          ts: row.ts,
          daily_oi: parseFloat(row.daily_oi),
        }));

        await redisService.set(cacheKey, result, CACHE_TTL);
      } catch (error) {
        console.error(`Error fetching data for ${chainToFetch}:`, error.message);
        result = [];
      }
    }

    return { [chainToFetch]: result };
  };

  try {
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
    console.error('Error in getOpenInterestData:', error);
    return {};
  }
};

const getDailyOpenInterestChangeData = async (chain, bypassCache = false, trx = troyDBKnex) => {
  const fetchDataForChain = async (chainToFetch) => {
    const cacheKey = `dailyOpenInterestChangeData:${chainToFetch}`;
    let result = bypassCache ? null : await redisService.get(cacheKey);

    if (!result) {
      console.log('not from cache');
      const tableName = `prod_${chainToFetch}_mainnet.fct_perp_market_history_${chainToFetch}_mainnet`;
      try {
        const queryResult = await trx.raw(`
          WITH daily_market_oi AS (
            SELECT
              DATE_TRUNC('day', ts) AS day,
              market_symbol,
              AVG(size_usd) AS daily_market_oi
            FROM
              ${tableName}
            GROUP BY
              DATE_TRUNC('day', ts),
              market_symbol
          ),
          daily_oi AS (
            SELECT
              day,
              SUM(daily_market_oi) AS daily_oi
            FROM
              daily_market_oi
            GROUP BY
              day
          ),
          daily_oi_change AS (
            SELECT
              day AS ts,
              daily_oi,
              daily_oi - LAG(daily_oi) OVER (ORDER BY day) AS daily_oi_change
            FROM
              daily_oi
          )
          SELECT
            ts,
            daily_oi,
            daily_oi_change
          FROM
            daily_oi_change
          WHERE
            daily_oi_change IS NOT NULL
          ORDER BY
            ts ASC;
        `);

        result = queryResult.rows.map(row => ({
          ts: row.ts,
          daily_oi_change: parseFloat(row.daily_oi_change)
        }));

        await redisService.set(cacheKey, result, CACHE_TTL);
      } catch (error) {
        console.error(`Error fetching data for ${chainToFetch}:`, error.message);
        result = [];
      }
    }

    return { [chainToFetch]: result };
  };

  try {
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
    console.error('Error in getDailyOpenInterestChangeData:', error);
    return {};
  }
};

const getOpenInterestSummaryStats = async (chain, bypassCache = false, trx = troyDBKnex) => {
  try {
    const processChainData = async (chainToProcess) => {
      const cacheKey = `openInterestSummaryStats:${chainToProcess}`;
      let result = bypassCache ? null : await redisService.get(cacheKey);

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
    console.error('Error in getOpenInterestSummaryStats:', error);
    return {};
  }
};

const refreshAllPerpMarketHistoryData = async () => {
  console.log('Starting to refresh Perp Market History data for all chains');
  
  for (const chain of CHAINS) {
    console.log(`Refreshing Perp Market History data for chain: ${chain}`);
    console.time(`${chain} total refresh time`);
    
    // Clear existing cache
    await redisService.del(`openInterestData:${chain}`);
    await redisService.del(`dailyOpenInterestChangeData:${chain}`);
    await redisService.del(`openInterestSummaryStats:${chain}`);

    // Use a separate transaction for each chain
    await troyDBKnex.transaction(async (trx) => {
      try {
        // Fetch new data
        console.time(`${chain} getOpenInterestData`);
        await getOpenInterestData(chain, true, trx);
        console.timeEnd(`${chain} getOpenInterestData`);

        console.time(`${chain} getDailyOpenInterestChangeData`);
        await getDailyOpenInterestChangeData(chain, true, trx);
        console.timeEnd(`${chain} getDailyOpenInterestChangeData`);

        console.time(`${chain} getOpenInterestSummaryStats`);
        await getOpenInterestSummaryStats(chain, true, trx);
        console.timeEnd(`${chain} getOpenInterestSummaryStats`);

      } catch (error) {
        console.error(`Error refreshing Perp Market History data for chain ${chain}:`, error);
        // Don't throw the error, just log it and continue with the next chain
      }
    });

    console.timeEnd(`${chain} total refresh time`);
  }

  console.log('Finished refreshing Perp Market History data for all chains');
};

module.exports = {
  getOpenInterestData,
  getDailyOpenInterestChangeData,
  getOpenInterestSummaryStats,
  refreshAllPerpMarketHistoryData
};