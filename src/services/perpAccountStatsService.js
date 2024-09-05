const { troyDBKnex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');

const {
  calculateDelta,
  calculatePercentage,
  smoothData
} = require('../helpers');

const CACHE_TTL = 3600; // 1 hour

const getCumulativeUniqueTraders = async (chain, bypassCache = false, trx = troyDBKnex) => {
  const fetchCumulativeData = async (chainToFetch) => {
    const cacheKey = `cumulativeUniqueTraders:${chainToFetch}`;
    let result = bypassCache ? null : await redisService.get(cacheKey);

    if (!result) {
      console.log('not from cache');
      const tableName = `prod_${chainToFetch}_mainnet.fct_perp_account_stats_daily_${chainToFetch}_mainnet`;
      try {
        const queryResult = await trx.raw(`
          WITH daily_unique_traders AS (
            SELECT
              ts,
              COUNT(DISTINCT account_id) AS unique_traders
            FROM
              ${tableName}
            GROUP BY
              ts
          ),
          cumulative_counts AS (
            SELECT
              ts,
              SUM(unique_traders) OVER (ORDER BY ts) AS cumulative_trader_count
            FROM
              daily_unique_traders
          )
          SELECT
            ts,
            cumulative_trader_count
          FROM
            cumulative_counts
          ORDER BY
            ts;
        `);

        result = queryResult.rows.map(row => ({
          ts: row.ts,
          cumulative_trader_count: parseInt(row.cumulative_trader_count),
        }));

        await redisService.set(cacheKey, result, CACHE_TTL);
      } catch (error) {
        console.error(`Error fetching data for ${chainToFetch}:`, error.message);
        result = []; // Return an empty array if there's an error
      }
    }

    return { [chainToFetch]: result };
  };

  try {
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
    console.error('Error in getCumulativeUniqueTraders:', error);
    return {}; // Return an empty object if there's an error at the top level
  }
};

const getUniqueTradersSummaryStats = async (chain, bypassCache = false, trx = troyDBKnex) => {
  try {
    const processChainData = async (chainToProcess) => {
      const cacheKey = `uniqueTradersSummary:${chainToProcess}`;
      let result = bypassCache ? null : await redisService.get(cacheKey);

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

const getDailyNewUniqueTraders = async (chain, bypassCache = false, trx = troyDBKnex) => {
  const fetchDailyData = async (chainToFetch) => {
    const cacheKey = `dailyNewUniqueTraders:${chainToFetch}`;
    let result = bypassCache ? null : await redisService.get(cacheKey);

    if (!result) {
      console.log('not from cache');
      const tableName = `prod_${chainToFetch}_mainnet.fct_perp_account_stats_daily_${chainToFetch}_mainnet`;
      try {
        const queryResult = await trx.raw(`
          SELECT
            ts,
            COUNT(DISTINCT account_id) AS daily_unique_traders
          FROM
            ${tableName}
          GROUP BY
            ts
          ORDER BY
            ts;
        `);

        result = queryResult.rows.map(row => ({
          ts: row.ts,
          daily_unique_traders: parseInt(row.daily_unique_traders),
        }));

        await redisService.set(cacheKey, result, CACHE_TTL);
      } catch (error) {
        console.error(`Error fetching data for ${chainToFetch}:`, error.message);
        result = []; // Return an empty array if there's an error
      }
    }

    return { [chainToFetch]: result };
  };

  try {
    if (chain) {
      return await fetchDailyData(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchDailyData));
      return CHAINS.reduce((acc, chain, index) => {
        acc[chain] = results[index][chain] || [];
        return acc;
      }, {});
    }
  } catch (error) {
    console.error('Error in getDailyNewUniqueTraders:', error);
    return {}; // Return an empty object if there's an error at the top level
  }
};

const refreshAllPerpAccountStatsData = async () => {
  console.log('Starting to refresh Perp Account Stats data for all chains');
  
  for (const chain of CHAINS) {
    console.log(`Refreshing Perp Account Stats data for chain: ${chain}`);
    console.time(`${chain} total refresh time`);

    // Clear existing cache
    await redisService.del(`cumulativeUniqueTraders:${chain}`);
    await redisService.del(`uniqueTradersSummary:${chain}`);
    await redisService.del(`dailyNewUniqueTraders:${chain}`);

    // Use a separate transaction for each chain
    await troyDBKnex.transaction(async (trx) => {
      try {
        // Fetch new data
        console.time(`${chain} getCumulativeUniqueTraders`);
        await getCumulativeUniqueTraders(chain, true, trx);
        console.timeEnd(`${chain} getCumulativeUniqueTraders`);

        console.time(`${chain} getUniqueTradersSummaryStats`);
        await getUniqueTradersSummaryStats(chain, true, trx);
        console.timeEnd(`${chain} getUniqueTradersSummaryStats`);

        console.time(`${chain} getDailyNewUniqueTraders`);
        await getDailyNewUniqueTraders(chain, true, trx);
        console.timeEnd(`${chain} getDailyNewUniqueTraders`);

      } catch (error) {
        console.error(`Error refreshing Perp Account Stats data for chain ${chain}:`, error);
        // Don't throw the error, just log it and continue with the next chain
      }
    });

    console.timeEnd(`${chain} total refresh time`);
  }

  console.log('Finished refreshing Perp Account Stats data for all chains');
};

module.exports = {
  getCumulativeUniqueTraders,
  getUniqueTradersSummaryStats,
  getDailyNewUniqueTraders,
  refreshAllPerpAccountStatsData
};