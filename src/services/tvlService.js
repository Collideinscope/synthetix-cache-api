const { troyDBKnex } = require('../config/db');
const redisService = require('./redisService');
const { CHAINS } = require('../helpers');

const {
  calculateDelta,
  calculatePercentage,
  smoothData
} = require('../helpers');

const CACHE_TTL = 3600; // 1 hour

const getLatestTVLData = async (chain, collateralType, bypassCache = false, trx = troyDBKnex) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchLatest = async (chainToFetch) => {
      const cacheKey = `latestTVL:${chainToFetch}:${collateralType}`;
      let result = bypassCache ? null : await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        const tableName = `prod_${chainToFetch}_mainnet.fct_core_vault_collateral_${chainToFetch}_mainnet`;
        try {
          result = await trx(tableName)
            .where('collateral_type', collateralType)
            .orderBy('ts', 'desc')
            .limit(1);
        } catch (error) {
          console.error(`Error fetching data for ${chainToFetch}:`, error);
          result = [];
        }
        await redisService.set(cacheKey, result, CACHE_TTL);
      }

      return { [chainToFetch]: result };
    };

    if (chain) {
      return await fetchLatest(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchLatest));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching latest TVL data: ' + error.message);
  }
};

const getCumulativeTVLData = async (chain, collateralType, bypassCache = false, trx = troyDBKnex) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchAll = async (chainToFetch) => {
      const cacheKey = `cumulativeTVL:${chainToFetch}:${collateralType}`;
      let result = bypassCache ? null : await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        const tableName = `prod_${chainToFetch}_mainnet.fct_core_vault_collateral_${chainToFetch}_mainnet`;
        const startDate = new Date('2024-03-26');

        try {
          result = await trx(tableName)
            .where('ts', '>=', startDate)
            .where({
              pool_id: 1,
              collateral_type: collateralType
            })
            .orderBy('ts', 'asc');
        } catch (error) {
          console.error(`Error fetching data for ${chainToFetch}:`, error);
          result = [];
        }
        await redisService.set(cacheKey, result, CACHE_TTL);
      }

      return { [chainToFetch]: result };
    };

    if (chain) {
      return await fetchAll(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchAll));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching cumulative TVL data: ' + error.message);
  }
};

const getTVLSummaryStats = async (chain, collateralType, bypassCache = false, trx = troyDBKnex) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const processChainData = async (chainToProcess) => {
      const cacheKey = `TVLSummary:${chainToProcess}:${collateralType}`;
      let result = bypassCache ? null : await redisService.get(cacheKey);

      if (!result) {
        console.log('Processing TVL summary');
        const allData = await getCumulativeTVLData(chainToProcess, collateralType);
        const chainData = allData[chainToProcess];
        
        if (chainData.length === 0) {
          result = {};
        } else {
          const smoothedData = smoothData(chainData, 'collateral_value');
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
          
          const current = parseFloat(latestData.collateral_value);
          const tvlValues = smoothedData.map(item => parseFloat(item.collateral_value));
          
          result = {
            current,
            delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.collateral_value) : null),
            delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.collateral_value) : null),
            delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.collateral_value) : null),
            delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.collateral_value) : null),
            ath: Math.max(...tvlValues),
            atl: Math.min(...tvlValues),
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
    throw new Error(`Error fetching TVL summary stats: ${error.message}`);
  }
};

const getDailyTVLData = async (chain, collateralType, bypassCache = false, trx = troyDBKnex) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchDaily = async (chainToProcess) => {
      const cacheKey = `dailyTVL:${chainToProcess}:${collateralType}`;
      let result = bypassCache ? null : await redisService.get(cacheKey);

      if (!result) {
        console.log('not from cache');
        const tableName = `prod_${chainToProcess}_mainnet.fct_core_vault_collateral_${chainToProcess}_mainnet`;
        try {
          const queryResult = await trx.raw(`
            WITH daily_data AS (
              SELECT
                DATE_TRUNC('day', ts) AS date,
                FIRST_VALUE(SUM(collateral_value)) OVER (PARTITION BY DATE_TRUNC('day', ts) ORDER BY ts ASC) AS start_of_day_tvl,
                LAST_VALUE(SUM(collateral_value)) OVER (PARTITION BY DATE_TRUNC('day', ts) ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS end_of_day_tvl
              FROM ${tableName}
              WHERE pool_id = 1
                AND collateral_type = ?
              GROUP BY DATE_TRUNC('day', ts), ts
            )
            SELECT DISTINCT
              date,
              end_of_day_tvl - start_of_day_tvl AS daily_tvl_change
            FROM daily_data
            ORDER BY date;
          `, [collateralType]);

          result = queryResult.rows.map(row => ({
            ts: row.date,
            daily_tvl_change: parseFloat(row.daily_tvl_change)
          }));
        } catch (error) {
          console.error(`Error fetching data for ${chainToProcess}:`, error);
          result = [];
        }
        await redisService.set(cacheKey, result, CACHE_TTL);
      }

      return { [chainToProcess]: result };
    };

    if (chain) {
      return await fetchDaily(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchDaily));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching daily TVL data: ' + error.message);
  }
};

const refreshAllTVLData = async (collateralType) => {
  console.log('Starting to refresh TVL data for all chains');
  
  for (const chain of CHAINS) {
    console.log(`Refreshing TVL data for chain: ${chain}`);
    console.time(`${chain} total refresh time`);
    
    // Clear existing cache
    await redisService.del(`latestTVL:${chain}:${collateralType}`);
    await redisService.del(`cumulativeTVL:${chain}:${collateralType}`);
    await redisService.del(`TVLSummary:${chain}:${collateralType}`);
    await redisService.del(`dailyTVL:${chain}:${collateralType}`);

    // Use a separate transaction for each chain
    await troyDBKnex.transaction(async (trx) => {
      try {
        // Fetch new data
        console.time(`${chain} getLatestTVLData`);
        await getLatestTVLData(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getLatestTVLData`);

        console.time(`${chain} getCumulativeTVLData`);
        await getCumulativeTVLData(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getCumulativeTVLData`);

        console.time(`${chain} getTVLSummaryStats`);
        await getTVLSummaryStats(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getTVLSummaryStats`);

        console.time(`${chain} getDailyTVLData`);
        await getDailyTVLData(chain, collateralType, true, trx);
        console.timeEnd(`${chain} getDailyTVLData`);

      } catch (error) {
        console.error(`Error refreshing TVL data for chain ${chain}:`, error);
        // Don't throw the error, just log it and continue with the next chain
      }
    });

    console.timeEnd(`${chain} total refresh time`);
  }

  console.log('Finished refreshing TVL data for all chains');
};

module.exports = {
  getLatestTVLData,
  getCumulativeTVLData,
  getTVLSummaryStats,
  getDailyTVLData,
  refreshAllTVLData
};