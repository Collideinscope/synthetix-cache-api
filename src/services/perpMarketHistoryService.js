const { knex, troyDBKnex } = require('../config/db');
const { CHAINS } = require('../helpers');
const {
  calculateDelta,
  calculatePercentage,
  calculateStandardDeviation,
  smoothData
} = require('../helpers');

const CHUNK_SIZE = 1000; // large queries

const fetchAndInsertAllPerpMarketHistoryData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
  }

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = `prod_${chain}_mainnet.fct_perp_market_history_base_mainnet`;

    const rows = await troyDBKnex.raw(`
      SELECT 
        DISTINCT ON (ts, market_id)
        ts, 
        block_number, 
        market_id, 
        market_symbol, 
        price, 
        size, 
        funding_rate, 
        long_rate_apr, 
        short_rate_apr, 
        size_usd, 
        long_oi, 
        short_oi, 
        long_oi_pct, 
        short_oi_pct
      FROM ${tableName}
      ORDER BY ts DESC, market_id;
    `);

    const dataToInsert = rows.rows.map(row => ({
      ...row,
      chain,
    }));

    const insertInChunks = async (data) => {
      for (let i = 0; i < data.length; i += CHUNK_SIZE) {
        const chunk = data.slice(i, i + CHUNK_SIZE);
        await knex('perp_market_history')
          .insert(chunk)
          .onConflict(['chain', 'ts', 'market_id'])
          .merge({
            price: knex.raw('EXCLUDED.price'),
            size: knex.raw('EXCLUDED.size'),
            funding_rate: knex.raw('EXCLUDED.funding_rate'),
            long_rate_apr: knex.raw('EXCLUDED.long_rate_apr'),
            short_rate_apr: knex.raw('EXCLUDED.short_rate_apr'),
            size_usd: knex.raw('EXCLUDED.size_usd'),
            long_oi: knex.raw('EXCLUDED.long_oi'),
            short_oi: knex.raw('EXCLUDED.short_oi'),
            long_oi_pct: knex.raw('EXCLUDED.long_oi_pct'),
            short_oi_pct: knex.raw('EXCLUDED.short_oi_pct')
          });
      }
    };

    await insertInChunks(dataToInsert);

    console.log(`Perp market history data seeded successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error seeding perp market history data for ${chain} chain:`, error);
  }
};

const fetchAndUpdateLatestPerpMarketHistoryData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
    return;
  }

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = `prod_${chain}_mainnet.fct_perp_market_history_base_mainnet`;

    const lastTimestampResult = await knex('perp_market_history')
      .where('chain', chain)
      .orderBy('ts', 'desc')
      .first();

    const lastTimestamp = lastTimestampResult ? lastTimestampResult.ts : null;

    const newRows = await troyDBKnex.raw(`
      SELECT 
        ts, 
        block_number, 
        market_id, 
        market_symbol, 
        price, 
        size, 
        funding_rate, 
        long_rate_apr, 
        short_rate_apr, 
        size_usd, 
        long_oi, 
        short_oi, 
        long_oi_pct, 
        short_oi_pct
      FROM ${tableName}
      WHERE ts > ?
      ORDER BY ts DESC;
    `, [lastTimestamp]);

    if (newRows.rows.length === 0) {
      console.log(`No new perp market history data to update for ${chain} chain.`);
      return;
    }

    const dataToInsert = newRows.rows.map(row => ({
      ...row,
      chain,
    }));

    if (dataToInsert.length > 0) {
      const insertInChunks = async (data) => {
        for (let i = 0; i < data.length; i += CHUNK_SIZE) {
          const chunk = data.slice(i, i + CHUNK_SIZE);
          await knex('perp_market_history')
            .insert(chunk)
            .onConflict(['chain', 'ts', 'market_id'])
            .merge({
              price: knex.raw('EXCLUDED.price'),
              size: knex.raw('EXCLUDED.size'),
              funding_rate: knex.raw('EXCLUDED.funding_rate'),
              long_rate_apr: knex.raw('EXCLUDED.long_rate_apr'),
              short_rate_apr: knex.raw('EXCLUDED.short_rate_apr'),
              size_usd: knex.raw('EXCLUDED.size_usd'),
              long_oi: knex.raw('EXCLUDED.long_oi'),
              short_oi: knex.raw('EXCLUDED.short_oi'),
              long_oi_pct: knex.raw('EXCLUDED.long_oi_pct'),
              short_oi_pct: knex.raw('EXCLUDED.short_oi_pct')
            });
        }
      };
    }

    console.log(`Perp market history data updated successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error updating perp market history data for ${chain} chain:`, error);
  }
};

const getAllPerpMarketHistoryData = async (chain) => {
  try {
    let query = knex('perp_market_history').orderBy('ts', 'asc');

    if (chain && CHAINS.includes(chain)) {
      query = query.where('chain', chain);
    }

    const result = await query;

    return result;
  } catch (error) {
    throw new Error('Error fetching all perp market history data: ' + error.message);
  }
};

const getOpenInterestData = async (chain) => {
  try {
    // Calculate daily average OI per market and then sum them up to get daily OI
    const result = await knex.raw(`
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
    `, [chain]);    

    return result.rows;
  } catch (error) {
    throw new Error('Error fetching daily OI data: ' + error.message);
  }
};

const getOpenInterestSummaryStats = async (chain) => {
  try {
    const allData = await getOpenInterestData(chain);

    if (allData.length === 0) {
      throw new Error('No data found');
    }

    const smoothedData = smoothData(allData, 'daily_oi');
    const reversedSmoothedData = [...smoothedData].reverse();

    const latestData = reversedSmoothedData[0];
    const latestTs = new Date(latestData.ts);

    const getDateFromLatest = (days) => new Date(latestTs.getTime() - days * 24 * 60 * 60 * 1000);

    const value24h = reversedSmoothedData.find(item => {
      return new Date(item.ts) <= getDateFromLatest(1)
    });
    const value7d = reversedSmoothedData.find(item => new Date(item.ts) <= getDateFromLatest(7));
    const value28d = reversedSmoothedData.find(item => new Date(item.ts) <= getDateFromLatest(28));
    let valueYtd = smoothedData.find(item => new Date(item.ts) >= new Date(latestTs.getFullYear(), 0, 1));

    if (!valueYtd) {
      valueYtd = reversedSmoothedData[reversedSmoothedData.length - 1];
    }

    const oiValues = smoothedData.map(item => parseFloat(item.daily_oi));
    const standardDeviation = calculateStandardDeviation(oiValues);

    const current = parseFloat(allData[allData.length -1].daily_oi);
    const ath = Math.max(...oiValues, current);
    const atl = Math.min(...oiValues, current);

    return {
      current,
      delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.daily_oi) : null),
      delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.daily_oi) : null),
      delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.daily_oi) : null),
      delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.daily_oi) : null),
      ath,
      atl,
      ath_percentage: calculatePercentage(current, ath),
      atl_percentage: atl === 0 ? 100 : calculatePercentage(current, atl),
      standard_deviation: standardDeviation
    };
  } catch (error) {
    throw new Error('Error fetching daily OI summary stats: ' + error.message);
  }
};

module.exports = {
  fetchAndInsertAllPerpMarketHistoryData,
  fetchAndUpdateLatestPerpMarketHistoryData,
  getAllPerpMarketHistoryData,
  getOpenInterestData,
  getOpenInterestSummaryStats,
};
