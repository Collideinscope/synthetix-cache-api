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

const CHUNK_SIZE = 1000; // large queries

const getAllPerpAccountStatsData = async (chain) => {
  try {
    let query = knex('perp_account_stats').orderBy('ts', 'asc');

    if (chain && CHAINS.includes(chain)) {
      query = query.where('chain', chain);
    }

    const result = await query;

    return result;
  } catch (error) {
    throw new Error('Error fetching all perp account stats data: ' + error.message);
  }
};

const getUniqueTradersSummaryStats = async (chain) => {
  try {
    const cumulativeData = await getCumulativeUniqueTraders(chain);

    const allData = cumulativeData[chain] || [];
    if (allData.length === 0) {
      throw new Error('No data found for the specified chain');
    }

    const smoothedData = smoothData(allData, 'cumulative_trader_count');  
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

    const volumeValues = smoothedData.map(item => parseFloat(item.cumulative_trader_count));
    const standardDeviation = calculateStandardDeviation(volumeValues);

    const current = parseFloat(allData[allData.length - 1].cumulative_trader_count);
    const ath = Math.max(...volumeValues, current);
    const atl = Math.min(...volumeValues, current);

    return {
      current,
      delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.cumulative_trader_count) : null),
      delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.cumulative_trader_count) : null),
      delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.cumulative_trader_count) : null),
      delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.cumulative_trader_count) : null),
      ath,
      atl,
      ath_percentage: calculatePercentage(current, ath),
      atl_percentage: atl === 0 ? 100 : calculatePercentage(current, atl),
      standard_deviation: standardDeviation
    };
  } catch (error) {
    throw new Error('Error fetching perp account stats summary stats: ' + error.message);
  }
};

const fetchAndInsertAllPerpAccountStatsData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
  }

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = `prod_${chain}_mainnet.fct_perp_account_stats_daily_${chain}_mainnet`;

    const rows = await troyDBKnex.raw(`
      SELECT ts, account_id, fees, volume, amount_liquidated, liquidations, cumulative_fees, cumulative_volume
      FROM ${tableName}
      ORDER BY ts DESC;
    `);

    const dataToInsert = rows.rows.map(row => ({
      ...row,
      chain,
    }));

    const insertInChunks = async (data) => {
      for (let i = 0; i < data.length; i += CHUNK_SIZE) {
        const chunk = data.slice(i, i + CHUNK_SIZE);
        await knex('perp_account_stats')
          .insert(chunk)
          .onConflict(['chain', 'ts', 'account_id'])
          .merge({
            fees: knex.raw('EXCLUDED.fees'),
            volume: knex.raw('EXCLUDED.volume'),
            amount_liquidated: knex.raw('EXCLUDED.amount_liquidated'),
            liquidations: knex.raw('EXCLUDED.liquidations'),
            cumulative_fees: knex.raw('EXCLUDED.cumulative_fees'),
            cumulative_volume: knex.raw('EXCLUDED.cumulative_volume')
          });
      }
    };

    await insertInChunks(dataToInsert);

    console.log(`Perp account stats data seeded successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error seeding perp account stats data for ${chain} chain:`, error);
  }
};

const fetchAndUpdateLatestPerpAccountStatsData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
    return;
  }

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = `prod_${chain}_mainnet.fct_perp_account_stats_daily_${chain}_mainnet`;

    const lastTimestampResult = await knex('perp_account_stats')
      .where('chain', chain)
      .orderBy('ts', 'desc')
      .first();

    const lastTimestamp = lastTimestampResult.ts;

    const newRows = await troyDBKnex.raw(`
      SELECT ts, account_id, fees, volume, amount_liquidated, liquidations, cumulative_fees, cumulative_volume
      FROM ${tableName}
      WHERE ts > ?
      ORDER BY ts DESC;
    `, [lastTimestamp]);

    if (newRows.rows.length === 0) {
      console.log(`No new perp account stats data to update for ${chain} chain.`);
      return;
    }

    const dataToInsert = newRows.rows.map(row => ({
      ...row,
      chain,
    }));

    const insertInChunks = async (data) => {
      for (let i = 0; i < data.length; i += CHUNK_SIZE) {
        const chunk = data.slice(i, i + CHUNK_SIZE);
        await knex('perp_account_stats')
          .insert(chunk)
          .onConflict(['chain', 'ts', 'account_id'])
          .merge({
            fees: knex.raw('EXCLUDED.fees'),
            volume: knex.raw('EXCLUDED.volume'),
            amount_liquidated: knex.raw('EXCLUDED.amount_liquidated'),
            liquidations: knex.raw('EXCLUDED.liquidations'),
            cumulative_fees: knex.raw('EXCLUDED.cumulative_fees'),
            cumulative_volume: knex.raw('EXCLUDED.cumulative_volume')
          });
      }
    };

    await insertInChunks(dataToInsert);

    console.log(`Perp account stats data updated successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error updating perp account stats data for ${chain} chain:`, error);
  }
};

const getCumulativeUniqueTraders = async (chain) => {
  try {
    const fetchCumulativeData = async (chain) => {
      const result = await knex.raw(`
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
        `, [chain]);

      return result.rows.map(row => ({
        ts: row.ts,
        cumulative_trader_count: row.cumulative_trader_count,
      }));
    };

    if (chain && CHAINS.includes(chain)) {
      const data = await fetchCumulativeData(chain);
      return { [chain]: data };
    }

    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const data = await fetchCumulativeData(chain);
        return { [chain]: data };
      })
    );

    return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
  } catch (error) {
    throw new Error('Error fetching cumulative unique trader data: ' + error.message);
  }
};

const getDailyNewUniqueTraders = async (chain) => {
  try {
    const fetchDailyData = async (chain) => {
      const result = await knex.raw(`
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
      `, [chain]);

      return result.rows.map(row => ({
        ts: row.date,
        daily_new_unique_traders: parseInt(row.daily_new_unique_traders),
      }));
    };

    if (chain && CHAINS.includes(chain)) {
      const data = await fetchDailyData(chain);
      return { [chain]: data };
    }

    const results = await Promise.all(
      CHAINS.map(async (chain) => {
        const data = await fetchDailyData(chain);
        return { [chain]: data };
      })
    );

    return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
  } catch (error) {
    throw new Error('Error fetching daily new unique traders: ' + error.message);
  }
};

const getDailyNewUniqueTradersSummary = async (chain) => {
  try {
    if (!chain || !CHAINS.includes(chain)) {
      throw new Error('Invalid chain parameter');
    }

    const dailyData = await getDailyNewUniqueTraders(chain);

    if (!dailyData[chain] || dailyData[chain].length === 0) {
      throw new Error('No data found for the specified chain');
    }

    const data = dailyData[chain];
    const latestData = data[data.length - 1];
    const latestDate = new Date(latestData.ts);

    const getDataFromLatest = (days) => {
      const targetDate = new Date(latestDate.getTime() - days * 24 * 60 * 60 * 1000);
      return data.find(item => new Date(item.ts) <= targetDate);
    };

    const value24h = getDataFromLatest(1);
    const value7d = getDataFromLatest(7);
    const value28d = getDataFromLatest(28);

    const valueYtd = data.find(item => new Date(item.ts).getFullYear() === latestDate.getFullYear());

    const allValues = data.map(item => item.daily_new_unique_traders);
    const standardDeviation = calculateStandardDeviation(allValues);

    const current = latestData.daily_new_unique_traders;
    const ath = Math.max(...allValues);
    const atl = Math.min(...allValues);

    return {
      current,
      delta_24h: calculateDelta(current, value24h ? value24h.daily_new_unique_traders : null),
      delta_7d: calculateDelta(current, value7d ? value7d.daily_new_unique_traders : null),
      delta_28d: calculateDelta(current, value28d ? value28d.daily_new_unique_traders : null),
      delta_ytd: calculateDelta(current, valueYtd ? valueYtd.daily_new_unique_traders : null),
      ath,
      atl,
      ath_percentage: calculatePercentage(current, ath),
      atl_percentage: atl === 0 ? 100 : calculatePercentage(current, atl),
      standard_deviation: standardDeviation
    };
  } catch (error) {
    throw new Error('Error fetching Daily New Unique Traders summary stats: ' + error.message);
  }
};

module.exports = {
  getAllPerpAccountStatsData,
  fetchAndInsertAllPerpAccountStatsData,
  fetchAndUpdateLatestPerpAccountStatsData,
  getUniqueTradersSummaryStats,
  getCumulativeUniqueTraders,
  getDailyNewUniqueTraders,
  getDailyNewUniqueTradersSummary,
};
