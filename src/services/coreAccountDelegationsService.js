const { knex, troyDBKnex } = require('../config/db');
const { CHAINS } = require('../helpers');

const {
  calculateDelta,
  calculatePercentage,
  calculateStandardDeviation,
  smoothData
} = require('../helpers');

const getStakerCount = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchCount = async (chainToFetch) => {
      const result = await knex('core_account_delegations')
        .where('chain', chainToFetch)
        .where('collateral_type', collateralType)
        .countDistinct('account_id as staker_count')
        .first();

      return { [chainToFetch]: parseInt(result.staker_count) };
    };

    if (chain) {
      return await fetchCount(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchCount));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching staker count: ' + error.message);
  }
};

const getCumulativeUniqueStakers = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchCumulativeData = async (chainToFetch) => {
      const result = await knex.raw(`
        WITH daily_new_stakers AS (
          SELECT
            date_trunc('day', ts) AS day,
            pool_id,
            collateral_type,
            account_id,
            MIN(date_trunc('day', ts)) OVER (PARTITION BY account_id, pool_id, collateral_type) AS first_staked_day
          FROM
            core_account_delegations
          WHERE
            chain = ?
            AND collateral_type = ?
        ),
        daily_cumulative_counts AS (
          SELECT
            day,
            pool_id,
            collateral_type,
            COUNT(DISTINCT CASE WHEN day = first_staked_day THEN account_id END) AS new_stakers,
            SUM(COUNT(DISTINCT CASE WHEN day = first_staked_day THEN account_id END)) 
              OVER (PARTITION BY pool_id, collateral_type ORDER BY day) AS cumulative_staker_count
          FROM
            daily_new_stakers
          GROUP BY
            day, pool_id, collateral_type
        )
        SELECT
          day AS ts,
          pool_id,
          collateral_type,
          cumulative_staker_count
        FROM
          daily_cumulative_counts
        ORDER BY
          ts, pool_id, collateral_type;
      `, [chainToFetch, collateralType]);

      return { [chainToFetch]: result.rows.map(row => ({
        ts: row.ts,
        cumulative_staker_count: parseInt(row.cumulative_staker_count),
        pool_id: row.pool_id,
        collateral_type: row.collateral_type,
      })) };
    };

    if (chain) {
      return await fetchCumulativeData(chain);
    } else {
      const results = await Promise.all(CHAINS.map(fetchCumulativeData));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching cumulative unique staker data: ' + error.message);
  }
};

const getCoreAccountDelegationsDataByAccount = async (accountId) => {
  try {
    const result = await knex('core_account_delegations')
      .where('account_id', accountId)
      .orderBy('ts', 'desc');

    return result;
  } catch (error) {
    throw new Error('Error fetching core account delegations data by account: ' + error.message);
  }
};

const getUniqueStakersSummaryStats = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const processChainData = async (chainToProcess) => {
      const cumulativeData = await getCumulativeUniqueStakers(chainToProcess, collateralType);
      const allData = cumulativeData[chainToProcess] || [];
      if (allData.length === 0) {
        return null;
      }

      const smoothedData = smoothData(allData, 'cumulative_staker_count');
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

      const stakerValues = smoothedData.map(item => parseFloat(item.cumulative_staker_count));
      const standardDeviation = calculateStandardDeviation(stakerValues);

      const current = parseFloat(allData[allData.length - 1].cumulative_staker_count);
      const ath = Math.max(...stakerValues, current);
      const atl = Math.min(...stakerValues, current);

      return {
        current,
        delta_24h: calculateDelta(current, value24h ? parseFloat(value24h.cumulative_staker_count) : null),
        delta_7d: calculateDelta(current, value7d ? parseFloat(value7d.cumulative_staker_count) : null),
        delta_28d: calculateDelta(current, value28d ? parseFloat(value28d.cumulative_staker_count) : null),
        delta_ytd: calculateDelta(current, valueYtd ? parseFloat(valueYtd.cumulative_staker_count) : null),
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
    throw new Error('Error fetching Unique Stakers summary stats: ' + error.message);
  }
};

const getDailyNewUniqueStakers = async (chain, collateralType) => {
  try {
    if (!collateralType) {
      throw new Error('collateralType is required');
    }

    const fetchDailyData = async (chainToFetch) => {
      const result = await knex.raw(`
        WITH first_staking_day AS (
          SELECT
            account_id,
            MIN(DATE_TRUNC('day', ts)) AS first_day
          FROM
            core_account_delegations
          WHERE
            chain = ?
            AND collateral_type = ?
          GROUP BY
            account_id
        )
        SELECT
          first_day AS date,
          COUNT(*) AS daily_new_unique_stakers
        FROM
          first_staking_day
        GROUP BY
          first_day
        ORDER BY
          first_day;
      `, [chainToFetch, collateralType]);

      return { [chainToFetch]: result.rows.map(row => ({
        ts: row.date,
        daily_new_unique_stakers: parseInt(row.daily_new_unique_stakers),
      })) };
    };

    if (chain) {
      return await fetchDailyData(chain);
    } else {
      const results = await Promise.all(CHAINS.map(chainToFetch => fetchDailyData(chainToFetch)));
      return results.reduce((acc, curr) => ({ ...acc, ...curr }), {});
    }
  } catch (error) {
    throw new Error('Error fetching daily new unique stakers: ' + error.message);
  }
};

// Initial seed
const fetchAndInsertAllCoreAccountDelegationsData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
    return;
  }

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = `prod_${chain}_mainnet.fct_core_account_delegation_${chain}_mainnet`;

    const rows = await troyDBKnex.raw(`
      SELECT ts, account_id, pool_id, collateral_type, amount_delegated
      FROM ${tableName}
      ORDER BY ts DESC;
    `);

    const dataWithChainAdded = rows.rows.map(row => {
      row.chain = chain;
      return row;
    });

    await knex('core_account_delegations')
      .insert(dataWithChainAdded)
      .onConflict(['chain', 'account_id', 'pool_id', 'collateral_type'])
      .merge({
        ts: knex.raw('EXCLUDED.ts'),
        amount_delegated: knex.raw('EXCLUDED.amount_delegated')
      });

    console.log(`Core account delegations data seeded successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error seeding core account delegations data for ${chain} chain:`, error);
  }
};

const fetchAndUpdateLatestCoreAccountDelegationsData = async (chain) => {
  if (!chain) {
    console.error(`Chain must be provided for data updates.`);
    return;
  }

  if (!CHAINS.includes(chain)) {
    console.error(`Chain ${chain} not recognized.`);
    return;
  }

  try {
    const tableName = `prod_${chain}_mainnet.fct_core_account_delegation_${chain}_mainnet`;

    // Fetch the last timestamp from the cache
    const lastTimestampResult = await knex('core_account_delegations')
      .where('chain', chain)
      .orderBy('ts', 'desc')
      .first();

    const lastTimestamp = lastTimestampResult.ts;

    // Fetch new data starting from the last timestamp
    const newRows = await troyDBKnex.raw(`
      SELECT ts, account_id, pool_id, collateral_type, amount_delegated
      FROM ${tableName}
      WHERE ts > ?
      ORDER BY ts ASC;
    `, [lastTimestamp]);

    if (newRows.rows.length === 0) {
      console.log(`No new core account delegations data to update for ${chain} chain.`);
      return;
    }

    const dataWithChainAdded = newRows.rows.map(row => ({
      ...row,
      chain
    }));

    await knex('core_account_delegations')
      .insert(dataWithChainAdded)
      .onConflict(['chain', 'account_id', 'pool_id', 'collateral_type'])
      .merge({
        ts: knex.raw('EXCLUDED.ts'),
        amount_delegated: knex.raw('EXCLUDED.amount_delegated')
      });
      
    console.log(`Core account delegations data updated successfully for ${chain} chain.`);
  } catch (error) {
    console.error(`Error updating core account delegations data for ${chain} chain:`, error);
  }
};

module.exports = {
  getStakerCount,
  getCoreAccountDelegationsDataByAccount,
  fetchAndInsertAllCoreAccountDelegationsData,
  fetchAndUpdateLatestCoreAccountDelegationsData,
  getCumulativeUniqueStakers,
  getUniqueStakersSummaryStats,
  getDailyNewUniqueStakers,
};