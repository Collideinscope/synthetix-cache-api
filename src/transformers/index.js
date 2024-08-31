const { TIMEFRAMES, calculateAPY } = require('../helpers');

/*
APY  
input: list of apy entries

  output: list of apy entries with with apys for every timeframe
*/
const modifyAPYDataWithTimeframes = (data) => {

  return data.map(obj => {
    const apyPeriods = {
      apy_24h: parseFloat(obj.apy_24h),
      apy_7d: parseFloat(obj.apy_7d),
      apy_28d: parseFloat(obj.apy_28d),
    };

    const apyValues = Object.entries(apyPeriods).reduce((acc, [period, apy]) => {
      acc[period] = TIMEFRAMES.reduce((result, timeframe) => {
        result[timeframe] = calculateAPY(apy, timeframe);
        return result;
      }, {});
  
      return acc;
    }, {});

    return {
      ts: obj.ts,
      apys: apyValues,
    };
  })

};

/*
  TVL
  input: list rows of tvl entries

  output: list of formatted tvl entries
*/
const transformTVLEntries = (rows, chain) => {
  const rowsTransformed = rows.map(row => {
    const hourKey = `${row.ts.toISOString().slice(0, 13)}:00:00Z`;

    return {
      ...row,
      block_ts: row.ts,
      chain,
      ts: new Date(hourKey)
    };
  });

  return rowsTransformed;
}

module.exports = {
  modifyAPYDataWithTimeframes,
  transformTVLEntries,
};
