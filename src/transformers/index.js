const { TIMEFRAMES, calculateAPY } = require('../helpers');

/*
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
  
    // remove the initial apy valus
    delete obj.apy_24h;
    delete obj.apy_7d;
    delete obj.apy_28d;

    return {
      ...obj,
      apys: apyValues,
    };
  })

};

module.exports = {
  modifyAPYDataWithTimeframes,
};
