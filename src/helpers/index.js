const TIMEFRAMES = [
  'minute',
  'hour',
  'week',
  'month',
  'year',
];

const CHAINS = [
  'base',
  'arbitrum'
];

function calculateAPY(apy, timeframe) {
  const periods = {
    'minute': 525600, // 365 days * 24 hours * 60 minutes
    'hour': 8760,     // 365 days * 24 hours
    'day': 365,       // 365 days
    'week': 52,       // 52 weeks
    'month': 12,      // 12 months
    'year': 1         // 1 year
  };

  const n = periods[timeframe];

  return (Math.pow(1 + apy, 1 / n) - 1).toFixed(8);
}


const calculateDelta = (current, previous) => {
  console.log(current, previous)
  return ((current - previous) / previous) * 100;
};

const calculatePercentage = (current, comparison) => {
  return ((current - comparison) / comparison) * 100;
};

const calculateStandardDeviation = (values) => {
  const mean = values.reduce((acc, val) => acc + val, 0) / values.length;
  const variance = values.reduce((acc, val) => acc + Math.pow(val - mean, 2), 0) / values.length;

  return Math.sqrt(variance);
};


// moving average, default 7d
const smoothData = (data, windowSize = 168) => {
  return data.map((obj, idx, array) => {
    if (idx < windowSize - 1) {
      // Not enough previous data objs, return original
      return obj;
    }
    const window = array.slice(idx - windowSize + 1, idx + 1);
    const sum = window.reduce((acc, curr) => acc + parseFloat(curr.apy_7d), 0);

    return {
      ...obj,
      apy_7d: sum / windowSize,
    }
  });
};

module.exports = {
  TIMEFRAMES,
  CHAINS,
  calculateAPY,
  calculateDelta,
  calculatePercentage,
  calculateStandardDeviation,
  smoothData,
}