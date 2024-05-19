const TIMEFRAMES = [
  'minute',
  'hour',
  'week',
  'month',
  'year',
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

module.exports = {
  TIMEFRAMES,
  calculateAPY
}