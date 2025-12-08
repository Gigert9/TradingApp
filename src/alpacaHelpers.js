/**
 * Convenience wrapper around Alpaca’s `getClock` endpoint.
 * @typedef {import('@alpacahq/alpaca-trade-api').default} Alpaca
 *
 * @param {Alpaca} alpaca An initialised Alpaca client instance.
 * @returns {Promise<any>} The current market clock information.
 */
export async function getCurrentClock(alpaca) {
  return alpaca.getClock();
}

/**
 * Convenience wrapper around Alpaca’s `getCalendar` endpoint.
 * Accepts optional params like { start: 'YYYY-MM-DD', end: 'YYYY-MM-DD' }.
 *
 * @param {Alpaca} alpaca An initialised Alpaca client instance.
 * @param {Record<string, any>} [params]
 * @returns {Promise<any>} The trading calendar entries.
 */
export async function getTradingCalendar(alpaca, params = {}) {
  return alpaca.getCalendar(params);
}
