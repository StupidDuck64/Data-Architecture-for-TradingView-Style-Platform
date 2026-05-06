import type { Candle } from "../../types";

interface DataPoint {
  time: number;
  value: number;
}

export function calcSMA(candles: Candle[], period: number): DataPoint[] {
  if (!candles || candles.length < period) return [];
  const out: DataPoint[] = [];
  for (let i = period - 1; i < candles.length; i++) {
    const avg =
      candles.slice(i - period + 1, i + 1).reduce((s, c) => s + c.close, 0) /
      period;
    out.push({ time: candles[i].time, value: +avg.toFixed(4) });
  }
  return out;
}

export function calcEMA(candles: Candle[], period: number): DataPoint[] {
  if (!candles || candles.length < period) return [];
  const k = 2 / (period + 1);
  const out: DataPoint[] = [];
  let ema = candles.slice(0, period).reduce((s, c) => s + c.close, 0) / period;
  out.push({ time: candles[period - 1].time, value: +ema.toFixed(4) });
  for (let i = period; i < candles.length; i++) {
    ema = candles[i].close * k + ema * (1 - k);
    out.push({ time: candles[i].time, value: +ema.toFixed(4) });
  }
  return out;
}

export function calcRSI(candles: Candle[], period: number): DataPoint[] {
  const out: DataPoint[] = [];
  if (candles.length < period + 1) return out;
  let gains = 0,
    losses = 0;
  for (let i = 1; i <= period; i++) {
    const diff = candles[i].close - candles[i - 1].close;
    if (diff > 0) gains += diff;
    else losses -= diff;
  }
  let avgGain = gains / period,
    avgLoss = losses / period;
  const rsi = (v: number) => (avgLoss === 0 ? 100 : 100 - 100 / (1 + v));
  out.push({
    time: candles[period].time,
    value: +rsi(avgGain / avgLoss).toFixed(2),
  });
  for (let i = period + 1; i < candles.length; i++) {
    const diff = candles[i].close - candles[i - 1].close;
    const g = diff > 0 ? diff : 0;
    const l = diff < 0 ? -diff : 0;
    avgGain = (avgGain * (period - 1) + g) / period;
    avgLoss = (avgLoss * (period - 1) + l) / period;
    out.push({
      time: candles[i].time,
      value: +rsi(avgGain / (avgLoss || 1e-10)).toFixed(2),
    });
  }
  return out;
}

export function calcMFI(candles: Candle[], period: number): DataPoint[] {
  const out: DataPoint[] = [];
  const typicals = candles.map((c) => ({
    time: c.time,
    tp: (c.high + c.low + c.close) / 3,
    vol: c.volume,
  }));
  for (let i = period; i < typicals.length; i++) {
    let posFlow = 0,
      negFlow = 0;
    for (let j = i - period + 1; j <= i; j++) {
      const mf = typicals[j].tp * typicals[j].vol;
      if (typicals[j].tp >= typicals[j - 1].tp) posFlow += mf;
      else negFlow += mf;
    }
    const ratio = negFlow === 0 ? 100 : 100 - 100 / (1 + posFlow / negFlow);
    out.push({ time: typicals[i].time, value: +ratio.toFixed(2) });
  }
  return out;
}
