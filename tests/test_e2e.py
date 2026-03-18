import json, time, urllib.request

# Test through nginx (same as browser)
BASE = "http://nginx"

now_ms = int(time.time() * 1000)

for tf in ["1m", "5m", "15m", "1h"]:
    # Step 1: Initial load (like when chart first opens)
    url = f"{BASE}/api/klines?symbol=BTCUSDT&interval={tf}&limit=10"
    with urllib.request.urlopen(url) as resp:
        initial = json.loads(resp.read())
    
    if len(initial) < 2:
        print(f"{tf}: Not enough initial data ({len(initial)} candles)")
        continue
    
    spacing_init = initial[1]["openTime"] - initial[0]["openTime"]
    earliest_ms = initial[0]["openTime"]
    
    # Step 2: Scroll-left load (like when user scrolls left)
    url2 = f"{BASE}/api/klines?symbol=BTCUSDT&interval={tf}&limit=10&endTime={earliest_ms}"
    with urllib.request.urlopen(url2) as resp:
        scroll = json.loads(resp.read())
    
    spacing_scroll = scroll[1]["openTime"] - scroll[0]["openTime"] if len(scroll) >= 2 else 0
    
    print(f"=== {tf} ===")
    print(f"  Initial: {len(initial)} candles, spacing={spacing_init/60000:.1f}min")
    print(f"  Scroll:  {len(scroll)} candles, spacing={spacing_scroll/60000:.1f}min")
    
    # Check OHLCV values differ between initial and scroll (aggregation proof)
    if scroll:
        sc = scroll[-1]
        print(f"  Last scroll candle: O={sc['open']:.2f} H={sc['high']:.2f} L={sc['low']:.2f} C={sc['close']:.2f} V={sc['volume']:.4f}")
    
    # For 5m: also fetch raw 1m data at the same time to compare
    if tf == "5m" and len(scroll) >= 1:
        # Get 1m data for the same time range as first 5m scroll candle
        t5m = scroll[0]["openTime"]
        url3 = f"{BASE}/api/klines?symbol=BTCUSDT&interval=1m&limit=5&endTime={t5m + 300000}"
        with urllib.request.urlopen(url3) as resp:
            raw_1m = json.loads(resp.read())
        
        print(f"\n  === Comparison: 5m candle vs raw 1m candles ===")
        print(f"  5m candle at {t5m}: O={scroll[0]['open']:.2f} H={scroll[0]['high']:.2f} L={scroll[0]['low']:.2f} C={scroll[0]['close']:.2f} V={scroll[0]['volume']:.4f}")
        for c in raw_1m:
            print(f"    1m at {c['openTime']}: O={c['open']:.2f} H={c['high']:.2f} L={c['low']:.2f} C={c['close']:.2f} V={c['volume']:.4f}")
        
        # Verify aggregation
        if raw_1m:
            agg_high = max(c["high"] for c in raw_1m)
            agg_low = min(c["low"] for c in raw_1m)
            agg_open = raw_1m[0]["open"]
            agg_close = raw_1m[-1]["close"]
            agg_vol = sum(c["volume"] for c in raw_1m)
            print(f"  Expected 5m: O={agg_open:.2f} H={agg_high:.2f} L={agg_low:.2f} C={agg_close:.2f} V={agg_vol:.4f}")
            match = (abs(scroll[0]["open"] - agg_open) < 0.01 and
                     abs(scroll[0]["high"] - agg_high) < 0.01 and
                     abs(scroll[0]["low"] - agg_low) < 0.01 and
                     abs(scroll[0]["close"] - agg_close) < 0.01)
            print(f"  Aggregation correct: {match}")
    
    print()
