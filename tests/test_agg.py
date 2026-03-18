import asyncio, time
from serving.routers.klines import get_klines

async def test():
    now_ms = int(time.time() * 1000)
    end_ms = now_ms - 3 * 86400 * 1000

    for tf in ["5m", "15m", "1h", "4h"]:
        print(f"=== {tf} with endTime (scroll-left) ===")
        result = await get_klines(symbol="BTCUSDT", interval=tf, limit=10, endTime=end_ms)
        print(f"  Got {len(result)} candles")
        if len(result) >= 2:
            for i, c in enumerate(result[:3]):
                print(f"  [{i}] openTime={c['openTime']}")
            diff = result[1]["openTime"] - result[0]["openTime"]
            print(f"  Spacing: {diff}ms = {diff/60000}min")
        print()

asyncio.run(test())
