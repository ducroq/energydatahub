"""Test Luchtmeetnet caching optimization"""
import asyncio
import time
import platform
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from collectors.luchtmeetnet import LuchtmeetnetCollector


async def test_caching():
    """Test that second collection is much faster due to caching."""
    collector = LuchtmeetnetCollector(51.966472, 5.94009)
    tz = ZoneInfo('Europe/Amsterdam')
    end = datetime.now(tz)
    start = end - timedelta(hours=24)

    print("="*60)
    print("Testing Luchtmeetnet Station Caching Optimization")
    print("="*60)

    # First collection (should fetch stations)
    print("\n[Test 1] First collection (cache miss)...")
    t0 = time.time()
    data1 = await collector.collect(start, end)
    t1 = time.time()
    time1 = t1 - t0

    if data1:
        print(f"[PASS] Collection successful")
        print(f"  Data points: {len(data1.data)}")
        print(f"  Duration: {time1:.2f}s")
    else:
        print(f"[FAIL] Collection failed")
        return

    # Small delay
    await asyncio.sleep(0.5)

    # Second collection (should use cache)
    print("\n[Test 2] Second collection (cache hit)...")
    t0 = time.time()
    data2 = await collector.collect(start, end)
    t1 = time.time()
    time2 = t1 - t0

    if data2:
        print(f"[PASS] Collection successful")
        print(f"  Data points: {len(data2.data)}")
        print(f"  Duration: {time2:.2f}s")
    else:
        print(f"[FAIL] Collection failed")
        return

    # Calculate improvement
    speedup = time1 / time2
    time_saved = time1 - time2

    print("\n" + "="*60)
    print("RESULTS")
    print("="*60)
    print(f"First run:  {time1:.2f}s (cache miss)")
    print(f"Second run: {time2:.2f}s (cache hit)")
    print(f"Speedup:    {speedup:.1f}x faster")
    print(f"Time saved: {time_saved:.2f}s ({time_saved/time1*100:.1f}% reduction)")
    print("="*60)

    if speedup > 5:
        print("\n[SUCCESS] Cache optimization working excellently! (>5x speedup)")
    elif speedup > 2:
        print("\n[SUCCESS] Cache optimization working well! (>2x speedup)")
    else:
        print(f"\n[WARNING] Cache speedup lower than expected ({speedup:.1f}x)")


if __name__ == "__main__":
    # Set appropriate event loop policy for Windows
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(test_caching())
