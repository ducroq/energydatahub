"""
Test script for new collectors
-------------------------------
Tests all migrated collectors to verify they work correctly.

Usage:
    python test_collectors.py
"""

import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from configparser import ConfigParser
import os
import platform

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Set Windows event loop policy if needed
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def test_elspot_collector():
    """Test Nord Pool Elspot collector."""
    print("\n" + "="*60)
    print("Testing ElspotCollector (Nord Pool)")
    print("="*60)

    from collectors.elspot import ElspotCollector

    try:
        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime.now(amsterdam_tz)
        end = start + timedelta(hours=24)

        collector = ElspotCollector()
        dataset = await collector.collect(start, end, country_code='NL')

        if dataset:
            print(f"[PASS] Success: Collected {len(dataset.data)} data points")
            print(f"  Source: {dataset.metadata['source']}")
            print(f"  Units: {dataset.metadata['units']}")

            # Show first few prices
            print(f"\n  First 3 prices:")
            for timestamp, price in list(dataset.data.items())[:3]:
                print(f"    {timestamp}: {price} EUR/MWh")

            # Show metrics
            metrics = collector.get_metrics(limit=1)[0]
            print(f"\n  Metrics:")
            print(f"    Duration: {metrics.duration_seconds:.2f}s")
            print(f"    Status: {metrics.status.value}")
            print(f"    Warnings: {len(metrics.warnings)}")

            return True
        else:
            print("[FAIL] Failed: No data returned")
            return False

    except Exception as e:
        print(f"[FAIL] Error: {type(e).__name__}: {e}")
        return False


async def test_entsoe_collector():
    """Test ENTSO-E collector."""
    print("\n" + "="*60)
    print("Testing EntsoeCollector (ENTSO-E)")
    print("="*60)

    from collectors.entsoe import EntsoeCollector

    try:
        # Load API key
        script_dir = os.path.dirname(os.path.abspath(__file__))
        secrets_file = os.path.join(script_dir, 'secrets.ini')

        config = ConfigParser()
        config.read(secrets_file)
        api_key = config.get('api_keys', 'entsoe')

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime.now(amsterdam_tz)
        end = start + timedelta(hours=24)

        collector = EntsoeCollector(api_key=api_key)
        dataset = await collector.collect(start, end, country_code='NL')

        if dataset:
            print(f"[PASS] Success: Collected {len(dataset.data)} data points")
            print(f"  Source: {dataset.metadata['source']}")
            print(f"  Units: {dataset.metadata['units']}")

            # Show first few prices
            print(f"\n  First 3 prices:")
            for timestamp, price in list(dataset.data.items())[:3]:
                print(f"    {timestamp}: {price} EUR/MWh")

            # Show metrics
            metrics = collector.get_metrics(limit=1)[0]
            print(f"\n  Metrics:")
            print(f"    Duration: {metrics.duration_seconds:.2f}s")
            print(f"    Status: {metrics.status.value}")
            print(f"    Warnings: {len(metrics.warnings)}")

            return True
        else:
            print("[FAIL] Failed: No data returned")
            return False

    except Exception as e:
        print(f"[FAIL] Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_energyzero_collector():
    """Test EnergyZero collector."""
    print("\n" + "="*60)
    print("Testing EnergyZeroCollector")
    print("="*60)

    from collectors.energyzero import EnergyZeroCollector
    from energyzero import PriceType

    try:
        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime.now(amsterdam_tz)
        end = start + timedelta(hours=24)

        collector = EnergyZeroCollector(price_type=PriceType.ALL_IN)
        dataset = await collector.collect(start, end)

        if dataset:
            print(f"[PASS] Success: Collected {len(dataset.data)} data points")
            print(f"  Source: {dataset.metadata['source']}")
            print(f"  Units: {dataset.metadata['units']}")

            # Show first few prices
            print(f"\n  First 3 prices:")
            for timestamp, price in list(dataset.data.items())[:3]:
                print(f"    {timestamp}: {price} EUR/kWh")

            # Show metrics
            metrics = collector.get_metrics(limit=1)[0]
            print(f"\n  Metrics:")
            print(f"    Duration: {metrics.duration_seconds:.2f}s")
            print(f"    Status: {metrics.status.value}")
            print(f"    Warnings: {len(metrics.warnings)}")

            return True
        else:
            print("[FAIL] Failed: No data returned")
            return False

    except Exception as e:
        print(f"[FAIL] Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_epex_collector():
    """Test EPEX SPOT collector."""
    print("\n" + "="*60)
    print("Testing EpexCollector (EPEX SPOT via Awattar)")
    print("="*60)

    from collectors.epex import EpexCollector

    try:
        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime.now(amsterdam_tz)
        end = start + timedelta(hours=24)

        collector = EpexCollector()
        dataset = await collector.collect(start, end)

        if dataset:
            print(f"[PASS] Success: Collected {len(dataset.data)} data points")
            print(f"  Source: {dataset.metadata['source']}")
            print(f"  Units: {dataset.metadata['units']}")

            # Show first few prices
            print(f"\n  First 3 prices:")
            for timestamp, price in list(dataset.data.items())[:3]:
                print(f"    {timestamp}: {price} EUR/MWh")

            # Show metrics
            metrics = collector.get_metrics(limit=1)[0]
            print(f"\n  Metrics:")
            print(f"    Duration: {metrics.duration_seconds:.2f}s")
            print(f"    Status: {metrics.status.value}")
            print(f"    Warnings: {len(metrics.warnings)}")

            return True
        else:
            print("[FAIL] Failed: No data returned")
            return False

    except Exception as e:
        print(f"[FAIL] Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_openweather_collector():
    """Test OpenWeather collector."""
    print("\n" + "="*60)
    print("Testing OpenWeatherCollector")
    print("="*60)

    from collectors.openweather import (
        OpenWeatherCollector,
        get_OpenWeather_geographical_coordinates_in_NL
    )

    try:
        # Load API key
        script_dir = os.path.dirname(os.path.abspath(__file__))
        secrets_file = os.path.join(script_dir, 'secrets.ini')

        config = ConfigParser()
        config.read(secrets_file)
        api_key = config.get('api_keys', 'openweather')

        # Get coordinates for Amsterdam
        coords = await get_OpenWeather_geographical_coordinates_in_NL(api_key, "Amsterdam")

        if not coords:
            print("[FAIL] Failed: Could not get coordinates for Amsterdam")
            return False

        amsterdam_tz = ZoneInfo('Europe/Amsterdam')
        start = datetime.now(amsterdam_tz)
        end = start + timedelta(hours=24)

        collector = OpenWeatherCollector(
            api_key=api_key,
            latitude=coords['latitude'],
            longitude=coords['longitude']
        )
        dataset = await collector.collect(start, end)

        if dataset:
            print(f"[PASS] Success: Collected {len(dataset.data)} data points")
            print(f"  Source: {dataset.metadata['source']}")
            print(f"  City: {dataset.metadata.get('city', 'Unknown')}")

            # Show first forecast
            print(f"\n  First forecast:")
            first_timestamp = list(dataset.data.keys())[0]
            first_data = dataset.data[first_timestamp]
            print(f"    Time: {first_timestamp}")
            for key, value in list(first_data.items())[:5]:
                print(f"      {key}: {value}")

            # Show metrics
            metrics = collector.get_metrics(limit=1)[0]
            print(f"\n  Metrics:")
            print(f"    Duration: {metrics.duration_seconds:.2f}s")
            print(f"    Status: {metrics.status.value}")
            print(f"    Warnings: {len(metrics.warnings)}")

            return True
        else:
            print("[FAIL] Failed: No data returned")
            return False

    except Exception as e:
        print(f"[FAIL] Error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Run all collector tests."""
    print("\n" + "="*60)
    print("COLLECTOR TEST SUITE")
    print("="*60)
    print(f"Time: {datetime.now()}")

    results = {}

    # Test each collector
    results['Elspot'] = await test_elspot_collector()
    results['ENTSO-E'] = await test_entsoe_collector()
    results['EnergyZero'] = await test_energyzero_collector()
    results['EPEX'] = await test_epex_collector()
    results['OpenWeather'] = await test_openweather_collector()

    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for name, result in results.items():
        status = "[PASS] PASS" if result else "[FAIL] FAIL"
        print(f"{status}: {name}")

    print(f"\nTotal: {passed}/{total} passed")

    if passed == total:
        print("\n[SUCCESS] All tests passed!")
        return 0
    else:
        print(f"\n[WARNING] {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
