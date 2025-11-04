"""
Test script for Google Weather API collector
"""
import asyncio
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import platform
import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from collectors import GoogleWeatherCollector
from utils.helpers import load_secrets

async def test_single_location():
    """Test single location collection"""
    print("\n" + "="*60)
    print("TEST 1: Single Location (Arnhem)")
    print("="*60)

    # Load API key
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = load_secrets(script_dir, 'secrets.ini')
    google_api_key = config.get('api_keys', 'google_weather')

    # Initialize collector
    collector = GoogleWeatherCollector(
        api_key=google_api_key,
        latitude=51.9851,
        longitude=5.8987,
        hours=48  # 2 days for quick test
    )

    # Set time range
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz)
    end = start + timedelta(days=2)

    print(f"Fetching weather for Arnhem from {start} to {end}")

    try:
        data = await collector.collect(start, end)

        if data:
            print(f"✓ Successfully collected {len(data.data)} data points")
            print(f"  Source: {data.source}")
            print(f"  Data type: {data.data_type}")

            # Show first few entries
            if data.data:
                print("\nFirst 3 entries:")
                for i, entry in enumerate(list(data.data)[:3]):
                    print(f"  {i+1}. {entry['datetime']}: {entry.get('temperature', 'N/A')}°C, "
                          f"wind {entry.get('wind_speed', 'N/A')} m/s")

            return True
        else:
            print("✗ No data collected")
            return False

    except Exception as e:
        print(f"✗ Error: {e}")
        return False

async def test_multi_location():
    """Test multi-location collection"""
    print("\n" + "="*60)
    print("TEST 2: Multi-Location (6 Strategic Locations)")
    print("="*60)

    # Load API key
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = load_secrets(script_dir, 'secrets.ini')
    google_api_key = config.get('api_keys', 'google_weather')

    # Strategic locations
    locations = [
        {"name": "Hamburg_DE", "lat": 53.5511, "lon": 9.9937},
        {"name": "Munich_DE", "lat": 48.1351, "lon": 11.5820},
        {"name": "Arnhem_NL", "lat": 51.9851, "lon": 5.8987},
        {"name": "IJmuiden_NL", "lat": 52.4608, "lon": 4.6262},
        {"name": "Brussels_BE", "lat": 50.8503, "lon": 4.3517},
        {"name": "Esbjerg_DK", "lat": 55.4760, "lon": 8.4516},
    ]

    # Initialize collector
    collector = GoogleWeatherCollector(
        api_key=google_api_key,
        locations=locations,
        hours=72  # 3 days for test
    )

    # Set time range
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz)
    end = start + timedelta(days=3)

    print(f"Fetching weather for {len(locations)} locations from {start} to {end}")

    try:
        data = await collector.collect(start, end)

        if data:
            print(f"✓ Successfully collected data")
            print(f"  Source: {data.source}")

            # Check each location
            print(f"\nData points per location:")
            for loc in locations:
                loc_name = loc['name']
                if loc_name in data.data:
                    count = len(data.data[loc_name])
                    print(f"  {loc_name}: {count} data points")

                    # Show sample
                    if data.data[loc_name]:
                        first_entry = list(data.data[loc_name].values())[0]
                        print(f"    Sample: {first_entry.get('temperature', 'N/A')}°C, "
                              f"wind {first_entry.get('wind_speed', 'N/A')} m/s, "
                              f"cloud {first_entry.get('cloud_cover', 'N/A')}%")
                else:
                    print(f"  {loc_name}: ✗ NO DATA")

            return True
        else:
            print("✗ No data collected")
            return False

    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

async def test_full_forecast():
    """Test full 10-day forecast"""
    print("\n" + "="*60)
    print("TEST 3: Full 10-Day Forecast (240 hours)")
    print("="*60)

    # Load API key
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = load_secrets(script_dir, 'secrets.ini')
    google_api_key = config.get('api_keys', 'google_weather')

    # Just test with Hamburg
    collector = GoogleWeatherCollector(
        api_key=google_api_key,
        latitude=53.5511,
        longitude=9.9937,
        hours=240  # Full 10 days
    )

    # Set time range
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz)
    end = start + timedelta(days=10)

    print(f"Fetching 10-day weather for Hamburg from {start} to {end}")

    try:
        data = await collector.collect(start, end)

        if data:
            print(f"✓ Successfully collected {len(data.data)} data points")
            print(f"  Expected: ~240 hours")
            print(f"  Coverage: {len(data.data) / 240 * 100:.1f}%")

            return True
        else:
            print("✗ No data collected")
            return False

    except Exception as e:
        print(f"✗ Error: {e}")
        return False

async def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("GOOGLE WEATHER API COLLECTOR TEST SUITE")
    print("="*60)

    results = []

    # Test 1: Single location
    results.append(("Single Location", await test_single_location()))

    # Test 2: Multi-location
    results.append(("Multi-Location", await test_multi_location()))

    # Test 3: Full forecast
    results.append(("Full 10-Day Forecast", await test_full_forecast()))

    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    for name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{status}: {name}")

    total = len(results)
    passed = sum(1 for _, p in results if p)
    print(f"\nTotal: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed!")
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")

if __name__ == "__main__":
    # Set appropriate event loop policy for Windows
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(main())
