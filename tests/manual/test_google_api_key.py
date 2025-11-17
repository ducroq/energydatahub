"""
Test Google Weather API with the actual key used in production
"""
import asyncio
import json
import aiohttp
import os
import sys
import platform

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.helpers import load_secrets

# Windows event loop fix
if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def test_api_with_key(api_key, key_name):
    """Test API call with a specific key"""
    print(f"\n{'='*60}")
    print(f"TESTING WITH {key_name}")
    print(f"{'='*60}")
    print(f"Key: {api_key[:20]}...")

    base_url = "https://weather.googleapis.com/v1/forecast/hours:lookup"

    # Test with Arnhem
    params = {
        'key': api_key,
        'location.latitude': 51.9851,
        'location.longitude': 5.8987,
        'hours': 24
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params) as response:
                print(f"\nHTTP Status: {response.status}")

                response_text = await response.text()

                if response.status == 200:
                    data = json.loads(response_text)
                    print(f"Response keys: {list(data.keys())}")

                    if 'hourlyForecasts' in data:
                        forecast_count = len(data['hourlyForecasts'])
                        print(f"Number of forecasts: {forecast_count}")

                        if forecast_count > 0:
                            print(f"\n✓ SUCCESS! Got {forecast_count} hourly forecasts")
                            print(f"\nFirst forecast sample:")
                            print(json.dumps(data['hourlyForecasts'][0], indent=2)[:500])
                            return True
                        else:
                            print(f"\n✗ FAILED: API returned 0 forecasts")
                            print(f"\nFull response:")
                            print(json.dumps(data, indent=2))
                            return False
                    else:
                        print(f"\n✗ FAILED: No 'hourlyForecasts' key")
                        print(f"\nFull response:")
                        print(json.dumps(data, indent=2))
                        return False
                else:
                    print(f"\n✗ FAILED: HTTP {response.status}")
                    print(f"\nResponse:")
                    print(response_text[:1000])
                    return False

    except Exception as e:
        print(f"\n✗ EXCEPTION: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = load_secrets(script_dir, 'secrets.ini')

    print("="*60)
    print("GOOGLE WEATHER API KEY COMPARISON TEST")
    print("="*60)

    # Test google_weather key (from env var or secrets)
    if config.has_option('api_keys', 'google_weather'):
        google_weather_key = config.get('api_keys', 'google_weather')
        result1 = await test_api_with_key(google_weather_key, "google_weather")
    else:
        print("\n✗ No google_weather key found!")
        result1 = False

    # Test old google key (for comparison)
    if config.has_option('api_keys', 'google'):
        google_key = config.get('api_keys', 'google')
        result2 = await test_api_with_key(google_key, "google (old)")
    else:
        print("\n✗ No google key found!")
        result2 = False

    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"google_weather: {'✓ WORKS' if result1 else '✗ FAILED'}")
    print(f"google (old):   {'✓ WORKS' if result2 else '✗ FAILED'}")

    if not result1:
        print(f"\n⚠️  The google_weather key is NOT returning forecast data!")
        print(f"   Possible issues:")
        print(f"   1. Weather API might not be enabled on this project")
        print(f"   2. API key might have restrictions (referrer, IP, etc.)")
        print(f"   3. API might have geographic restrictions")
        print(f"   4. API might still be propagating (wait a few minutes)")

if __name__ == "__main__":
    asyncio.run(main())
