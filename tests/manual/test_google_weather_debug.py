"""
Debug script for Google Weather API - Show raw response
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

async def test_raw_api():
    """Test raw Google Weather API call and show full response"""

    # Load API key
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config = load_secrets(script_dir, 'secrets.ini')

    # Try to get google_weather key first, fall back to google if not found
    if config.has_option('api_keys', 'google_weather'):
        api_key = config.get('api_keys', 'google_weather')
        print("Using google_weather API key")
    else:
        api_key = config.get('api_keys', 'google')
        print("Using google API key (fallback)")

    # Test single location (Arnhem)
    base_url = "https://weather.googleapis.com/v1/forecast/hours:lookup"

    params = {
        'key': api_key,
        'location.latitude': 51.9851,
        'location.longitude': 5.8987,
        'hours': 24  # Just 24 hours for test
    }

    print("\n" + "="*60)
    print("GOOGLE WEATHER API RAW TEST")
    print("="*60)
    print(f"\nEndpoint: {base_url}")
    print(f"Parameters:")
    for k, v in params.items():
        if k == 'key':
            print(f"  {k}: {v[:20]}...")  # Truncate key
        else:
            print(f"  {k}: {v}")

    print(f"\nSending request...")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(base_url, params=params) as response:
                print(f"\nResponse Status: {response.status}")
                print(f"Response Headers:")
                for k, v in response.headers.items():
                    print(f"  {k}: {v}")

                response_text = await response.text()
                print(f"\nResponse Body Length: {len(response_text)} characters")
                print(f"\nRaw Response:")
                print("-" * 60)
                print(response_text[:2000])  # First 2000 chars
                if len(response_text) > 2000:
                    print(f"\n... (truncated, total {len(response_text)} chars)")
                print("-" * 60)

                if response.status == 200:
                    try:
                        data = json.loads(response_text)
                        print(f"\nParsed JSON Keys: {list(data.keys())}")

                        if 'hourlyForecasts' in data:
                            print(f"Number of hourly forecasts: {len(data['hourlyForecasts'])}")
                            if data['hourlyForecasts']:
                                print(f"\nFirst forecast sample:")
                                print(json.dumps(data['hourlyForecasts'][0], indent=2)[:500])
                        else:
                            print("⚠️  No 'hourlyForecasts' key in response!")
                            print(f"Available keys: {list(data.keys())}")

                        return data
                    except json.JSONDecodeError as e:
                        print(f"\n❌ Failed to parse JSON: {e}")
                        return None
                else:
                    print(f"\n❌ API returned non-200 status")
                    return None

    except Exception as e:
        print(f"\n❌ Request failed: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    asyncio.run(test_raw_api())
