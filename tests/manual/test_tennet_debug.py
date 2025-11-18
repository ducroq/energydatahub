"""
Debug TenneT API response to understand data structure
"""
import asyncio
import platform
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from tenneteu import TenneTeuClient

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from utils.helpers import load_secrets


async def debug_tennet_api():
    """Debug the TenneT API response structure."""
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Load API key from secrets.ini
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    config = load_secrets(project_root, 'secrets.ini')
    api_key = config.get('api_keys', 'tennet')

    # Setup time range - TenneT data may have a delay, try yesterday
    amsterdam_tz = ZoneInfo("Europe/Amsterdam")
    end = datetime.now(amsterdam_tz) - timedelta(days=1)
    start = end - timedelta(hours=6)  # 6 hours from yesterday

    print(f"Testing TenneT API")
    print(f"Time range: {start} to {end}")
    print("-" * 60)

    client = TenneTeuClient(api_key=api_key)

    try:
        # Fetch settlement prices
        print("\n1. Settlement Prices DataFrame:")
        settlement_df = client.query_settlement_prices(start, end)
        print(f"\nShape: {settlement_df.shape}")
        print(f"\nColumns: {list(settlement_df.columns)}")
        print(f"\nFirst 3 rows:")
        print(settlement_df.head(3))
        print(f"\nData types:")
        print(settlement_df.dtypes)

        # Fetch balance delta
        print("\n\n2. Balance Delta DataFrame:")
        balance_df = client.query_balance_delta(start, end)
        print(f"\nShape: {balance_df.shape}")
        print(f"\nColumns: {list(balance_df.columns)}")
        print(f"\nFirst 3 rows:")
        print(balance_df.head(3))
        print(f"\nData types:")
        print(balance_df.dtypes)

    except Exception as e:
        print(f"\nError: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(debug_tennet_api())
