"""
Test TenneT collector and show detailed breakdown of data
"""
import asyncio
import platform
import os
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from collectors.tennet import TennetCollector
from collectors.base import RetryConfig, CircuitBreakerConfig
from utils.helpers import load_secrets


async def test_tennet_detailed():
    """Test the TenneT collector and show data breakdown."""
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Load API key from secrets.ini
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    config = load_secrets(project_root, 'secrets.ini')
    api_key = config.get('api_keys', 'tennet')

    # Use a smaller time range for detailed analysis
    amsterdam_tz = ZoneInfo("Europe/Amsterdam")
    start = datetime(2025, 11, 16, 9, 0, 0, tzinfo=amsterdam_tz)
    end = datetime(2025, 11, 16, 12, 0, 0, tzinfo=amsterdam_tz)  # 3 hours

    print(f"Testing TenneT collector - Detailed analysis")
    print(f"Time range: {start} to {end}")
    print("-" * 80)

    collector = TennetCollector(
        api_key=api_key,
        retry_config=RetryConfig(max_attempts=3),
        circuit_breaker_config=CircuitBreakerConfig(failure_threshold=3),
    )

    print("\nFetching data...")
    dataset = await collector.collect(start, end)

    if dataset:
        print(f"\n[SUCCESS] Collection successful!")
        print(f"Data points collected: {dataset.metadata['data_points']}")

        # Count non-zero prices
        price_data = dataset.data["imbalance_price"]
        non_zero_prices = sum(1 for p in price_data.values() if p != 0.0)
        print(f"Non-zero price entries: {non_zero_prices}")
        print(f"Zero price entries: {len(price_data) - non_zero_prices}")

        # Show all data points with non-zero prices
        print(f"\nAll entries with imbalance prices (15-min intervals):")
        print(f"{'Timestamp':<30} {'Price (EUR/MWh)':<18} {'Balance (MW)':<15} {'Direction':<10}")
        print("-" * 80)

        for timestamp in sorted(dataset.data["imbalance_price"].keys()):
            price = dataset.data["imbalance_price"][timestamp]
            if price != 0.0:  # Only show non-zero prices
                balance = dataset.data["balance_delta"][timestamp]
                direction = dataset.data["direction"][timestamp]
                print(f"{timestamp:<30} {price:<18.2f} {balance:<15.2f} {direction:<10}")

        print(f"\nSample balance delta entries (minute-by-minute):")
        print(f"{'Timestamp':<30} {'Price (EUR/MWh)':<18} {'Balance (MW)':<15} {'Direction':<10}")
        print("-" * 80)

        count = 0
        for timestamp in sorted(dataset.data["balance_delta"].keys()):
            if count >= 20:
                break
            price = dataset.data["imbalance_price"][timestamp]
            balance = dataset.data["balance_delta"][timestamp]
            direction = dataset.data["direction"][timestamp]
            print(f"{timestamp:<30} {price:<18.2f} {balance:<15.2f} {direction:<10}")
            count += 1

        return True
    else:
        print("\n[FAILED] Collection failed")
        return False


if __name__ == "__main__":
    success = asyncio.run(test_tennet_detailed())
    exit(0 if success else 1)
