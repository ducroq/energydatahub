"""
Test TenneT collector with API key
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


async def test_tennet_collector():
    """Test the TenneT collector with the provided API key."""
    # Set Windows event loop policy if needed
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Load API key from secrets.ini
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    config = load_secrets(project_root, 'secrets.ini')
    api_key = config.get('api_keys', 'tennet')

    # Setup time range - TenneT data has a delay, use yesterday's data
    amsterdam_tz = ZoneInfo("Europe/Amsterdam")
    end = datetime.now(amsterdam_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=1)  # Yesterday (full day)

    print(f"Testing TenneT collector")
    print(f"Time range: {start} to {end}")
    print(f"API key: {api_key[:8]}...{api_key[-8:]}")
    print("-" * 60)

    # Create collector
    collector = TennetCollector(
        api_key=api_key,
        retry_config=RetryConfig(max_attempts=3),
        circuit_breaker_config=CircuitBreakerConfig(failure_threshold=3),
    )

    # Fetch data
    print("\nFetching data...")
    dataset = await collector.collect(start, end)

    if dataset:
        print(f"\n[SUCCESS] Collection successful!")
        print(f"Data points collected: {dataset.metadata['data_points']}")
        print(f"Source: {dataset.metadata['source']}")
        print(f"Units: {dataset.metadata['units']}")

        # Display first 10 values
        print(f"\nFirst 10 data points:")
        print(f"{'Timestamp':<30} {'Price (€/MWh)':<15} {'Balance (MW)':<15} {'Direction':<10}")
        print("-" * 75)

        count = 0
        for timestamp in sorted(dataset.data["imbalance_price"].keys()):
            if count >= 10:
                break
            price = dataset.data["imbalance_price"][timestamp]
            balance = dataset.data["balance_delta"][timestamp]
            direction = dataset.data["direction"][timestamp]
            print(f"{timestamp:<30} {price:<15.2f} {balance:<15.2f} {direction:<10}")
            count += 1

        # Check metrics
        metrics = collector.get_metrics(limit=1)
        if metrics:
            print(f"\nCollection metrics:")
            print(f"  Duration: {metrics[0].duration_seconds:.2f}s")
            print(f"  Status: {metrics[0].status.value}")
            print(f"  Attempts: {metrics[0].attempt_count}")

        return True
    else:
        print("\n[FAILED] Collection failed")
        metrics = collector.get_metrics(limit=1)
        if metrics and metrics[0].errors:
            print(f"Errors: {metrics[0].errors}")
        return False


if __name__ == "__main__":
    success = asyncio.run(test_tennet_collector())
    exit(0 if success else 1)
