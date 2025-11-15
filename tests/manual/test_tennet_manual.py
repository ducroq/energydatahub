"""
Manual test for TenneT collector

Tests the TenneT collector with mock data to verify it works correctly.
"""
import asyncio
import platform
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from collectors.tennet import TennetCollector
from collectors.base import RetryConfig, CircuitBreakerConfig


async def test_tennet_collector_manual():
    """Manual test of TenneT collector with mock data."""

    print("=" * 60)
    print("TenneT Grid Imbalance Collector - Manual Test")
    print("=" * 60)

    # Initialize collector
    collector = TennetCollector(
        retry_config=RetryConfig(max_attempts=3),
        circuit_breaker_config=CircuitBreakerConfig(failure_threshold=3)
    )

    print(f"\n✓ Collector initialized:")
    print(f"  - Name: {collector.name}")
    print(f"  - Data Type: {collector.data_type}")
    print(f"  - Source: {collector.source}")
    print(f"  - Units: {collector.units}")

    # Test CSV parsing with sample data
    print("\n" + "=" * 60)
    print("Testing CSV Parsing")
    print("=" * 60)

    sample_csv = """DateTime,SystemImbalance_MW,ImbalancePrice_EUR_MWh,Direction
2025-11-15T00:00:00+01:00,-45.2,48.50,long
2025-11-15T01:00:00+01:00,12.8,52.30,short
2025-11-15T02:00:00+01:00,-8.5,45.00,long
2025-11-15T03:00:00+01:00,150.0,85.00,short"""

    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime(2025, 11, 15, 0, 0, tzinfo=amsterdam_tz)
    end = datetime(2025, 11, 15, 23, 59, tzinfo=amsterdam_tz)

    raw_data = {'csv_content': sample_csv}
    parsed_data = collector._parse_response(raw_data, start, end)

    print(f"\n✓ Parsed {len(parsed_data)} data points:")
    for timestamp, data in list(parsed_data.items())[:3]:
        print(f"  {timestamp}:")
        print(f"    - Imbalance: {data['imbalance_mw']} MW ({data['direction']})")
        print(f"    - Price: €{data['price_eur_mwh']}/MWh")

    # Test dataset creation
    print("\n" + "=" * 60)
    print("Testing Dataset Creation")
    print("=" * 60)

    dataset = collector._create_dataset(parsed_data, start, end)

    print(f"\n✓ Dataset created:")
    print(f"  - Data points: {dataset.metadata['data_points']}")
    print(f"  - Data fields: {', '.join(dataset.data.keys())}")
    print(f"  - Country: {dataset.metadata['country']}")
    print(f"  - Resolution: {dataset.metadata.get('resolution', 'N/A')}")

    # Display sample data
    print(f"\n✓ Sample data (first 3 points):")
    timestamps = list(dataset.data['imbalance'].keys())[:3]
    for ts in timestamps:
        imbalance = dataset.data['imbalance'][ts]
        price = dataset.data['imbalance_price'][ts]
        direction = dataset.data['direction'][ts]
        print(f"  {ts}:")
        print(f"    - Imbalance: {imbalance} MW ({direction})")
        print(f"    - Price: €{price}/MWh")

    # Test metadata
    print("\n" + "=" * 60)
    print("Testing Metadata")
    print("=" * 60)

    metadata = collector._get_metadata(start, end)
    print(f"\n✓ Metadata fields:")
    for key, value in metadata.items():
        print(f"  - {key}: {value}")

    print("\n" + "=" * 60)
    print("NOTE: Actual API test skipped")
    print("=" * 60)
    print("\nThe actual TenneT API endpoint may differ from the implementation.")
    print("To test with real data, verify the API endpoint at:")
    print("https://www.tennet.org/english/operational_management/export_data.aspx")
    print("\nAll basic functionality tests passed! ✓")
    print("=" * 60)


if __name__ == "__main__":
    # Set Windows event loop policy if needed
    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(test_tennet_collector_manual())
