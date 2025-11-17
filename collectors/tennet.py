"""
TenneT System Imbalance Collector
----------------------------------
Collects Dutch grid system imbalance data from TenneT TSO using the official
tenneteu-py library and the new tennet.eu API.

Data includes:
- System imbalance volume (MW)
- Imbalance settlement price (EUR/MWh)
- Balance delta information

File: collectors/tennet.py
Created: 2025-11-15
Updated: 2025-11-15 (migrated to tennet.eu API)
Author: Energy Data Hub Project

API Documentation: https://developer.tennet.eu/
API Registration: https://www.tennet.eu/registration-api-token
"""

import asyncio
import logging
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from tenneteu import TenneTeuClient

from collectors.base import (
    BaseCollector,
    CircuitBreakerConfig,
    CollectionMetrics,
    CollectorStatus,
    RetryConfig,
)
from utils.data_types import EnhancedDataSet
from utils.timezone_helpers import normalize_timestamp_to_amsterdam

logger = logging.getLogger(__name__)


class TennetCollector(BaseCollector):
    """
    Collector for TenneT system imbalance data using the official tennet.eu API.

    Uses the tenneteu-py library to access TenneT's settlement prices and
    balance delta data.

    Note: Requires API key from https://www.tennet.eu/registration-api-token
    """

    def __init__(
        self,
        api_key: str,
        retry_config: Optional[RetryConfig] = None,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
    ):
        """
        Initialize TenneT collector.

        Args:
            api_key: TenneT API key (register at tennet.eu)
            retry_config: Optional retry configuration
            circuit_breaker_config: Optional circuit breaker configuration
        """
        super().__init__(
            name="TennetCollector",
            data_type="grid_imbalance",
            source="TenneT TSO (tennet.eu API)",
            units="MW",
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config,
        )
        self.api_key = api_key
        self.client = TenneTeuClient(api_key=api_key)

    async def _fetch_raw_data(
        self, start_time: datetime, end_time: datetime, **kwargs
    ) -> Dict[str, pd.DataFrame]:
        """
        Fetch raw data from TenneT API using tenneteu-py library.

        Args:
            start_time: Start of data range (Amsterdam timezone)
            end_time: End of data range (Amsterdam timezone)
            **kwargs: Additional parameters

        Returns:
            Dict with DataFrames for settlement prices and balance delta

        Raises:
            Exception: If API request fails
        """
        self.logger.debug(
            f"Fetching TenneT data from {start_time.date()} to {end_time.date()}"
        )

        # Run the synchronous API calls in an executor to avoid blocking
        loop = asyncio.get_event_loop()

        try:
            # Fetch settlement prices (imbalance prices)
            settlement_prices_df = await loop.run_in_executor(
                None,
                self.client.query_settlement_prices,
                start_time,
                end_time
            )

            # Fetch balance delta data
            balance_delta_df = await loop.run_in_executor(
                None,
                self.client.query_balance_delta,
                start_time,
                end_time
            )

            self.logger.info(
                f"Fetched TenneT data: {len(settlement_prices_df)} settlement price records, "
                f"{len(balance_delta_df)} balance delta records"
            )

            return {
                'settlement_prices': settlement_prices_df,
                'balance_delta': balance_delta_df
            }

        except Exception as e:
            self.logger.error(f"Failed to fetch TenneT data: {e}")
            raise

    def _parse_response(
        self, raw_data: Dict[str, pd.DataFrame], start_time: datetime, end_time: datetime
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse TenneT API response DataFrames into normalized dict.

        Args:
            raw_data: Dict containing 'settlement_prices' and 'balance_delta' DataFrames
            start_time: Start of data range
            end_time: End of data range

        Returns:
            Dict with timestamps as keys, imbalance data as values
            {
                '2025-11-15T00:00:00+01:00': {
                    'imbalance_price': 48.50,
                    'balance_delta': -45.2,
                    'direction': 'long'
                },
                ...
            }

        Raises:
            ValueError: If data parsing fails
        """
        settlement_prices_df = raw_data.get('settlement_prices')
        balance_delta_df = raw_data.get('balance_delta')

        if settlement_prices_df is None or settlement_prices_df.empty:
            raise ValueError("No settlement prices data received from TenneT API")

        parsed_data = {}

        # Parse settlement prices (imbalance prices per balancing time unit)
        # The timestamp is in the DataFrame index, not in a column
        for timestamp, row in settlement_prices_df.iterrows():
            try:
                # Timestamp is the index
                if isinstance(timestamp, pd.Timestamp):
                    timestamp_str = timestamp.isoformat()
                elif isinstance(timestamp, datetime):
                    timestamp_str = timestamp.isoformat()
                else:
                    timestamp_str = str(timestamp)

                # Get imbalance price from shortage/surplus prices
                # Use Mid Price if available, otherwise average of shortage and surplus
                imbalance_price = None

                # Try 'Mid Price' first
                if 'Mid Price' in row.index and pd.notna(row['Mid Price']):
                    imbalance_price = float(row['Mid Price'])
                # Otherwise try Price Shortage/Surplus
                elif 'Price Shortage' in row.index and 'Price Surplus' in row.index:
                    shortage = row['Price Shortage']
                    surplus = row['Price Surplus']
                    if pd.notna(shortage) and pd.notna(surplus):
                        imbalance_price = (float(shortage) + float(surplus)) / 2
                    elif pd.notna(shortage):
                        imbalance_price = float(shortage)
                    elif pd.notna(surplus):
                        imbalance_price = float(surplus)

                # Try dispatch prices as fallback
                if imbalance_price is None:
                    if 'Price Dispatch Up' in row.index and pd.notna(row['Price Dispatch Up']):
                        imbalance_price = float(row['Price Dispatch Up'])
                    elif 'Price Dispatch Down' in row.index and pd.notna(row['Price Dispatch Down']):
                        imbalance_price = float(row['Price Dispatch Down'])

                if imbalance_price is None:
                    self.logger.debug(f"No price data available for {timestamp_str}")
                    continue

                # Initialize data entry
                if timestamp_str not in parsed_data:
                    parsed_data[timestamp_str] = {
                        'imbalance_price': imbalance_price,
                        'balance_delta': 0.0,
                        'direction': 'unknown'
                    }
                else:
                    parsed_data[timestamp_str]['imbalance_price'] = imbalance_price

            except (KeyError, ValueError, TypeError) as e:
                self.logger.warning(f"Failed to parse settlement price row: {e}")
                continue

        # Parse balance delta if available
        if balance_delta_df is not None and not balance_delta_df.empty:
            for timestamp, row in balance_delta_df.iterrows():
                try:
                    # Timestamp is the index
                    if isinstance(timestamp, pd.Timestamp):
                        timestamp_str = timestamp.isoformat()
                    elif isinstance(timestamp, datetime):
                        timestamp_str = timestamp.isoformat()
                    else:
                        timestamp_str = str(timestamp)

                    # Calculate net balance delta from IGCC (International Grid Control Cooperation)
                    # Power In IGCC = import (positive), Power Out IGCC = export (negative)
                    balance_delta = None

                    if 'Power In Igcc' in row.index and 'Power Out Igcc' in row.index:
                        power_in = row['Power In Igcc']
                        power_out = row['Power Out Igcc']
                        if pd.notna(power_in) and pd.notna(power_out):
                            # Net balance: positive = importing/short, negative = exporting/long
                            balance_delta = float(power_in) - float(power_out)

                    # Also consider activated aFRR (automatic Frequency Restoration Reserve)
                    if 'Power In Activated Afrr' in row.index and 'Power Out Activated Afrr' in row.index:
                        afrr_in = row['Power In Activated Afrr']
                        afrr_out = row['Power Out Activated Afrr']
                        if pd.notna(afrr_in) and pd.notna(afrr_out):
                            if balance_delta is None:
                                balance_delta = 0.0
                            balance_delta += float(afrr_in) - float(afrr_out)

                    if balance_delta is None:
                        continue

                    # Calculate direction from balance delta
                    # Negative = net export/oversupply (long), Positive = net import/undersupply (short)
                    direction = 'long' if balance_delta < 0 else 'short' if balance_delta > 0 else 'balanced'

                    # Update or create data entry
                    if timestamp_str in parsed_data:
                        parsed_data[timestamp_str]['balance_delta'] = balance_delta
                        parsed_data[timestamp_str]['direction'] = direction
                    else:
                        parsed_data[timestamp_str] = {
                            'imbalance_price': 0.0,
                            'balance_delta': balance_delta,
                            'direction': direction
                        }

                except (KeyError, ValueError, TypeError) as e:
                    self.logger.warning(f"Failed to parse balance delta row: {e}")
                    continue

        self.logger.info(f"Parsed {len(parsed_data)} TenneT data points")

        if not parsed_data:
            raise ValueError("No valid data points parsed from TenneT API")

        return parsed_data

    def _normalize_timestamps(
        self, data: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Normalize all timestamps to Amsterdam timezone.

        Overrides base class to handle complex data structure.

        Args:
            data: Dict with timestamp strings as keys, dicts as values

        Returns:
            Dict with normalized timestamp strings
        """
        normalized = {}

        for timestamp_str, values in data.items():
            try:
                # Parse the timestamp
                dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

                # Normalize to Amsterdam
                normalized_dt = normalize_timestamp_to_amsterdam(dt)

                # Use ISO format as key
                normalized[normalized_dt.isoformat()] = values

            except Exception as e:
                self.logger.warning(
                    f"Failed to normalize timestamp '{timestamp_str}': {e}. Keeping original."
                )
                normalized[timestamp_str] = values

        return normalized

    def _create_dataset(
        self,
        parsed_data: Dict[str, Dict[str, Any]],
        start_time: datetime,
        end_time: datetime,
    ) -> EnhancedDataSet:
        """
        Create EnhancedDataSet from parsed TenneT data.

        Converts complex per-timestamp data into separate time series
        for imbalance price, balance delta, and direction.

        Args:
            parsed_data: Dict of timestamp -> imbalance data
            start_time: Start of data range
            end_time: End of data range

        Returns:
            EnhancedDataSet with metadata and data
        """
        metadata = {
            "data_type": "grid_imbalance",
            "source": "TenneT TSO (tennet.eu API)",
            "units": "EUR/MWh (price), MW (balance)",
            "country": "NL",
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "data_points": len(parsed_data),
            "collection_timestamp": datetime.now().isoformat(),
            "api_version": "tennet.eu v1",
        }

        # Convert to simplified format for chart display
        # Separate imbalance price from balance delta
        imbalance_price_data = {ts: data["imbalance_price"] for ts, data in parsed_data.items()}
        balance_delta_data = {ts: data["balance_delta"] for ts, data in parsed_data.items()}
        direction_data = {ts: data["direction"] for ts, data in parsed_data.items()}

        dataset = EnhancedDataSet(
            metadata=metadata,
            data={
                "imbalance_price": imbalance_price_data,
                "balance_delta": balance_delta_data,
                "direction": direction_data,
            },
        )

        return dataset

    async def collect(
        self, start_time: datetime, end_time: datetime, **kwargs
    ) -> Optional[EnhancedDataSet]:
        """
        Main collection workflow for TenneT data.

        Overrides base class to handle complex data structure with
        multiple fields per timestamp.

        Args:
            start_time: Start of time range
            end_time: End of time range
            **kwargs: Additional parameters

        Returns:
            EnhancedDataSet with collected data, or None if failed
        """
        collection_id = str(uuid.uuid4())[:8]
        start_timestamp = time.time()

        # Check circuit breaker
        if not self._check_circuit_breaker():
            self.logger.warning(
                f"[{collection_id}] Collection skipped - circuit breaker OPEN"
            )
            return None

        self.logger.info(
            f"[{collection_id}] Starting collection: {start_time} to {end_time}"
        )

        metrics = CollectionMetrics(
            collection_id=collection_id,
            collector_name=self.name,
            start_time=start_time,
            end_time=end_time,
            duration_seconds=0.0,
            status=CollectorStatus.FAILED,
            attempt_count=0,
            data_points_collected=0,
        )

        try:
            # Step 1: Fetch raw data with retry
            self.logger.debug(f"[{collection_id}] Fetching raw data...")
            raw_data = await self._retry_with_backoff(
                self._fetch_raw_data, start_time, end_time, **kwargs
            )
            metrics.attempt_count = self.retry_config.max_attempts

            # Step 2: Parse response
            self.logger.debug(f"[{collection_id}] Parsing response...")
            parsed_data = self._parse_response(raw_data, start_time, end_time)

            # Step 3: Normalize timestamps
            self.logger.debug(f"[{collection_id}] Normalizing timestamps...")
            normalized_data = self._normalize_timestamps(parsed_data)

            # Step 4: Create EnhancedDataSet with complex structure
            self.logger.debug(f"[{collection_id}] Creating dataset...")
            dataset = self._create_dataset(normalized_data, start_time, end_time)

            # Update metrics
            metrics.data_points_collected = len(normalized_data)
            metrics.status = CollectorStatus.SUCCESS
            metrics.duration_seconds = time.time() - start_timestamp

            self.logger.info(
                f"[{collection_id}] Collection complete: "
                f"{metrics.data_points_collected} data points in "
                f"{metrics.duration_seconds:.2f}s "
                f"(status: {metrics.status.value})"
            )

            # Record success for circuit breaker
            self._record_success()

            self._metrics_history.append(metrics)
            return dataset

        except Exception as e:
            # Collection failed
            metrics.duration_seconds = time.time() - start_timestamp
            metrics.status = CollectorStatus.FAILED
            metrics.errors.append(f"{type(e).__name__}: {e}")

            self.logger.error(
                f"[{collection_id}] Collection failed after "
                f"{metrics.duration_seconds:.2f}s: {e}"
            )

            # Record failure for circuit breaker
            self._record_failure()

            self._metrics_history.append(metrics)
            return None

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        Get metadata for TenneT dataset.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Metadata dictionary
        """
        # Get base metadata from parent class
        metadata = super()._get_metadata(start_time, end_time)

        # Add TenneT-specific metadata
        metadata.update(
            {
                "country_code": "NL",
                "market": "transmission",
                "resolution": "PTU (15 minutes)",
                "data_fields": ["imbalance_price", "balance_delta", "direction"],
                "api_version": "tennet.eu v1",
            }
        )

        return metadata


# Example usage
async def main():
    """Example usage of TennetCollector."""
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    import platform
    import os

    # Set Windows event loop policy if needed
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Get API key from environment or config
    api_key = os.getenv("TENNET_API_KEY", "your_api_key_here")

    if api_key == "your_api_key_here":
        print("Please set TENNET_API_KEY environment variable")
        print("Register at: https://www.tennet.eu/registration-api-token")
        return

    # Setup time range
    amsterdam_tz = ZoneInfo("Europe/Amsterdam")
    start = datetime.now(amsterdam_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)

    # Create collector and fetch data
    collector = TennetCollector(
        api_key=api_key,
        retry_config=RetryConfig(max_attempts=3),
        circuit_breaker_config=CircuitBreakerConfig(failure_threshold=3),
    )
    dataset = await collector.collect(start, end)

    if dataset:
        print(f"Collected {dataset.metadata['data_points']} data points")
        print(f"\nFirst 5 values:")
        for timestamp, price in list(dataset.data["imbalance_price"].items())[:5]:
            balance = dataset.data["balance_delta"][timestamp]
            direction = dataset.data["direction"][timestamp]
            print(f"  {timestamp}: €{price}/MWh, {balance} MW ({direction})")

        # Check metrics
        metrics = collector.get_metrics(limit=1)
        print(f"\nCollection metrics:")
        print(f"  Duration: {metrics[0].duration_seconds:.2f}s")
        print(f"  Status: {metrics[0].status.value}")
    else:
        print("Collection failed")


if __name__ == "__main__":
    asyncio.run(main())
