"""
TenneT System Imbalance Collector
----------------------------------
Collects Dutch grid system imbalance data from TenneT TSO.

Data includes:
- System imbalance volume (MW)
- Imbalance settlement price (EUR/MWh)
- Direction (short/long)

File: collectors/tennet.py
Created: 2025-11-15
Author: Energy Data Hub Project
"""

import asyncio
import csv
import logging
import time
import uuid
from datetime import datetime
from io import StringIO
from typing import Any, Dict, List, Optional

import aiohttp

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
    """Collector for TenneT system imbalance data."""

    BASE_URL = "https://www.tennet.org/english/operational_management/export_data.aspx"

    def __init__(
        self,
        retry_config: Optional[RetryConfig] = None,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None,
    ):
        """
        Initialize TenneT collector.

        Args:
            retry_config: Optional retry configuration
            circuit_breaker_config: Optional circuit breaker configuration
        """
        super().__init__(
            name="TennetCollector",
            data_type="grid_imbalance",
            source="TenneT TSO",
            units="MW",
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config,
        )

    async def _fetch_raw_data(
        self, start_time: datetime, end_time: datetime, **kwargs
    ) -> dict:
        """
        Fetch raw CSV data from TenneT API.

        Args:
            start_time: Start of data range (Amsterdam timezone)
            end_time: End of data range (Amsterdam timezone)
            **kwargs: Additional parameters

        Returns:
            Dict with raw CSV data

        Raises:
            aiohttp.ClientError: If API request fails
        """
        params = {
            "DataType": "SystemImbalance",
            "StartDate": start_time.strftime("%Y-%m-%d"),
            "EndDate": end_time.strftime("%Y-%m-%d"),
            "Output": "csv",
        }

        self.logger.debug(
            f"Fetching TenneT data from {start_time.date()} to {end_time.date()}"
        )

        async with aiohttp.ClientSession() as session:
            async with session.get(self.BASE_URL, params=params) as response:
                response.raise_for_status()
                csv_data = await response.text()

                self.logger.info(f"Fetched TenneT data: {len(csv_data)} bytes")

                return {"csv_content": csv_data}

    def _parse_response(
        self, raw_data: dict, start_time: datetime, end_time: datetime
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse TenneT CSV response into normalized dict.

        Args:
            raw_data: Dict containing 'csv_content' field
            start_time: Start of data range
            end_time: End of data range

        Returns:
            Dict with timestamps as keys, imbalance data as values
            {
                '2025-11-15T00:00:00+01:00': {
                    'imbalance_mw': -45.2,
                    'price_eur_mwh': 48.50,
                    'direction': 'long'
                },
                ...
            }

        Raises:
            ValueError: If CSV parsing fails
        """
        csv_content = raw_data["csv_content"]
        reader = csv.DictReader(StringIO(csv_content))

        parsed_data = {}

        for row in reader:
            try:
                timestamp = row["DateTime"]  # Already in Amsterdam timezone

                # Parse values
                imbalance = float(row["SystemImbalance_MW"])
                price = float(row["ImbalancePrice_EUR_MWh"])
                direction = "long" if imbalance < 0 else "short"

                parsed_data[timestamp] = {
                    "imbalance_mw": imbalance,
                    "price_eur_mwh": price,
                    "direction": direction,
                }
            except (KeyError, ValueError) as e:
                self.logger.warning(f"Failed to parse row: {row}. Error: {e}")
                continue

        self.logger.info(f"Parsed {len(parsed_data)} TenneT data points")

        if not parsed_data:
            raise ValueError("No valid data points parsed from TenneT CSV")

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
        for imbalance, price, and direction.

        Args:
            parsed_data: Dict of timestamp -> imbalance data
            start_time: Start of data range
            end_time: End of data range

        Returns:
            EnhancedDataSet with metadata and data
        """
        metadata = {
            "data_type": "grid_imbalance",
            "source": "TenneT TSO",
            "units": "MW",
            "country": "NL",
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "data_points": len(parsed_data),
            "collection_timestamp": datetime.now().isoformat(),
        }

        # Convert to simplified format for chart display
        # Separate imbalance volume from price
        imbalance_data = {ts: data["imbalance_mw"] for ts, data in parsed_data.items()}
        price_data = {ts: data["price_eur_mwh"] for ts, data in parsed_data.items()}
        direction_data = {ts: data["direction"] for ts, data in parsed_data.items()}

        dataset = EnhancedDataSet(
            metadata=metadata,
            data={
                "imbalance": imbalance_data,
                "imbalance_price": price_data,
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
                "resolution": "hourly",
                "data_fields": ["imbalance_mw", "price_eur_mwh", "direction"],
            }
        )

        return metadata


# Example usage
async def main():
    """Example usage of TennetCollector."""
    from datetime import timedelta
    from zoneinfo import ZoneInfo
    import platform

    # Set Windows event loop policy if needed
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    # Setup time range
    amsterdam_tz = ZoneInfo("Europe/Amsterdam")
    start = datetime.now(amsterdam_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)

    # Create collector and fetch data
    collector = TennetCollector(
        retry_config=RetryConfig(max_attempts=3),
        circuit_breaker_config=CircuitBreakerConfig(failure_threshold=3),
    )
    dataset = await collector.collect(start, end)

    if dataset:
        print(f"Collected {dataset.metadata['data_points']} data points")
        print(f"\nFirst 5 imbalance values:")
        for timestamp, value in list(dataset.data["imbalance"].items())[:5]:
            price = dataset.data["imbalance_price"][timestamp]
            direction = dataset.data["direction"][timestamp]
            print(f"  {timestamp}: {value} MW ({direction}) @ {price} EUR/MWh")

        # Check metrics
        metrics = collector.get_metrics(limit=1)
        print(f"\nCollection metrics:")
        print(f"  Duration: {metrics[0].duration_seconds:.2f}s")
        print(f"  Status: {metrics[0].status.value}")
    else:
        print("Collection failed")


if __name__ == "__main__":
    asyncio.run(main())
