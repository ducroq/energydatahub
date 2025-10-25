"""
Base Collector Class for Energy Data Hub
-----------------------------------------
Provides abstract base class for all data collectors with built-in retry logic,
error handling, structured logging, and data validation.

File: collectors/base.py
Created: 2025-10-25
Author: Energy Data Hub Project

Description:
    Abstract base class that all data collectors (energy prices, weather, etc.)
    inherit from. Provides common functionality:
    - Automatic retry with exponential backoff
    - Structured logging with correlation IDs
    - Data validation and normalization
    - Error handling and reporting
    - Performance metrics

Usage:
    from collectors.base import BaseCollector, RetryConfig
    from datetime import datetime

    class MyCollector(BaseCollector):
        def _fetch_raw_data(self, start_time, end_time):
            # Implement API call
            return api_response

        def _parse_response(self, raw_data, start_time, end_time):
            # Parse API response to dict
            return {'timestamp': value, ...}

    collector = MyCollector(name='MyAPI', retry_config=RetryConfig(max_attempts=3))
    data = await collector.collect(start_time, end_time)
"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Optional, Any, List
from enum import Enum
import uuid

from utils.data_types import EnhancedDataSet
from utils.timezone_helpers import normalize_timestamp_to_amsterdam


class CollectorStatus(Enum):
    """Status of a data collection attempt."""
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"  # Some data retrieved but incomplete
    SKIPPED = "skipped"


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    initial_delay: float = 1.0  # seconds
    max_delay: float = 60.0  # seconds
    exponential_base: float = 2.0
    jitter: bool = True  # Add randomness to prevent thundering herd


@dataclass
class CollectionMetrics:
    """Metrics from a data collection attempt."""
    collection_id: str
    collector_name: str
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    status: CollectorStatus
    attempt_count: int
    data_points_collected: int
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class BaseCollector(ABC):
    """
    Abstract base class for all data collectors.

    Provides common functionality:
    - Retry logic with exponential backoff
    - Structured logging
    - Error handling
    - Data validation
    - Performance metrics
    """

    def __init__(
        self,
        name: str,
        data_type: str,
        source: str,
        units: str,
        retry_config: Optional[RetryConfig] = None,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize base collector.

        Args:
            name: Collector name (e.g., "ElspotCollector")
            data_type: Type of data (e.g., "energy_price", "weather")
            source: Data source name (e.g., "Nord Pool API")
            units: Data units (e.g., "EUR/MWh")
            retry_config: Retry configuration
            logger: Logger instance (creates new if None)
        """
        self.name = name
        self.data_type = data_type
        self.source = source
        self.units = units
        self.retry_config = retry_config or RetryConfig()
        self.logger = logger or logging.getLogger(f"collectors.{name}")

        # Metrics tracking
        self._metrics_history: List[CollectionMetrics] = []

    @abstractmethod
    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Any:
        """
        Fetch raw data from API.

        This method must be implemented by subclasses.

        Args:
            start_time: Start of time range
            end_time: End of time range
            **kwargs: Additional parameters specific to the API

        Returns:
            Raw API response (format depends on API)

        Raises:
            Exception: Any error during API call
        """
        pass

    @abstractmethod
    def _parse_response(
        self,
        raw_data: Any,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, float]:
        """
        Parse raw API response to standardized format.

        This method must be implemented by subclasses.

        Args:
            raw_data: Raw API response from _fetch_raw_data()
            start_time: Start of time range (for context)
            end_time: End of time range (for context)

        Returns:
            Dict mapping ISO timestamp strings to values
            Example: {'2025-10-25T12:00:00+02:00': 100.5}

        Raises:
            ValueError: If data cannot be parsed
        """
        pass

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """
        Get metadata for the dataset.

        Can be overridden by subclasses to add custom metadata.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Metadata dictionary
        """
        return {
            'data_type': self.data_type,
            'source': self.source,
            'units': self.units,
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'collector': self.name
        }

    def _normalize_timestamps(
        self,
        data: Dict[str, float]
    ) -> Dict[str, float]:
        """
        Normalize all timestamps to Amsterdam timezone.

        Args:
            data: Dict with timestamp strings as keys

        Returns:
            Dict with normalized timestamp strings
        """
        normalized = {}

        for timestamp_str, value in data.items():
            try:
                # Parse the timestamp
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))

                # Normalize to Amsterdam
                normalized_dt = normalize_timestamp_to_amsterdam(dt)

                # Use ISO format as key
                normalized[normalized_dt.isoformat()] = value

            except Exception as e:
                self.logger.warning(
                    f"Failed to normalize timestamp '{timestamp_str}': {e}. Keeping original."
                )
                normalized[timestamp_str] = value

        return normalized

    def _validate_data(
        self,
        data: Dict[str, float],
        start_time: datetime,
        end_time: datetime
    ) -> tuple[bool, List[str]]:
        """
        Validate collected data.

        Args:
            data: Collected data dictionary
            start_time: Expected start time
            end_time: Expected end time

        Returns:
            (is_valid, list of warnings)
        """
        warnings = []

        # Check if data is empty
        if not data:
            warnings.append("No data points collected")
            return False, warnings

        # Check for None values
        none_count = sum(1 for v in data.values() if v is None)
        if none_count > 0:
            warnings.append(f"{none_count} data points have None values")

        # Check data point count
        if len(data) < 2:
            warnings.append(f"Only {len(data)} data points collected (expected more)")

        # Check for timezone issues
        from utils.timezone_helpers import validate_timestamp_format
        malformed = [ts for ts in data.keys() if not validate_timestamp_format(ts)]
        if malformed:
            warnings.append(f"Found {len(malformed)} malformed timestamps")
            for ts in malformed[:3]:  # Show first 3
                warnings.append(f"  Malformed: {ts}")

        return len(warnings) == 0, warnings

    async def _retry_with_backoff(
        self,
        func,
        *args,
        **kwargs
    ) -> Any:
        """
        Execute function with retry and exponential backoff.

        Args:
            func: Async function to execute
            *args: Positional arguments for func
            **kwargs: Keyword arguments for func

        Returns:
            Result from func

        Raises:
            Exception: Last exception if all retries fail
        """
        last_exception = None
        delay = self.retry_config.initial_delay

        for attempt in range(1, self.retry_config.max_attempts + 1):
            try:
                self.logger.debug(f"Attempt {attempt}/{self.retry_config.max_attempts}")
                return await func(*args, **kwargs)

            except Exception as e:
                last_exception = e
                self.logger.warning(
                    f"Attempt {attempt} failed: {type(e).__name__}: {e}"
                )

                # Don't sleep after last attempt
                if attempt < self.retry_config.max_attempts:
                    # Calculate backoff delay
                    backoff_delay = min(
                        delay * (self.retry_config.exponential_base ** (attempt - 1)),
                        self.retry_config.max_delay
                    )

                    # Add jitter to prevent thundering herd
                    if self.retry_config.jitter:
                        import random
                        backoff_delay *= (0.5 + random.random() * 0.5)

                    self.logger.info(f"Retrying in {backoff_delay:.2f} seconds...")
                    await asyncio.sleep(backoff_delay)

        # All retries exhausted
        self.logger.error(
            f"All {self.retry_config.max_attempts} attempts failed. "
            f"Last error: {type(last_exception).__name__}: {last_exception}"
        )
        raise last_exception

    async def collect(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Optional[EnhancedDataSet]:
        """
        Main collection workflow with retry, validation, and error handling.

        This is the primary method to call. It orchestrates:
        1. Data fetching (with retry)
        2. Response parsing
        3. Timestamp normalization
        4. Data validation
        5. Metrics collection

        Args:
            start_time: Start of time range
            end_time: End of time range
            **kwargs: Additional parameters for the specific collector

        Returns:
            EnhancedDataSet with collected data, or None if failed
        """
        collection_id = str(uuid.uuid4())[:8]
        start_timestamp = time.time()

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
            data_points_collected=0
        )

        try:
            # Step 1: Fetch raw data with retry
            self.logger.debug(f"[{collection_id}] Fetching raw data...")
            raw_data = await self._retry_with_backoff(
                self._fetch_raw_data,
                start_time,
                end_time,
                **kwargs
            )
            metrics.attempt_count = self.retry_config.max_attempts  # Successful after some attempts

            # Step 2: Parse response
            self.logger.debug(f"[{collection_id}] Parsing response...")
            parsed_data = self._parse_response(raw_data, start_time, end_time)

            # Step 3: Normalize timestamps
            self.logger.debug(f"[{collection_id}] Normalizing timestamps...")
            normalized_data = self._normalize_timestamps(parsed_data)

            # Step 4: Validate data
            self.logger.debug(f"[{collection_id}] Validating data...")
            is_valid, warnings = self._validate_data(normalized_data, start_time, end_time)

            for warning in warnings:
                self.logger.warning(f"[{collection_id}] {warning}")
                metrics.warnings.append(warning)

            # Step 5: Create EnhancedDataSet
            metadata = self._get_metadata(start_time, end_time)
            dataset = EnhancedDataSet(
                metadata=metadata,
                data=normalized_data
            )

            # Update metrics
            metrics.data_points_collected = len(normalized_data)
            metrics.status = CollectorStatus.PARTIAL if warnings else CollectorStatus.SUCCESS
            metrics.duration_seconds = time.time() - start_timestamp

            self.logger.info(
                f"[{collection_id}] Collection complete: "
                f"{metrics.data_points_collected} data points in "
                f"{metrics.duration_seconds:.2f}s "
                f"(status: {metrics.status.value})"
            )

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

            self._metrics_history.append(metrics)
            return None

    def get_metrics(self, limit: int = 10) -> List[CollectionMetrics]:
        """
        Get recent collection metrics.

        Args:
            limit: Maximum number of recent metrics to return

        Returns:
            List of CollectionMetrics, most recent first
        """
        return self._metrics_history[-limit:]

    def get_success_rate(self) -> float:
        """
        Calculate success rate from metrics history.

        Returns:
            Success rate as float (0.0 to 1.0)
        """
        if not self._metrics_history:
            return 0.0

        successful = sum(
            1 for m in self._metrics_history
            if m.status == CollectorStatus.SUCCESS
        )
        return successful / len(self._metrics_history)
