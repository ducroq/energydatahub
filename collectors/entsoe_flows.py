"""
ENTSO-E Cross-Border Physical Flows Collector
----------------------------------------------
Collects physical electricity flows between countries from ENTSO-E Transparency Platform.

File: collectors/entsoe_flows.py
Created: 2025-12-01
Author: Energy Data Hub Project

Description:
    Implements BaseCollector for ENTSO-E cross-border physical flows. Fetches
    actual and scheduled electricity flows between Netherlands and neighboring
    countries. Import/export flows directly impact local electricity prices.

    Key features:
    - Physical flows between bidding zones (MW)
    - Scheduled commercial flows
    - Net position calculation
    - Multi-border support (NL↔DE, NL↔BE, NL↔UK, NL↔DK)

Usage:
    from collectors.entsoe_flows import EntsoeFlowsCollector
    from datetime import datetime
    from zoneinfo import ZoneInfo

    collector = EntsoeFlowsCollector(api_key="your_api_key")
    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz)
    end = start + timedelta(days=1)

    data = await collector.collect(start, end)

API Documentation:
    https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
    Document type A11: Physical Flows
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
from entsoe import EntsoePandasClient
from functools import partial

from collectors.base import BaseCollector, RetryConfig, CircuitBreakerConfig
from utils.timezone_helpers import normalize_timestamp_to_amsterdam


class EntsoeFlowsCollector(BaseCollector):
    """
    Collector for ENTSO-E cross-border physical flows.

    Fetches physical electricity flows between Netherlands and neighboring
    countries. Positive values = import to NL, negative = export from NL.
    """

    # Border definitions: (from_zone, to_zone, display_name)
    # Flows are bidirectional - we fetch both directions
    NL_BORDERS = [
        ('NL', 'DE_LU', 'NL→DE'),
        ('DE_LU', 'NL', 'DE→NL'),
        ('NL', 'BE', 'NL→BE'),
        ('BE', 'NL', 'BE→NL'),
        ('NL', 'NO_2', 'NL→NO'),  # NorNed cable
        ('NO_2', 'NL', 'NO→NL'),
        ('NL', 'GB', 'NL→GB'),    # BritNed cable
        ('GB', 'NL', 'GB→NL'),
        ('NL', 'DK_1', 'NL→DK'),  # COBRAcable
        ('DK_1', 'NL', 'DK→NL'),
    ]

    # Zone names for metadata
    ZONE_NAMES = {
        'NL': 'Netherlands',
        'DE_LU': 'Germany-Luxembourg',
        'BE': 'Belgium',
        'NO_2': 'Norway (NO2)',
        'GB': 'Great Britain',
        'DK_1': 'Denmark-West',
    }

    def __init__(
        self,
        api_key: str,
        borders: Optional[List[Tuple[str, str, str]]] = None,
        retry_config: RetryConfig = None,
        circuit_breaker_config: CircuitBreakerConfig = None
    ):
        """
        Initialize ENTSO-E Flows collector.

        Args:
            api_key: ENTSO-E API key
            borders: List of (from_zone, to_zone, name) tuples. Default: all NL borders
            retry_config: Optional retry configuration
            circuit_breaker_config: Optional circuit breaker configuration
        """
        super().__init__(
            name="EntsoeFlowsCollector",
            data_type="cross_border_flows",
            source="ENTSO-E Transparency Platform API v1.3",
            units="MW",
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config
        )
        self.api_key = api_key
        self.borders = borders or self.NL_BORDERS

        border_names = [b[2] for b in self.borders]
        self.logger.info(f"Initialized for borders: {', '.join(border_names)}")

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Dict[str, pd.Series]:
        """
        Fetch cross-border physical flows from ENTSO-E.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict mapping border names to pandas Series with flow data

        Raises:
            Exception: If API call fails
        """
        self.logger.debug(f"Fetching cross-border flows for {len(self.borders)} borders")

        # Convert to pandas Timestamp and UTC for API
        start_timestamp = pd.Timestamp(start_time).tz_convert('UTC')
        end_timestamp = pd.Timestamp(end_time).tz_convert('UTC')

        self.logger.debug(f"Query range: {start_timestamp} to {end_timestamp} (UTC)")

        # Create client
        client = EntsoePandasClient(api_key=self.api_key)

        # Fetch data for each border
        results = {}
        loop = asyncio.get_running_loop()

        for from_zone, to_zone, border_name in self.borders:
            try:
                self.logger.debug(f"Fetching flow {border_name}")

                # query_crossborder_flows returns physical flows
                query_func = partial(
                    client.query_crossborder_flows,
                    country_code_from=from_zone,
                    country_code_to=to_zone,
                    start=start_timestamp,
                    end=end_timestamp
                )

                # Execute in thread pool to not block event loop
                data = await loop.run_in_executor(None, query_func)

                if data is not None and not data.empty:
                    results[border_name] = data
                    self.logger.debug(f"{border_name}: Got {len(data)} data points")
                else:
                    self.logger.warning(f"{border_name}: No flow data returned")

            except Exception as e:
                self.logger.warning(f"{border_name}: Failed to fetch - {e}")
                # Continue with other borders
                continue

        if not results:
            raise ValueError("No cross-border flow data returned from any border")

        return results

    def _parse_response(
        self,
        raw_data: Dict[str, pd.Series],
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Dict[str, Any]]:
        """
        Parse ENTSO-E flow response to standardized format.

        Also calculates net flows per country pair and total net position.

        Args:
            raw_data: Dict of border_name -> Series
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict with structure:
            {
                'flows': {
                    '2025-12-01T00:00:00+01:00': {
                        'NL→DE': 500.0,
                        'DE→NL': 800.0,
                        'NL_DE_net': 300.0,  # positive = import to NL
                        ...
                        'total_net_position': 450.0  # total net import
                    },
                    ...
                },
                'summary': {
                    'borders': ['NL→DE', 'DE→NL', ...],
                    'avg_net_position': 380.5
                }
            }
        """
        # First, collect all flows by timestamp
        all_flows = {}

        for border_name, series in raw_data.items():
            if isinstance(series, pd.DataFrame):
                # If DataFrame, take first column
                series = series.iloc[:, 0]

            for timestamp, value in series.items():
                dt = timestamp.to_pydatetime()

                # Filter to requested time range
                if start_time <= dt < end_time:
                    amsterdam_dt = normalize_timestamp_to_amsterdam(dt)
                    ts_key = amsterdam_dt.isoformat()

                    if ts_key not in all_flows:
                        all_flows[ts_key] = {}

                    if pd.notna(value):
                        all_flows[ts_key][border_name] = float(value)

        # Calculate net flows for each timestamp
        parsed_flows = {}
        net_positions = []

        for ts_key, flows in all_flows.items():
            flow_data = flows.copy()

            # Calculate net per country pair
            # Convention: positive = import to NL
            pairs = [('DE', 'DE_LU'), ('BE', 'BE'), ('NO', 'NO_2'), ('GB', 'GB'), ('DK', 'DK_1')]

            total_net = 0.0

            for short_name, zone_code in pairs:
                export_key = f'NL→{short_name}'
                import_key = f'{short_name}→NL'

                export_val = flows.get(export_key, 0.0)
                import_val = flows.get(import_key, 0.0)

                if export_val or import_val:
                    net = import_val - export_val
                    flow_data[f'NL_{short_name}_net'] = round(net, 1)
                    total_net += net

            flow_data['total_net_position'] = round(total_net, 1)
            net_positions.append(total_net)

            parsed_flows[ts_key] = flow_data

        # Build result with flows and summary
        result = {
            'flows': parsed_flows,
            'summary': {
                'borders': list(raw_data.keys()),
                'data_points': len(parsed_flows),
                'avg_net_position': round(sum(net_positions) / len(net_positions), 1) if net_positions else 0.0
            }
        }

        self.logger.debug(f"Parsed {len(parsed_flows)} flow timestamps, avg net: {result['summary']['avg_net_position']} MW")

        return result

    def _normalize_timestamps(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Timestamps already normalized in _parse_response."""
        return data

    def _validate_data(
        self,
        data: Dict[str, Any],
        start_time: datetime,
        end_time: datetime
    ) -> tuple[bool, List[str]]:
        """
        Validate cross-border flow data.

        Args:
            data: Parsed flow data with 'flows' and 'summary' keys
            start_time: Expected start time
            end_time: Expected end time

        Returns:
            (is_valid, list of warnings)
        """
        warnings = []

        flows = data.get('flows', {})
        summary = data.get('summary', {})

        if not flows:
            warnings.append("No flow data collected")
            return False, warnings

        # Check data point count
        if len(flows) < 12:
            warnings.append(f"Only {len(flows)} data points (expected at least 12)")

        # Check that we have at least some borders
        borders = summary.get('borders', [])
        if len(borders) < 2:
            warnings.append(f"Only {len(borders)} borders returned (expected more)")

        return len(warnings) == 0, warnings

    def _get_metadata(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Get metadata for cross-border flows dataset."""
        metadata = super()._get_metadata(start_time, end_time)

        metadata.update({
            'borders': [b[2] for b in self.borders],
            'zones': self.ZONE_NAMES,
            'resolution': 'hourly',
            'sign_convention': 'positive = import to NL, negative = export from NL',
            'api_version': 'v1.3',
            'description': 'Cross-border physical electricity flows from ENTSO-E'
        })

        return metadata


# Example usage
async def main():
    """Example usage of EntsoeFlowsCollector."""
    import os
    from configparser import ConfigParser
    from zoneinfo import ZoneInfo
    from datetime import timedelta

    script_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_file = os.path.join(script_dir, '..', 'secrets.ini')

    config = ConfigParser()
    config.read(secrets_file)
    api_key = config.get('api_keys', 'entsoe')

    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    start = datetime.now(amsterdam_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)

    collector = EntsoeFlowsCollector(api_key=api_key)
    dataset = await collector.collect(start, end)

    if dataset:
        flows = dataset.data.get('flows', {})
        summary = dataset.data.get('summary', {})
        print(f"Collected flows for {len(summary.get('borders', []))} borders")
        print(f"Average net position: {summary.get('avg_net_position', 0)} MW")

        for ts, flow_data in list(flows.items())[:3]:
            print(f"\n{ts}:")
            for k, v in flow_data.items():
                print(f"  {k}: {v} MW")
    else:
        print("Collection failed")


if __name__ == "__main__":
    asyncio.run(main())
