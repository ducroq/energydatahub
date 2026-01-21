"""
ENTSOG Gas Flows Collector
--------------------------
Fetches European gas flow data from the ENTSOG Transparency Platform.

File: collectors/entsog_flows.py
Created: 2025-01-19

Description:
    Collects gas flow data from ENTSOG (European Network of Transmission
    System Operators for Gas) Transparency Platform.

    Gas flow data is important for electricity price prediction because:
    - Gas-fired power plants set the marginal price ~40% of the time in NL
    - Import/export flows affect local gas availability and prices
    - Flow disruptions can cause price spikes in electricity markets

    Data Source:
    - ENTSOG Transparency Platform REST API
    - Public API, no authentication required
    - https://transparency.entsog.eu/

Usage:
    from collectors.entsog_flows import EntsogFlowsCollector

    collector = EntsogFlowsCollector(country_code='NL')
    data = await collector.collect(start_time, end_time)
"""

import asyncio
import aiohttp
import logging
from datetime import datetime
from typing import Dict, Optional, Any, List
from zoneinfo import ZoneInfo

from collectors.base import BaseCollector, RetryConfig, CircuitBreakerConfig


class EntsogFlowsCollector(BaseCollector):
    """
    Collector for European gas flow data from ENTSOG Transparency Platform.

    Uses the public ENTSOG REST API (no authentication required).
    """

    BASE_URL = "https://transparency.entsog.eu/api/v1"

    # Operator keys for Dutch gas interconnection points
    # See: https://transparency.entsog.eu/operatorsList
    NL_OPERATORS = {
        'gasunie': '21X-NL-A-A0A0A-Z',  # Gasunie Transport Services
    }

    # Country to operator zone mapping
    COUNTRY_ZONES = {
        'NL': 'NL',
        'DE': 'DE',
        'BE': 'BE',
        'UK': 'UK',
        'NO': 'NO',
    }

    def __init__(
        self,
        country_code: str = 'NL',
        retry_config: Optional[RetryConfig] = None,
        circuit_breaker_config: Optional[CircuitBreakerConfig] = None
    ):
        """
        Initialize ENTSOG Flows collector.

        Args:
            country_code: ISO country code (default: 'NL')
            retry_config: Optional retry configuration
            circuit_breaker_config: Optional circuit breaker configuration
        """
        super().__init__(
            name="EntsogFlowsCollector",
            data_type="gas_flows",
            source="ENTSOG Transparency Platform",
            units="kWh/d",
            retry_config=retry_config,
            circuit_breaker_config=circuit_breaker_config
        )

        self.country_code = country_code.upper()

    async def _fetch_raw_data(
        self,
        start_time: datetime,
        end_time: datetime,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Fetch gas flow data from ENTSOG API.

        Args:
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict with API response data
        """
        self.logger.debug(f"Fetching ENTSOG flow data for {self.country_code}")

        # Format dates for API (YYYY-MM-DD)
        from_date = start_time.strftime('%Y-%m-%d')
        to_date = end_time.strftime('%Y-%m-%d')

        # Build query parameters
        params = {
            'from': from_date,
            'to': to_date,
            'indicator': 'Physical Flow',
            'periodType': 'day',
            'timezone': 'CET',
            'limit': -1,  # No limit
        }

        # Filter by country zone if available
        if self.country_code in self.COUNTRY_ZONES:
            params['operatorCountry'] = self.country_code

        url = f"{self.BASE_URL}/operationaldata"

        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    error_text = await response.text()
                    raise ValueError(
                        f"ENTSOG API error {response.status}: {error_text[:200]}"
                    )

                data = await response.json()

        # Extract operational data (API uses lowercase 'operationaldata')
        records = data.get('operationaldata', [])
        self.logger.info(f"ENTSOG flows: Retrieved {len(records)} records")

        return {'records': records, 'meta': data.get('meta', {})}

    def _parse_response(
        self,
        raw_data: Dict[str, Any],
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """
        Parse ENTSOG API response to standardized format.

        Aggregates entry and exit flows by timestamp.

        Args:
            raw_data: API response dict
            start_time: Start of time range
            end_time: End of time range

        Returns:
            Dict mapping ISO timestamp strings to aggregated flow data
        """
        records = raw_data.get('records', [])

        if not records:
            self.logger.warning("No ENTSOG flow data in response")
            return {}

        # Aggregate flows by date
        daily_flows = {}

        for record in records:
            # Get gas day
            period_from = record.get('periodFrom', '')
            if not period_from:
                continue

            # Parse the date (YYYY-MM-DD format expected)
            try:
                if 'T' in period_from:
                    date_str = period_from.split('T')[0]
                else:
                    date_str = period_from[:10]

                # Create timestamp with timezone
                dt = datetime.strptime(date_str, '%Y-%m-%d')
                dt = dt.replace(hour=0, minute=0, second=0, tzinfo=ZoneInfo('Europe/Amsterdam'))
                timestamp_str = dt.isoformat()
            except (ValueError, IndexError) as e:
                self.logger.debug(f"Skipping record with invalid date: {period_from} - {e}")
                continue

            # Initialize daily entry if needed
            if timestamp_str not in daily_flows:
                daily_flows[timestamp_str] = {
                    'entry_total_gwh': 0.0,
                    'exit_total_gwh': 0.0,
                    'entry_points': 0,
                    'exit_points': 0,
                }

            # Get flow value (in kWh/d, convert to GWh)
            value = record.get('value')
            if value is None:
                continue

            try:
                flow_kwh = float(value)
                flow_gwh = flow_kwh / 1_000_000  # Convert kWh to GWh
            except (ValueError, TypeError):
                continue

            # Determine flow direction from point type
            direction_key = record.get('directionKey', '')
            point_type = record.get('pointType', '')

            # Entry points (imports/production)
            if direction_key == 'entry' or 'Entry' in point_type:
                daily_flows[timestamp_str]['entry_total_gwh'] += flow_gwh
                daily_flows[timestamp_str]['entry_points'] += 1
            # Exit points (exports/consumption)
            elif direction_key == 'exit' or 'Exit' in point_type:
                daily_flows[timestamp_str]['exit_total_gwh'] += flow_gwh
                daily_flows[timestamp_str]['exit_points'] += 1

        # Calculate net flows and round values
        for ts, values in daily_flows.items():
            values['entry_total_gwh'] = round(values['entry_total_gwh'], 2)
            values['exit_total_gwh'] = round(values['exit_total_gwh'], 2)
            values['net_flow_gwh'] = round(
                values['entry_total_gwh'] - values['exit_total_gwh'], 2
            )

        return daily_flows

    def _validate_data(
        self,
        data: Dict[str, Any],
        start_time: datetime,
        end_time: datetime
    ) -> tuple[bool, List[str]]:
        """
        Validate gas flow data.

        Args:
            data: Parsed data dictionary
            start_time: Expected start time
            end_time: Expected end time

        Returns:
            (is_valid, list of warnings)
        """
        warnings = []

        if not data:
            warnings.append("No gas flow data collected")
            return False, warnings

        # Check for meaningful flow data
        total_entry = sum(v.get('entry_total_gwh', 0) for v in data.values())
        total_exit = sum(v.get('exit_total_gwh', 0) for v in data.values())

        if total_entry == 0 and total_exit == 0:
            warnings.append("All flow values are zero")

        # Validate data point count
        if len(data) < 1:
            warnings.append(f"Only {len(data)} data points collected")

        return len([w for w in warnings if 'No' in w]) == 0, warnings

    def _get_metadata(
        self,
        start_time: datetime,
        end_time: datetime
    ) -> Dict[str, Any]:
        """Get metadata for gas flows dataset."""
        metadata = super()._get_metadata(start_time, end_time)

        metadata.update({
            'country_code': self.country_code,
            'data_frequency': 'daily',
            'description': (
                f'Gas flow data for {self.country_code} from ENTSOG Transparency Platform. '
                'Entry flows represent gas entering the transmission system (imports, production). '
                'Exit flows represent gas leaving the system (exports, consumption).'
            ),
            'usage_notes': [
                'entry_total_gwh = total gas entering the system (GWh/day)',
                'exit_total_gwh = total gas leaving the system (GWh/day)',
                'net_flow_gwh = entry - exit (positive = net import)',
                'Data is aggregated daily in gas day format (06:00-06:00 CET)',
                'Public API - no authentication required'
            ],
            'api_documentation': 'https://transparency.entsog.eu/api/v1/documentation'
        })

        return metadata


async def get_gas_flows(
    start_time: datetime,
    end_time: datetime,
    country_code: str = 'NL'
) -> Optional[Any]:
    """
    Convenience function to fetch gas flow data.

    Args:
        start_time: Start of time range
        end_time: End of time range
        country_code: ISO country code (default: 'NL')

    Returns:
        EnhancedDataSet with flow data or None if failed
    """
    collector = EntsogFlowsCollector(country_code=country_code)
    return await collector.collect(start_time, end_time)


# Example usage
async def main():
    """Example usage of EntsogFlowsCollector."""
    from datetime import timedelta

    logging.basicConfig(level=logging.INFO)

    amsterdam_tz = ZoneInfo('Europe/Amsterdam')
    end = datetime.now(amsterdam_tz)
    start = end - timedelta(days=7)

    collector = EntsogFlowsCollector(country_code='NL')
    dataset = await collector.collect(start, end)

    if dataset:
        print("\nGas Flow Data (NL):")
        print("=" * 60)
        for ts, values in sorted(dataset.data.items()):
            print(f"\n{ts}:")
            print(f"  Entry: {values.get('entry_total_gwh', 0):.2f} GWh")
            print(f"  Exit:  {values.get('exit_total_gwh', 0):.2f} GWh")
            print(f"  Net:   {values.get('net_flow_gwh', 0):.2f} GWh")
    else:
        print("Collection failed")


if __name__ == "__main__":
    asyncio.run(main())
