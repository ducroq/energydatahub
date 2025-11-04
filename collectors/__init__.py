"""
Data Collectors for Energy Data Hub

Provides base classes and implementations for collecting data from various APIs.

All collectors inherit from BaseCollector and provide:
- Automatic retry with exponential backoff
- Structured logging with correlation IDs
- Timestamp normalization to Europe/Amsterdam
- Data validation and quality checks
- Performance metrics tracking
"""

from collectors.base import (
    BaseCollector,
    RetryConfig,
    CircuitBreakerConfig,
    CircuitState,
    CollectorStatus
)
from collectors.elspot import ElspotCollector
from collectors.entsoe import EntsoeCollector
from collectors.energyzero import EnergyZeroCollector
from collectors.epex import EpexCollector
from collectors.openweather import OpenWeatherCollector
from collectors.googleweather import GoogleWeatherCollector
from collectors.meteoserver import MeteoServerWeatherCollector, MeteoServerSunCollector
from collectors.luchtmeetnet import LuchtmeetnetCollector

__all__ = [
    # Base classes
    'BaseCollector',
    'RetryConfig',
    'CircuitBreakerConfig',
    'CircuitState',
    'CollectorStatus',

    # Energy price collectors
    'ElspotCollector',           # Nord Pool Elspot
    'EntsoeCollector',           # ENTSO-E Transparency Platform
    'EnergyZeroCollector',       # EnergyZero (NL)
    'EpexCollector',             # EPEX SPOT (via Awattar)

    # Weather collectors
    'OpenWeatherCollector',      # OpenWeather API
    'GoogleWeatherCollector',    # Google Weather API (multi-location)
    'MeteoServerWeatherCollector',  # MeteoServer weather (HARMONIE)
    'MeteoServerSunCollector',   # MeteoServer solar radiation

    # Air quality collectors
    'LuchtmeetnetCollector',     # Dutch air quality monitoring
]
