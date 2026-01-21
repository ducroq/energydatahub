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
from collectors.entsoe_wind import EntsoeWindCollector
from collectors.entsoe_flows import EntsoeFlowsCollector
from collectors.entsoe_load import EntsoeLoadCollector
from collectors.entsoe_generation import EntsoeGenerationCollector
from collectors.energyzero import EnergyZeroCollector
from collectors.epex import EpexCollector
from collectors.openweather import OpenWeatherCollector
from collectors.googleweather import GoogleWeatherCollector
from collectors.meteoserver import MeteoServerWeatherCollector, MeteoServerSunCollector
from collectors.luchtmeetnet import LuchtmeetnetCollector
from collectors.tennet import TennetCollector
from collectors.ned import NedCollector
from collectors.openmeteo_solar import OpenMeteoSolarCollector
from collectors.openmeteo_weather import OpenMeteoWeatherCollector
from collectors.market_proxies import MarketProxyCollector
from collectors.gie_storage import GieStorageCollector
from collectors.entsog_flows import EntsogFlowsCollector

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
    'EntsoeWindCollector',       # ENTSO-E Wind Generation Forecasts
    'EntsoeFlowsCollector',      # ENTSO-E Cross-border flows
    'EntsoeLoadCollector',       # ENTSO-E Load forecasts
    'EntsoeGenerationCollector', # ENTSO-E Generation by type (nuclear, gas)
    'EnergyZeroCollector',       # EnergyZero (NL)
    'EpexCollector',             # EPEX SPOT (via Awattar)

    # Weather collectors
    'OpenWeatherCollector',      # OpenWeather API
    'GoogleWeatherCollector',    # Google Weather API (multi-location)
    'MeteoServerWeatherCollector',  # MeteoServer weather (HARMONIE)
    'MeteoServerSunCollector',   # MeteoServer solar radiation

    # Air quality collectors
    'LuchtmeetnetCollector',     # Dutch air quality monitoring

    # Grid data collectors
    'TennetCollector',           # TenneT TSO grid imbalance

    # Dutch energy production
    'NedCollector',              # NED.nl (Nationaal Energie Dashboard)

    # Solar irradiance
    'OpenMeteoSolarCollector',   # Open-Meteo solar radiation (free)

    # Demand-side weather
    'OpenMeteoWeatherCollector', # Open-Meteo weather for demand prediction (free)

    # Market prices (carbon, gas)
    'MarketProxyCollector',      # Carbon and gas price proxies via Alpha Vantage

    # Gas data collectors
    'GieStorageCollector',       # GIE AGSI+ gas storage levels
    'EntsogFlowsCollector',      # ENTSOG gas flow data
]
