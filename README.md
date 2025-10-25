# Energy Data Hub

A robust Python-based system for collecting, processing, and publishing energy price and weather forecast data for energy optimization applications. The system fetches data from multiple sources including ENTSO-E, Energy Zero, EPEX SPOT, Nord Pool Elspot, OpenWeather, and MeteoServer, providing comprehensive data for energy management decisions.

## 🌟 Features

- **Multi-Source Data Collection**: Automated collection from 7+ energy and weather APIs
- **Robust Architecture**: BaseCollector pattern with retry logic and circuit breakers
- **Data Validation**: Comprehensive timezone normalization and data type validation
- **Secure Publishing**: AES-CBC encryption with HMAC-SHA256 for all published data
- **Automated Workflows**: GitHub Actions for scheduled data collection and publishing (daily at 16:00 UTC)
- **High Test Coverage**: 49% code coverage with comprehensive unit and integration tests
- **Production Ready**: Circuit breaker pattern prevents API overload, caching optimizes performance

## 🌐 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Energy Data Hub (Backend)                 │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Data Collectors (BaseCollector Pattern)              │ │
│  │  ├── EnergyZeroCollector    (NL Energy Prices)        │ │
│  │  ├── EpexCollector          (EPEX SPOT)               │ │
│  │  ├── ElspotCollector        (Nord Pool)               │ │
│  │  ├── EntsoeCollector        (ENTSO-E)                 │ │
│  │  ├── OpenWeatherCollector   (Weather Data)            │ │
│  │  ├── MeteoServerCollector   (NL Weather)              │ │
│  │  └── LuchtmeetnetCollector  (Air Quality)             │ │
│  └────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Data Processing Pipeline                             │ │
│  │  ├── Timezone Normalization (Europe/Amsterdam)        │ │
│  │  ├── Data Validation & Type Conversion                │ │
│  │  ├── CombinedDataSet Creation                         │ │
│  │  └── Encryption (AES-CBC + HMAC-SHA256)               │ │
│  └────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Publishing                                           │ │
│  │  └── GitHub Pages (Public Encrypted Data)             │ │
│  └────────────────────────────────────────────────────────┘ │
└────────────────────┬───────────────────────────────────────┘
                     │
              [GitHub Pages]
                     │
         ┌───────────┴────────────┐
         │                        │
    ┌────▼─────────────┐    ┌────▼──────────┐
    │ energyDataDashboard│    │Future Consumers│
    │   (Visualization)  │    │ (Mobile/API)   │
    └────────────────────┘    └────────────────┘
```

## 📊 Published Data Endpoints

Encrypted data is automatically published to GitHub Pages daily:

- **Energy Price Forecast**: https://ducroq.github.io/energydatahub/energy_price_forecast.json
- **Weather Forecast**: https://ducroq.github.io/energydatahub/weather_forecast.json
- **Sun Forecast**: https://ducroq.github.io/energydatahub/sun_forecast.json
- **Air Quality Data**: https://ducroq.github.io/energydatahub/air_history.json

### Data Update Schedule

- **Collection Frequency**: Daily at 16:00 UTC (18:00 CEST / 17:00 CET)
- **Encryption**: All data encrypted before publishing
- **Publishing**: Current data endpoints updated on GitHub Pages

### Data Format

```json
{
  "version": "2.0",
  "entsoe": {
    "metadata": {
      "data_type": "energy_price",
      "source": "ENTSO-E Transparency Platform",
      "units": "EUR/MWh",
      "country": "NL",
      "start_time": "2025-10-25T00:00:00+02:00",
      "end_time": "2025-10-26T00:00:00+02:00"
    },
    "data": {
      "2025-10-25T00:00:00+02:00": 45.32,
      "2025-10-25T01:00:00+02:00": 42.18,
      ...
    }
  },
  "energy_zero": { ... },
  "epex": { ... }
}
```

## 🚀 Quick Start

### Local Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/ducroq/energydatahub.git
   cd energydatahub
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure secrets** (create `secrets.ini`)
   ```ini
   [api_keys]
   entsoe = YOUR_ENTSOE_API_KEY
   openweather = YOUR_OPENWEATHER_API_KEY
   meteo = YOUR_METEOSERVER_API_KEY

   [security_keys]
   encryption = YOUR_BASE64_ENCODED_ENCRYPTION_KEY
   hmac = YOUR_BASE64_ENCODED_HMAC_KEY

   [location]
   latitude = 51.9851
   longitude = 5.8987
   ```

4. **Run data collection**
   ```bash
   python data_fetcher.py
   ```

### GitHub Actions Setup

For automated daily collection, configure these repository secrets:

#### Required Secrets:
- `PAT`: GitHub Personal Access Token (repo scope)
- `ENCRYPTION_KEY`: Base64-encoded 256-bit encryption key
- `HMAC_KEY`: Base64-encoded 256-bit HMAC key
- `ENTSOE_API_KEY`: ENTSO-E Transparency Platform key
- `OPENWEATHER_API_KEY`: OpenWeather API key
- `METEO_API_KEY`: MeteoServer API key

#### Optional (for Google Drive archival):
- `GDRIVE_SERVICE_ACCOUNT_JSON`: Service account credentials
- `GDRIVE_ROOT_FOLDER_ID`: Target folder ID for backups

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage report
pytest --cov=. --cov-report=html

# Run specific test suite
pytest tests/unit/test_base_collector.py -v
```

## 🔧 Advanced Configuration

### Collector Customization

Each collector supports custom retry and circuit breaker configuration:

```python
from collectors import EpexCollector, RetryConfig, CircuitBreakerConfig

collector = EpexCollector(
    retry_config=RetryConfig(
        max_attempts=5,
        initial_delay=2.0,
        max_delay=120.0
    ),
    circuit_breaker_config=CircuitBreakerConfig(
        failure_threshold=3,
        success_threshold=2,
        timeout=60.0
    )
)
```

### Environment Variables

Alternative to `secrets.ini` for CI/CD:

```bash
export ENCRYPTION_KEY="base64_encoded_key"
export HMAC_KEY="base64_encoded_key"
export ENTSOE_API_KEY="your_api_key"
export OPENWEATHER_API_KEY="your_api_key"
export METEO_API_KEY="your_api_key"
```

## 📊 Data Sources

| Source | Data Type | Coverage | Update Frequency | Coverage |
|--------|-----------|----------|------------------|----------|
| **ENTSO-E** | Energy Prices | Europe | Hourly | Day-ahead prices |
| **Energy Zero** | Energy Prices | Netherlands | Hourly | Real-time + forecast |
| **EPEX SPOT** | Energy Prices | Central Europe | Hourly | Day-ahead auction |
| **Nord Pool Elspot** | Energy Prices | Nordic/Baltic | Hourly | Day-ahead market |
| **OpenWeather** | Weather | Global | 3-hour intervals | 5-day forecast |
| **MeteoServer** | Weather | Netherlands | Hourly | Detailed NL forecast |
| **Luchtmeetnet** | Air Quality | Netherlands | Hourly | Real-time AQI |

## 🛡️ Security & Reliability

### Encryption

- **Algorithm**: AES-CBC with 256-bit keys
- **Authentication**: HMAC-SHA256 signatures
- **Key Management**: Environment variables or encrypted config
- **Data Integrity**: Verified before decryption

### Reliability Features

- **Retry Logic**: Exponential backoff for transient failures
- **Circuit Breaker**: Prevents cascading failures (CLOSED → OPEN → HALF_OPEN)
- **Caching**: Luchtmeetnet station cache (24h) reduces API calls by 79.5x
- **Graceful Degradation**: System continues with partial data on collector failures
- **Metrics Tracking**: Success rates, error logging, performance monitoring

### Test Coverage

- **Overall**: 49% code coverage
- **Base Collector**: 94% coverage
- **Utils**: 75-91% coverage
- **177 tests**: Unit, integration, and failure scenario tests

## 🔗 Related Projects

### Energy Data Dashboard
Visual interface for the collected data:
- **Repository**: https://github.com/ducroq/energyDataDashboard
- **Live Demo**: https://your-dashboard-url.netlify.app
- **Technology**: Hugo + Plotly.js + Netlify
- **Features**:
  - Interactive price charts
  - Live Energy Zero data (10-min refresh)
  - Historical price comparisons
  - Time range selection
  - Mobile responsive

### Integration Flow

```
energyDataHub (This Repo)
    ↓ (Daily at 16:00 UTC)
GitHub Pages (Encrypted JSON)
    ↓ (Webhook trigger)
energyDataDashboard
    ↓ (Build process)
Netlify CDN (Static Site)
    ↓
End Users (Interactive Dashboard)
```

## 📈 Performance Optimizations

### Recent Improvements (Phase 6)

1. **Luchtmeetnet Caching**
   - 24-hour station list cache
   - 79.5x speedup on subsequent calls
   - 98.7% time reduction

2. **Circuit Breaker Pattern**
   - Prevents retry storms
   - Auto-recovery after timeout
   - Configurable thresholds

3. **Comprehensive Testing**
   - 49% code coverage achieved
   - Failure scenario validation
   - Integration test suite

## 🤝 Contributing

Contributions welcome! Areas for enhancement:

- Additional data collectors (new APIs)
- Enhanced data validation rules
- Performance optimizations
- Documentation improvements
- Test coverage expansion

### Development Workflow

1. Fork the repository
2. Create feature branch: `git checkout -b feature/amazing-feature`
3. Make changes with tests
4. Run test suite: `pytest`
5. Commit: `git commit -m "Add amazing feature"`
6. Push: `git push origin feature/amazing-feature`
7. Open Pull Request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Troubleshooting

### Common Issues

**Issue**: Import errors
```bash
pip install -r requirements.txt
```

**Issue**: API key errors
```bash
# Verify keys in secrets.ini or environment variables
python -c "from utils.helpers import load_secrets; print(load_secrets('.'))"
```

**Issue**: Timezone warnings
```bash
# System should auto-normalize to Europe/Amsterdam
# Check logs for timezone conversion details
```

**Issue**: Collector failures
```bash
# Check circuit breaker metrics
python -c "from collectors import EpexCollector; c = EpexCollector(); print(c.get_metrics())"
```

## 📞 Support

- **Issues**: https://github.com/ducroq/energydatahub/issues
- **Documentation**: See `/docs` folder for detailed guides
- **Test Results**: See GitHub Actions workflow logs

## 🏆 Project Status

✅ **Production Ready** - Active development and maintenance

**Latest Updates:**
- ✅ Phase 6: Performance optimizations and test coverage (49%)
- ✅ Phase 5: Production integration with GitHub Actions & Google Drive
- ✅ Phase 4: BaseCollector architecture with retry/circuit breaker
- ✅ Phase 3: CI/CD pipeline with automated testing
- ✅ Phase 2: Timezone normalization fixes

**Roadmap:**
- 🔄 REST API layer for third-party integrations
- 🔄 Mobile app support
- 🔄 Advanced ML predictions
- 🔄 MQTT broker for IoT devices

---

**Made with ❤️ for sustainable energy management**
