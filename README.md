# Energy Data Hub

A Python-based system for collecting, processing, and publishing energy price and weather forecast data for energy optimization applications. The system fetches data from multiple sources including ENTSO-E, Energy Zero, EPEX SPOT, Nord Pool Elspot, OpenWeather, and MeteoServer.

## Features

- Automated data collection from multiple energy price and weather sources
- Data validation and standardization
- Secure data encryption for published data
- Automated GitHub Actions workflow for regular data updates
- Support for both local and cloud-based deployment
- Integration with LabVIEW and National Instruments hardware

## Published Data

Encrypted data is published to the following endpoints:
- [Energy Price Forecast](https://ducroq.github.io/energydatahub/energy_price_forecast.json)
- [Weather Forecast](https://ducroq.github.io/energydatahub/weather_forecast.json)
- [Sun Forecast](https://ducroq.github.io/energydatahub/sun_forecast.json)
- [Air Quality Data](https://ducroq.github.io/energydatahub/air_quality.json)

## Requirements

- Python 3.x
- Required Python packages listed in `requirements.txt`
- API keys for various data sources
- Internet connection for data fetching

## Local Setup

1. Clone the repository
2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `settings.ini` file in the project root with the following structure:
```ini
[location]
latitude = YOUR_LATITUDE
longitude = YOUR_LONGITUDE

[data]
encryption = 1
```
Here data is a flag setting encryption on/off.

4. Create a `secrets.ini` file in the project root with the following structure:
```ini
[api_keys]
entsoe = YOUR_ENTSOE_API_KEY
openweather = YOUR_OPENWEATHER_API_KEY
meteo = YOUR_METEOSERVER_API_KEY

[security_keys]
encryption = YOUR_BASE64_ENCODED_ENCRYPTION_KEY
hmac = YOUR_BASE64_ENCODED_HMAC_KEY
```

## GitHub Actions Setup

For running the data collection workflow on GitHub Actions, the following secrets need to be configured in your repository:

- `PAT`: GitHub Personal Access Token with repo scope
- `ENCRYPTION_KEY`: Base64 encoded encryption key
- `HMAC_KEY`: Base64 encoded HMAC key
- `ENTSOE_API_KEY`: ENTSO-E API key
- `OPENWEATHER_API_KEY`: OpenWeather API key
- `METEO_API_KEY`: MeteoServer API key
- `GOOGLE_API_KEY`: Google API key (if using Google services)

## Usage

### Local Data Collection
```bash
python data_fetcher.py
```

### Manual GitHub Actions Trigger
You can manually trigger the data collection workflow from the Actions tab in your GitHub repository.

## Data Sources

- ENTSO-E: European Network of Transmission System Operators for Electricity
- Energy Zero: Dutch energy price provider
- EPEX SPOT: European Power Exchange
- Nord Pool Elspot: Nordic power exchange
- OpenWeather: Weather data provider
- MeteoServer: Dutch weather service provider
- Luchtmeetnet: Dutch air quality monitoring network

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Security

All published data is encrypted using industry-standard encryption methods. To decrypt the data, you'll need the appropriate encryption and HMAC keys.

## Project Status

Active development - Regular updates and maintenance ongoing.