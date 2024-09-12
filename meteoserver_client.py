import asyncio
import logging
import meteoserver as meteo

async def get_MeteoServer_sun_forecast(api_key: str, plaats: str) -> dict:
    """
    Retrieves sun forecast data from MeteoServer API for a specified location.

    Args:
        api_key (str): The API key for accessing the MeteoServer API.
        plaats (str): The name of the location for which to fetch the sun forecast.

    Returns:
        dict: A dictionary containing the sun forecast data, units, and metadata.
    """
    try:
        async def read_json_url_sunData(api_key, plaats):
            _, forecast = meteo.read_json_url_sunData(api_key, plaats, loc=False, numeric=False)
            return forecast

        forecast = await read_json_url_sunData(api_key, plaats)
        
        return_data = {
            'sun forecast': forecast.to_dict(orient='records'),
            'units': {
                "temp": "°C",
                "elev (sun altitude at the start of the current hour)": "°",
                "az (sun azimuth at the start of the current hour,  N=0, E=90)": "°",
                "gr (global horizontal radiation intensity)": "J/hr/cm²",
                "gr_w (global horizontal radiation intensity)": "W/m²",
                "sd (number of sunshine minutes in the current hour)": "min",
                "tc (total cloud cover)": "%",
                "lc (low-cloud cover)": "%",
                "mc (intermediate-cloud cover)": "%",
                "hc (high-cloud cover)": "%",
                "vis (visibility)": "m",
                "prec (total precipitation in the current hour)": "mm(/h)"
            },
            'metadata': {
                "plaats": plaats
            }
        }

        return return_data

    except Exception as e:
        logging.error(f"Error retrieving MeteoServer sun forecast data: {e}")
        return None
    

async def get_MeteoServer_weather_forecast_data(api_key: str, plaats: str, model: str = 'HARMONIE') -> dict:
    """
    Retrieves weather forecast data from MeteoServer API for a specified location.

    Args:
        api_key (str): The API key for accessing the MeteoServer API.
        plaats (str): The name of the location for which to fetch the forecast.
        model (str, optional): The forecast model to use. Defaults to 'HARMONIE'.

    Returns:
        dict: A dictionary containing the weather forecast data, units, and metadata.
    """
    try:
        model = 'HARMONIE' # Option 1: HARMONIE/HiRLAM

        async def read_json_url_weatherforecast(api_key, plaats, model):
            data = meteo.read_json_url_weatherforecast(api_key, plaats, model=model)  
            return data

        data = await read_json_url_weatherforecast(api_key, plaats, model)

        return_data = {
            'weather forecast': data.to_dict(orient='records'),
            'units': {
                "temp": "°C",
                "winds (mean wind velocity)": "m/s",
                "windb (mean wind force)": "Beaufort",
                "windknp (mean wind velocity)": "knots",
                "windkmh (mean wind velocity)": "km/h",
                "windr (wind direction)": "°",
                "windrltr (wind direction)": "abbreviation",
                "gust (wind gust, GFS only)": "m/s",
                "gustb (wind gust, GFS only)": "Beaufort",
                "gustkt (wind gust, GFS only)": "knots",
                "gustkmh (wind gust, GFS only)": "km/h",
                "vis (visibility)": "m",
                "neersl (precipitation)": "mm",
                "luchtd (air pressure)": "mbar / hPa",
                "luchtdmmhg (air pressure)": "mm Hg",
                "luchtdinhg (air pressure)": "inch Hg",
                "rv (relative humidity)": "%",
                "gr (global horizontal radiation)": "W/m²",
                "hw (high cloud cover)": "%",
                "mw (medium cloud cover)": "%",
                "lw (low cloud cover)": "%",
                "tw (total cloud cover)": "%",
                "cape (convective available potential energy, GFS only)": "J/kg",
                "cond": "weather condition code",
                "ico": "weather icon code",
                "samenv": "text",
                "icoon": "image name"
            },
            'metadata': {
                "plaats": plaats,
                "model": f"{model} (Benelux)"
            }
        }

        return return_data

    except Exception as e:
        logging.error(f"Error retrieving MeteoServer forecast data: {e}")
        return None

async def main():
    import os
    from configparser import ConfigParser

    logging.basicConfig(level=logging.INFO)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    secrets_file = os.path.join(script_dir, 'secrets.ini')

    configur = ConfigParser() 
    configur.read(secrets_file)
    meteoserver_api_key = configur.get('api_keys', 'meteo')

    api_key = meteoserver_api_key
    plaats = "Amsterdam"

    forecast_data = await get_MeteoServer_weather_forecast_data(api_key, plaats)
    if forecast_data:
        print(forecast_data)
        print(f"Weather forecast for {plaats}:")
        print(f"Model: {forecast_data['metadata']['model']}")
        print("\nFirst 3 forecast entries:")
        for entry in forecast_data['weather forecast'][:3]:
            print(entry)
        print("\nUnits:")
        for key, value in list(forecast_data['units'].items())[:5]:  # Print first 5 units
            print(f"{key}: {value}")
    else:
        print(f"Failed to retrieve forecast data for {plaats}")

    sun_forecast_data = await get_MeteoServer_sun_forecast(api_key, plaats)
    if sun_forecast_data:
        print(f"Sun forecast for {plaats}:")
        print("\nFirst 3 forecast entries:")
        for entry in sun_forecast_data['sun forecast'][:3]:
            print(entry)
        print("\nUnits:")
        for key, value in list(sun_forecast_data['units'].items())[:5]:  # Print first 5 units
            print(f"{key}: {value}")
    else:
        print(f"Failed to retrieve sun forecast data for {plaats}")

if __name__ == "__main__":
    asyncio.run(main())