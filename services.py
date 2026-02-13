import httpx
import os
import redis.asyncio as redis
import json

GEOCODE_API_URL = os.getenv('GEOCODE_API_URL')
FORECAST_API_URL = os.getenv('FORECAST_API_URL')


def get_cache_key(lat: float, lon: float):
    lat = round(lat/0.05)*0.05
    lon = round(lon/0.05)*0.05

    return f"weather:{lat:.2f}:{lon:.2f}"


async def fetch_locations(name: str, client: httpx.AsyncClient):
    '''
    Fetch location metadata from geocode api for a given name value.
    Returns 10 locations that match query string, name.
    
    :param name: Name of the location to fetch metadata for
    :type name: str
    '''

    payload = {
        'name': name,
        'count': 10,
        'language': 'en',
        'format': 'json'
    }

    response = await client.get(GEOCODE_API_URL, params=payload)
    response.raise_for_status()
    response_json = response.json()

    normalized_response = []

    if 'results' in response_json:
        results = response_json['results']

        for result in results:
            item = {
                'name': result['name'],
                'latitude': result['latitude'],
                'longitude': result['longitude'],
                'country': result.get('country', ''),
                'admin1': result.get('admin1', '')
            }
            normalized_response.append(item)
    
    return normalized_response


async def fetch_weather(lat: float, lon: float, client: httpx.AsyncClient, redis: redis.Redis):
    '''
    Fetch current and forecast weather data for a given location. 
    Automatically normalize time based on location's timezone
    
    :param lat: location's Latitude
    :type lat: float
    :param lon: location's Longitude
    :type lon: float
    '''

    cache_key = get_cache_key(lat, lon)
    cached_data = await redis.get(cache_key)

    if cached_data:
        return json.loads(cached_data)

    payload = {
        'latitude': lat,
        'longitude': lon,
        'current': ','.join(['temperature_2m', 'apparent_temperature', 'weather_code', 'is_day', 'precipitation_probability', 'precipitation', 'wind_speed_10m', 'wind_direction_10m', 'relative_humidity_2m', 'pressure_msl', 'visibility']),
        'hourly': ','.join(['temperature_2m', 'apparent_temperature', 'weather_code', 'is_day', 'precipitation_probability', 'precipitation', 'wind_speed_10m', 'wind_direction_10m', 'relative_humidity_2m', 'pressure_msl', 'visibility']),
        'timezone': 'auto',
        'forecast_hours': 25
    }

    response = await client.get(FORECAST_API_URL, params=payload)
    response.raise_for_status()
    response_json = response.json()

    normalized_response = {}

    current = response_json.get('current', {})
    hourly_raw = response_json.get('hourly', {})
    hourly_formatted = []

    if hourly_raw:
        count = len(hourly_raw['time'])

        for i in range(1, count):
            hour_obj = {}
            for key in hourly_raw:
                hour_obj[key] = hourly_raw[key][i]
            hourly_formatted.append(hour_obj)

    normalized_response['current'] = current
    normalized_response['hourly'] = hourly_formatted 

    await redis.set(cache_key, json.dumps(normalized_response), ex=900)  

    return normalized_response