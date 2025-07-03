import os
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv('../../config/.env')
print("Loaded DB_URL:", os.getenv("DB_URL"))  # Debug print


class WeatherDataFetcher:
    def __init__(self):
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.base_url = "https://api.openweathermap.org/data/2.5/"
        self.db_engine = create_engine(os.getenv('DB_URL'))
        
    def get_current_weather(self, city_name, country_code=''):
        """Fetch current weather data for a location"""
        try:
            query = f"{city_name},{country_code}" if country_code else city_name
            url = f"{self.base_url}weather?q={query}&appid={self.api_key}&units=metric"
            response = requests.get(url)
            response.raise_for_status()
            
            data = response.json()
            processed_data = self._process_current_data(data)
            
            # Save to database
            self._save_to_db(processed_data, 'current_weather')
            
            return processed_data
        except Exception as e:
            logger.error(f"Error fetching current weather: {e}")
            raise

    def get_forecast(self, city_name, country_code=''):
        """Fetch 5-day weather forecast"""
        try:
            query = f"{city_name},{country_code}" if country_code else city_name
            url = f"{self.base_url}forecast?q={query}&appid={self.api_key}&units=metric"
            response = requests.get(url)
            response.raise_for_status()
            
            forecast_data = response.json()
            processed_data = self._process_forecast_data(forecast_data)
            
            # Save to database
            self._save_to_db(processed_data, 'forecast')
            
            return processed_data
        except Exception as e:
            logger.error(f"Error fetching forecast: {e}")
            raise

    def _process_current_data(self, data):
        """Extract relevant fields from current weather response"""
        return {
            'timestamp': datetime.fromtimestamp(data['dt']),
            'city': data['name'],
            'country': data['sys']['country'],
            'latitude': data['coord']['lat'],
            'longitude': data['coord']['lon'],
            'temperature': data['main']['temp'],
            'feels_like': data['main']['feels_like'],
            'temp_min': data['main']['temp_min'],
            'temp_max': data['main']['temp_max'],
            'pressure': data['main']['pressure'],
            'humidity': data['main']['humidity'],
            'wind_speed': data['wind']['speed'],
            'wind_deg': data['wind']['deg'],
            'cloudiness': data['clouds']['all'],
            'weather_main': data['weather'][0]['main'],
            'weather_desc': data['weather'][0]['description'],
            'rain_1h': data.get('rain', {}).get('1h', 0),
            'snow_1h': data.get('snow', {}).get('1h', 0),
            'sunrise': datetime.fromtimestamp(data['sys']['sunrise']),
            'sunset': datetime.fromtimestamp(data['sys']['sunset']),
            'timezone': data['timezone'],
            'data_source': 'openweather'
        }

    def _process_forecast_data(self, data):
        """Process forecast data into list of records"""
        processed = []
        city_info = {
            'city': data['city']['name'],
            'country': data['city']['country'],
            'latitude': data['city']['coord']['lat'],
            'longitude': data['city']['coord']['lon']
        }
        
        for item in data['list']:
            record = {
                'timestamp': datetime.fromtimestamp(item['dt']),
                'temperature': item['main']['temp'],
                'feels_like': item['main']['feels_like'],
                'temp_min': item['main']['temp_min'],
                'temp_max': item['main']['temp_max'],
                'pressure': item['main']['pressure'],
                'humidity': item['main']['humidity'],
                'wind_speed': item['wind']['speed'],
                'wind_deg': item['wind']['deg'],
                'cloudiness': item['clouds']['all'],
                'weather_main': item['weather'][0]['main'],
                'weather_desc': item['weather'][0]['description'],
                'pop': item.get('pop', 0),  # probability of precipitation
                'rain_3h': item.get('rain', {}).get('3h', 0),
                'snow_3h': item.get('snow', {}).get('3h', 0),
                'data_source': 'openweather'
            }
            record.update(city_info)
            processed.append(record)
            
        return processed

    def _save_to_db(self, data, table_name):
        """Save data to SQLite database"""
        try:
            if isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                df = pd.DataFrame(data)
                
            df.to_sql(
                table_name,
                self.db_engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            logger.info(f"Successfully saved data to {table_name}")
        except Exception as e:
            logger.error(f"Error saving to database: {e}")
            raise

if __name__ == "__main__":
    fetcher = WeatherDataFetcher()
    # Example usage
    current = fetcher.get_current_weather("London", "UK")
    forecast = fetcher.get_forecast("London", "UK")
