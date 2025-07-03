import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import logging

load_dotenv('../../config/.env')

logger = logging.getLogger(__name__)

class WeatherDataProcessor:
    def __init__(self):
        self.db_engine = create_engine(os.getenv('DB_URL'))
        
    def get_recent_weather(self, city, hours=24):
        """Get recent weather data for a city"""
        query = f"""
        SELECT * FROM current_weather 
        WHERE city = '{city}' 
        AND timestamp >= NOW() - INTERVAL '{hours} hour'
        ORDER BY timestamp DESC
        """
        return pd.read_sql(query, self.db_engine)
    
    def get_forecast(self, city):
        """Get forecast data for a city"""
        query = f"""
        SELECT * FROM forecast 
        WHERE city = '{city}' 
        AND timestamp >= NOW()
        ORDER BY timestamp ASC
        """
        return pd.read_sql(query, self.db_engine)
    
    def detect_severe_weather(self, city):
        """Detect potential severe weather conditions"""
        current = self.get_recent_weather(city, 1)
        forecast = self.get_forecast(city)
        
        alerts = []
        
        # Check current conditions
        if not current.empty:
            latest = current.iloc[0]
            
            # Heavy rain alert
            if latest['rain_1h'] > 10:  # mm in 1 hour
                alerts.append({
                    'type': 'heavy_rain',
                    'severity': 'warning',
                    'message': f"Heavy rain alert: {latest['rain_1h']}mm in the last hour",
                    'timestamp': latest['timestamp']
                })
                
            # High wind alert
            if latest['wind_speed'] > 25:  # m/s
                alerts.append({
                    'type': 'high_wind',
                    'severity': 'warning',
                    'message': f"High wind alert: {latest['wind_speed']} m/s winds",
                    'timestamp': latest['timestamp']
                })
                
            # Extreme temperature alert
            if latest['temperature'] > 35:
                alerts.append({
                    'type': 'heat_wave',
                    'severity': 'danger',
                    'message': f"Heat wave alert: Temperature is {latest['temperature']}°C",
                    'timestamp': latest['timestamp']
                })
            elif latest['temperature'] < -10:
                alerts.append({
                    'type': 'cold_wave',
                    'severity': 'danger',
                    'message': f"Extreme cold alert: Temperature is {latest['temperature']}°C",
                    'timestamp': latest['timestamp']
                })
        
        # Check forecast for upcoming severe weather
        if not forecast.empty:
            # Check for heavy rain in forecast
            max_rain = forecast['rain_3h'].max()
            if max_rain > 20:  # mm in 3 hours
                rain_time = forecast[forecast['rain_3h'] == max_rain].iloc[0]['timestamp']
                alerts.append({
                    'type': 'heavy_rain_forecast',
                    'severity': 'warning',
                    'message': f"Heavy rain forecast: Up to {max_rain}mm expected around {rain_time}",
                    'timestamp': datetime.now()
                })
                
            # Check for temperature extremes in forecast
            max_temp = forecast['temp_max'].max()
            min_temp = forecast['temp_min'].min()
            
            if max_temp > 35:
                alert_time = forecast[forecast['temp_max'] == max_temp].iloc[0]['timestamp']
                alerts.append({
                    'type': 'heat_wave_forecast',
                    'severity': 'warning',
                    'message': f"Heat wave forecast: Temperature up to {max_temp}°C expected around {alert_time}",
                    'timestamp': datetime.now()
                })
                
            if min_temp < -10:
                alert_time = forecast[forecast['temp_min'] == min_temp].iloc[0]['timestamp']
                alerts.append({
                    'type': 'cold_wave_forecast',
                    'severity': 'warning',
                    'message': f"Extreme cold forecast: Temperature down to {min_temp}°C expected around {alert_time}",
                    'timestamp': datetime.now()
                })
        
        return alerts
    
    def get_historical_trends(self, city, days=30):
        """Get historical weather trends for analysis"""
        query = f"""
        SELECT 
            date(timestamp) as date,
            avg(temperature) as avg_temp,
            max(temp_max) as max_temp,
            min(temp_min) as min_temp,
            sum(rain_1h) as total_rain,
            avg(humidity) as avg_humidity,
            avg(wind_speed) as avg_wind
        FROM current_weather
        WHERE city = '{city}'
        AND timestamp >= NOW() - INTERVAL '{days} days'
        GROUP BY date(timestamp)
        ORDER BY date
        """
        return pd.read_sql(query, self.db_engine)
