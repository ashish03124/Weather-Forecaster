from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv('../../config/.env')

Base = declarative_base()

class CurrentWeather(Base):
    __tablename__ = 'current_weather'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    city = Column(String)
    country = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    temperature = Column(Float)
    feels_like = Column(Float)
    temp_min = Column(Float)
    temp_max = Column(Float)
    pressure = Column(Float)
    humidity = Column(Float)
    wind_speed = Column(Float)
    wind_deg = Column(Float)
    cloudiness = Column(Float)
    weather_main = Column(String)
    weather_desc = Column(String)
    rain_1h = Column(Float)
    snow_1h = Column(Float)
    sunrise = Column(DateTime)
    sunset = Column(DateTime)
    timezone = Column(Integer)
    data_source = Column(String)

class Forecast(Base):
    __tablename__ = 'forecast'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime)
    city = Column(String)
    country = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    temperature = Column(Float)
    feels_like = Column(Float)
    temp_min = Column(Float)
    temp_max = Column(Float)
    pressure = Column(Float)
    humidity = Column(Float)
    wind_speed = Column(Float)
    wind_deg = Column(Float)
    cloudiness = Column(Float)
    weather_main = Column(String)
    weather_desc = Column(String)
    pop = Column(Float)
    rain_3h = Column(Float)
    snow_3h = Column(Float)
    data_source = Column(String)

def init_db():
    engine = create_engine(os.getenv('DB_URL'))
    Base.metadata.create_all(engine)
    return engine

def get_session():
    engine = init_db()
    Session = sessionmaker(bind=engine)
    return Session()
