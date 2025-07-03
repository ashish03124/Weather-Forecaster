
import dash
from dash import dcc, html, Input, Output, State
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv
from src.data_collection.fetch_weather import WeatherDataFetcher
from src.data_processing.process_data import WeatherDataProcessor
from apscheduler.schedulers.background import BackgroundScheduler

load_dotenv(os.path.join(os.path.dirname(__file__), '../../config/.env'))

# Initialize data components
fetcher = WeatherDataFetcher()
processor = WeatherDataProcessor()

# Initialize the Dash app
app = dash.Dash(__name__,
                            external_stylesheets=[dbc.themes.BOOTSTRAP],
                            meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}])
server = app.server

# Set up auto-refresh scheduler
scheduler = BackgroundScheduler()
cities = ['London', 'New York', 'Tokyo', 'Sydney', 'Mumbai']

def fetch_all_data():
    for city in cities:
        try:
            fetcher.get_current_weather(city)
            fetcher.get_forecast(city)
            print(f"Data fetched for {city}")
        except Exception as e:
            print(f"Error fetching data for {city}: {e}")

# Schedule to run every 30 minutes
scheduler.add_job(fetch_all_data, 'interval', minutes=30)
fetch_all_data()  # Fetch once when app starts
scheduler.start()

# App layout
app.layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H1("Real-Time Weather Dashboard", className="text-center my-4"), width=12)
    ]),
    
    dbc.Row([
        dbc.Col([
            dcc.Dropdown(
                id='city-selector',
                options=[{'label': city, 'value': city} for city in cities],
                value='London',
                clearable=False,
                className="mb-3"
            ),
            dcc.Interval(id='update-interval', interval=5*60*1000),  # 5 minutes
            dbc.Button("Refresh Data", id='refresh-button', className="mb-3")
        ], width=12)
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Current Weather"),
                dbc.CardBody(id='current-weather')
            ], className="mb-4")
        ], width=6),
        
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Severe Weather Alerts"),
                dbc.CardBody(id='alerts')
            ], className="mb-4")
        ], width=6)
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Temperature Forecast (5 days)"),
                dbc.CardBody([
                    dcc.Graph(id='forecast-graph')
                ])
            ], className="mb-4")
        ], width=12)
    ]),
    
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Historical Trends (30 days)"),
                dbc.CardBody([
                    dcc.Graph(id='historical-graph')
                ])
            ])
        ], width=12)
    ])
], fluid=True)

# Callbacks
@app.callback(
    [Output('current-weather', 'children'),
     Output('alerts', 'children'),
     Output('forecast-graph', 'figure'),
     Output('historical-graph', 'figure')],
    [Input('city-selector', 'value'),
     Input('refresh-button', 'n_clicks'),
     Input('update-interval', 'n_intervals')]
)
def update_dashboard(city, refresh_clicks, interval):
    # Get current weather
    current = processor.get_recent_weather(city, 1)
    
    current_weather_card = []
    if not current.empty:
        latest = current.iloc[0]
        current_weather_card = [
            html.H4(f"{latest['city']}, {latest['country']}", className="card-title"),
            html.H6(latest['timestamp'].strftime('%Y-%m-%d %H:%M'), className="card-subtitle mb-2"),
            html.P(f"Temperature: {latest['temperature']}°C (Feels like {latest['feels_like']}°C)"),
            html.P(f"Weather: {latest['weather_main']} - {latest['weather_desc']}"),
            html.P(f"Humidity: {latest['humidity']}%"),
            html.P(f"Wind: {latest['wind_speed']} m/s at {latest['wind_deg']}°"),
            html.P(f"Pressure: {latest['pressure']} hPa"),
            html.P(f"Rain (1h): {latest['rain_1h']}mm" if latest['rain_1h'] > 0 else "No recent rain"),
            html.P(f"Cloudiness: {latest['cloudiness']}%")
        ]
    
    # Get alerts
    alerts = processor.detect_severe_weather(city)
    alerts_card = []
    if alerts:
        for alert in alerts:
            alert_color = 'danger' if alert['severity'] == 'danger' else 'warning'
            alerts_card.append(
                dbc.Alert(
                    alert['message'],
                    color=alert_color,
                    className="mb-2"
                )
            )
    else:
        alerts_card.append(
            dbc.Alert("No severe weather alerts", color="success")
        )
    
    # Get forecast and create graph
    forecast = processor.get_forecast(city)
    forecast_graph = px.line()
    if not forecast.empty:
        forecast['time'] = forecast['timestamp'].dt.strftime('%a %H:%M')
        forecast_graph = px.line(
            forecast,
            x='time',
            y='temperature',
            title=f"5-Day Forecast for {city}",
            labels={'time': 'Time', 'temperature': 'Temperature (°C)'}
        )
        forecast_graph.update_layout(
            xaxis=dict(tickangle=45),
            hovermode="x unified"
        )
    
    # Get historical data
    historical = processor.get_historical_trends(city, 30)
    historical_graph = px.line()
    if not historical.empty:
        historical_graph = px.line(
            historical,
            x='date',
            y=['avg_temp', 'max_temp', 'min_temp'],
            title=f"30-Day Temperature Trends for {city}",
            labels={'value': 'Temperature (°C)', 'variable': 'Metric', 'date': 'Date'}
        )
        historical_graph.update_layout(
            hovermode="x unified"
        )
    
    return current_weather_card, alerts_card, forecast_graph, historical_graph

if __name__ == '__main__':
    app.run(debug=True)
