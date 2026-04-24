import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from geopy.geocoders import Nominatim
import time
import os

# 1. Setup the API and Geocoder
cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)
geolocator = Nominatim(user_agent="bicol_yield_project_ml")

# 2. Your Bicol Municipality Dictionary
bicol_locations = {
    "Masbate": ["San Fernando", "San Jacinto", "San Pascual", "Uson"],
    "Sorsogon": ["Barcelona", "Bulan", "Bulusan", "Casiguran", "Castilla", "Donsol", "Gubat", "Irosin", "Juban", "Magallanes", "Matnog", "Pilar", "Prieto Diaz", "Santa Magdalena", "Sorsogon City"]
}

csv_filename = "bicol_weather_checkpoint.csv"
print("Starting the Checkpoint Data Pull...")

# 3. Loop through Provinces and Municipalities
for province, towns in bicol_locations.items():
    for town in towns:
        success = False 
        
        while not success:
            try:
                query = f"{town}, {province}, Bicol, Philippines"
                location = geolocator.geocode(query)
                
                if location:
                    lat = location.latitude
                    lon = location.longitude
                    
                    url = "https://archive-api.open-meteo.com/v1/archive"
                    params = {
                        "latitude": lat,
                        "longitude": lon,
                        "start_date": "2012-01-01", 
                        "end_date": "2026-04-22",   
                        "daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean", 
                                  "precipitation_sum", "wind_speed_10m_max", "wind_gusts_10m_max"],
                        "timezone": "Asia/Singapore"
                    }
                    
                    responses = openmeteo.weather_api(url, params=params)
                    response = responses[0]
                    daily = response.Daily()
                    
                    daily_data = {"date": pd.date_range(
                        start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
                        end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
                        freq = pd.Timedelta(seconds = daily.Interval()),
                        inclusive = "left"
                    )}
                    
                    daily_data["Province"] = province
                    daily_data["Municipality"] = town
                    daily_data["Max_Temp"] = daily.Variables(0).ValuesAsNumpy()
                    daily_data["Min_Temp"] = daily.Variables(1).ValuesAsNumpy()
                    daily_data["Mean_Temp"] = daily.Variables(2).ValuesAsNumpy()
                    daily_data["Precipitation_Sum"] = daily.Variables(3).ValuesAsNumpy()
                    daily_data["Max_Wind_Speed"] = daily.Variables(4).ValuesAsNumpy()
                    daily_data["Max_Wind_Gusts"] = daily.Variables(5).ValuesAsNumpy()
                    
                    town_df = pd.DataFrame(data = daily_data)
                    
                    # IMMEDIATELY SAVE TO CSV (Append Mode)
                    town_df.to_csv(csv_filename, mode='a', header=not os.path.exists(csv_filename), index=False)
                    
                    print(f"Success: Grabbed and SAVED data for {town}, {province}")
                    success = True 
                    time.sleep(3)
                    
                else:
                    print(f"Warning: Could not find exact coordinates for {town}. Skipping.")
                    success = True 
                    time.sleep(1)
                    
            except Exception as e:
                error_message = str(e)
                if "Minutely" in error_message:
                    print(f"Warning: Hit Minutely Limit on {town}. Pausing 60s...")
                    time.sleep(61) 
                elif "Hourly" in error_message:
                    print(f"Warning: HOURLY LIMIT HIT at {town}. Everything before this is already saved safely in {csv_filename}!")
                    raise SystemExit("Stopping script to prevent further errors. Change your IP or wait an hour, remove the finished towns from the dictionary, and run again.")
                else:
                    print(f"Error processing {town}: {e}. Skipping.")
                    success = True 
                    time.sleep(2)

print("\nAll towns processed successfully!")
