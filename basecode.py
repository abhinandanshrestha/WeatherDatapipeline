import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from dotenv import dotenv_values
import json

# Load the .env file
dotenv_values('.env')

url = "http://api.weatherapi.com/v1/history.json"
key = dotenv_values().get('key')
td=datetime.now()

records=[]

for i in range(1,7+1):
    today=(td- relativedelta(days=i))
    dt=today.strftime("%Y-%m-%d")

    q="Kathmandu"

    params = {
        "key": key,
        "dt": dt,
        "q":q
    }

    response = requests.get(url, params=params)
    # # Prettify the JSON response
    # prettified_data = json.dumps(response.json(), indent=4)

    # # Print the prettified JSON response
    # print(prettified_data)
    # break
    # Check if the request was successful

    if response.status_code == 200:
        data = response.json()

        # # # Extract the required data
        # forecast_data = data['forecast']['forecastday'][0]['hour']
        # print(forecast_data[0]['time'].split())
        # print(forecast_data[0]['time'].split()[0],forecast_data[0]['time'].split()[1])
        # break
        
        for i in data['forecast']['forecastday'][0]['hour']:
            time_epoch = i['time_epoch']
            date=i['time'].split()[0]
            time=i['time'].split()[1]
            temp_c = i['temp_c']
            temp_f = i['temp_f']
            is_day = i['is_day']
            condition=i['condition']['text']
            wind_mph = i['wind_mph']
            wind_kph = i['wind_kph']
            wind_degree = i['wind_degree']
            wind_dir = i['wind_dir']
            pressure_mb = i['pressure_mb']
            pressure_in = i['pressure_in']
            precip_mm = i['precip_mm']
            precip_in = i['precip_in']
            humidity = i['humidity']
            cloud = i['cloud']
            feelslike_c = i['feelslike_c']
            feelslike_f = i['feelslike_f']
            windchill_c = i['windchill_c']
            windchill_f = i['windchill_f']
            heatindex_c = i['heatindex_c']
            heatindex_f = i['heatindex_f']
            dewpoint_c = i['dewpoint_c']
            dewpoint_f = i['dewpoint_f']
            will_it_rain = i['will_it_rain']
            chance_of_rain = i['chance_of_rain']
            will_it_snow = i['will_it_snow']
            chance_of_snow = i['chance_of_snow']
            vis_km = i['vis_km']
            vis_miles = i['vis_miles']
            gust_mph = i['gust_mph']
            gust_kph = i['gust_kph']
            uv = i['uv']
            records.append({
                "date":date,
                "time": time,
                "time_epoch": time_epoch,
                "temp_c": temp_c,
                "temp_f": temp_f,
                "is_day": is_day,
                "condition": condition,
                "wind_mph": wind_mph,
                "wind_kph": wind_kph,
                "wind_degree": wind_degree,
                "wind_dir": wind_dir,
                "pressure_mb": pressure_mb,
                "pressure_in": pressure_in,
                "precip_mm": precip_mm,
                "precip_in": precip_in,
                "humidity": humidity,
                "cloud": cloud,
                "feelslike_c": feelslike_c,
                "feelslike_f": feelslike_f,
                "windchill_c": windchill_c,
                "windchill_f": windchill_f,
                "heatindex_c": heatindex_c,
                "heatindex_f": heatindex_f,
                "dewpoint_c": dewpoint_c,
                "dewpoint_f": dewpoint_f,
                "will_it_rain": will_it_rain,
                "chance_of_rain": chance_of_rain,
                "will_it_snow": will_it_snow,
                "chance_of_snow": chance_of_snow,
                "vis_km": vis_km,
                "vis_miles": vis_miles,
                "gust_mph": gust_mph,
                "gust_kph": gust_kph,
                "uv": uv
            })
    else:
        print("Request failed with status code:", response.status_code)


# Create a DataFrame
df = pd.DataFrame(records)
# Save the DataFrame as a CSV file
df.to_csv('data/'+dt+'.csv', index=False)