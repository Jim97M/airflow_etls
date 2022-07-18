import requests
import json
import datetime as dt
from collections import OrderedDict
import csv
import time
from datetime import datetime
import psycopg2


conn = psycopg2.connect(
             "dbname= 'weather' user='postgres' host='localhost' password='postgres123' "
    )

cur = conn.cursor()    

url = 'https://api.open-meteo.com/v1/forecast?latitude=-1.2762&longitude=36.7965&daily=temperature_2m_max,rain_sum&current_weather=true&timezone=Europe%2FMoscow';
res = requests.get(url, headers={'Accept': 'application/json', 'Content-Type': 'application/json'})
datavals = json.loads(res.text)
print(datavals['daily'])
wetrain = []
wettemp = []
wetdate = []
for i in datavals['daily']['temperature_2m_max']:
    wettemp.append(i)

for j in datavals['daily']['rain_sum']:
    wetrain.append(j)

for k in datavals['daily']['time']:
    wetdate.append(k)

print(wetdate)
print(wettemp)
print(wetrain)

for index in range(0,7):
    print(wetdate[index])
    print(wettemp[index])
    print(wetrain[index])
    
    # create_table_query = """CREATE TABLE daily_units(id VARCHAR(250) NOT NULL, temperature_2m_max INT NOT NULL, rain_sum INT NOT NULL)"""
    insert_sql = """INSERT INTO daily_units VALUES(%s,%s, %s) """
    cur.execute(insert_sql, [wetdate[index], wettemp[index], wetrain[index]])    

    conn.commit()

    