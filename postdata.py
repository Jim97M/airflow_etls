import pandas as pd
import json
import psycopg2
import requests
from extract import json_extract


def weather_data():

    
    conn = psycopg2.connect(
             "dbname= 'weather' user='postgres' host='localhost' password='postgres123' "
         )

    cur = conn.cursor()
    insert_sql = """ INSERT INTO current_weather VALUES(%s, %s)"""    
    endpoint = "https://api.open-meteo.com/v1/forecast?latitude=-1.2762&longitude=36.7965&daily=temperature_2m_max,rain_sum&current_weather=true&timezone=Europe%2FMoscow"
    r = requests.get(endpoint)
    data = r.json()
 
    # data_dict = json.loads(data)
    for record in data["daily_units"].items():
        cur.execute(insert_sql, [record[0], record[1]])
    conn.commit()    
    # print(data)
    # weather_values = json_extract(r.json(), 'daily')
    # print(weather_values)
    # return weather_values
    
weather_data()    


# conn = psycopg2.connect(
#             "dbname= 'weather' user='postgres' host='localhost' password='postgres123' "
#         )

# cur = conn.cursor()

# conn.autocommit = True        


# with open("/home/james/airflow/dags/data.json") as f:
#     data = json.load(f)

# df = pd.DataFrame(data)

# df.to_sql("weather", conn)



# def creating_staging_table(cursor):
#     cursor.execute("""
#       DROP TABLE IF EXISTS nested_weather;
#       CREATE UNLOGGED TABLE nested_weather(
#           ID serial NOT NULL PRIMARY KEY,
#           weathered jsonb
#       );
#     """)

# with conn.cursor() as cursor:
#     creating_staging_table(cursor)    



# weather = pd.read_json('/home/james/airflow/dags/data.json')
# df = weather
# df['weathered']  
# df = df.iloc[:, 1:]

# def fcn(df, table, cur):
#     if len(df) > 0:
#         df_columns = list(df)
#         columns = ",".join(df_columns)
#         values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))

#         insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)
#         cur.execute("truncate " + table + ";")
#         cur = conn.cursor()
#         psycopg2.extras.execute_batch(cur,insert_stmt, df.values)
#     conn.commit()    

# fcn(df, 'nested_weather', cur)    