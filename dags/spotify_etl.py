import sqlalchemy
import pandas as pd
from datetime import datetime
import requests

#Generate Spotify Auth Token

def check_if_valid_data(df: pd.DataFrame) -> bool:
    #Check if DataFrame is Empty
    if df.empty:
        print("No Schema Available")
        return False

    # Primary Key Check
    if pd.Series(df['played_at']).is_unique:    
        pass
    else:
        raise Exception("Primary Key Check is Violated")

    #Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    #Check that all timestamp's are of yesterday date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].toList()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
           raise Exception("One of the returned songs does not have a timestamp")

    return True


def run_spotify_etl():
    DATABASE_LOCATION = "sqlite3:///my_played_tracks.sqlite3"
    USER_ID = 'Jimmy'
    TOKEN = 'BQA87UIRvchM8JibsETDA_CCbmrFA1fjeme1i26456dgUneE-2lSHSakFvHPm4y4uXfzA6vn9CbJfQNkoh6aZDvaTOSQDHZ9dU-CbioOpmz2eKI4D50oAUxQPJAjio8tprqSBMZFOK_0NW8Q3XvEWatp0nb4_cmI0R4rG6IF'
    #Extract Part of The ETL Process

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer{token}".format(token=TOKEN)
    }

    #Time to Unix Time
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1) 
    yesterday_time_unix = int(yesterday.timestamp()) * 1000
    
    #Download all songs listened after yesterday, in the last 24hrs
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time = yesterday_time_unix), headers = headers)
    
    data = r.json()
    
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []
    
    
    #Extracting important bits in json
    
    for song in data["items"]:
       song_names.append(song["track"]["name"])
       artist_names.append(song["track"]["albums"]["artists"][0]["name"])
       played_at_list.append(song["played_at"])
       timestamps.append(song["played_at"][0:10])
    

    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps

    }


    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])


    #Validate
     
    if check_if_valid_data(song_df):
        print("Data valid, proceed to load stage")

    #Load

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
        CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)

    """
         
    cursor.execute(sql_query)
    print("Opened database successfully")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    conn.close()
    print("Close database successfully")


   