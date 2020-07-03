import os
import pandas as pd
from create_database import connect_database
from sql_queries import *
    
def process_accidents_data(cur):
    """ process the data for the accidents tables
    Args:
        ::cur:: psycopg2 cursor object
    Returns:
        ::None::
    """
    
    df = pd.read_csv('./data/us_accidents.csv', delimiter=',', chunk=1000)
    for chunk in df:
        for row in chunk:
            accident_row = (row.ID, 
                row.Severity, 
                row.Start_Time, 
                row.End_Time, 
                row.Start_Lat, 
                row.Start_Lng, 
                row.Description, 
                row.City, 
                row.County, 
                row.State,
                row.Weather_Timestamp, 
                row['Temperature(F)'], 
                row['Wind_Chill(F)'], 
                row['Humidity(%)'], 
                row['Pressure(in)'],
                row['Visibility(mi)'], 
                row['Wind_Speed(mph)'],
                row['Precipitation(in)'],
                row.Weather_Condition, 
                row.Amenity, 
                row.Bump, 
                row.Crossing,
                row.Give_Way, 
                row.Junction, 
                row.No_Exit, 
                row.Railway, 
                row.Roundabout, 
                row.Stop, 
                row.Traffic_Calming, 
                row.Traffic_Signal, 
                row.Turning_Loop,
                row.Sunrise_Sunset,
                row.Civil_Twilight,
                row.Nautical_Twilight,
                row.Astronomical_Twilight)
            cur.execute(insert_accidents,accident_row) 

def process_holidays_data(cur):
    # read csv and change column names
    df = pd.read_csv('./data/usholidays.csv', delimiter=',')
    holidays_df.columns = ['holiday_id', 'date', 'name']
    for index, row in df.iterrows():
        holiday = (row.holiday_id, row.date, row.name)
        cur.execute(insert_holidays, holiday)

def process_road_rankings_data(cur):
    #read csv
    df = pd.read_csv()
    
        
    
def main():
    print("ETL starting...")
    
    conn, curr = connect_database()    
    process_accidents_data(cur)
    process_holidays_data(cur)
    
    conn.close()
    
if __name__ == '__main__':
    main()