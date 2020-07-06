import os
import pandas as pd
import datetime
import time
from create_database import connect_database
from sql_queries import *

""" ETL process for a US-Accidents-ETL Process 
"""

def process_holidays_data(cur):
    """ process the data for the holidays table
    Args:
        ::cur:: psycopg2 cursor object
    Returns:
        ::None::
    """
    # read csv and change column names
    df = pd.read_csv('./data/usholidays.csv', delimiter=',')
    df.columns = ['holiday_id', 'date', 'name']
    for index, row in df.iterrows():
        dt = pd.to_datetime(row.date)
        holiday = (row.holiday_id, row.name, datetime.date(dt.year, dt.month, dt.day))
        cur.execute(insert_holidays, holiday)
    
def process_road_rankings_data(cur):
    """ process the data for the roadRankings table
    Args:
        ::cur:: psycopg2 cursor object
    Returns:
        ::None::
    """    
    #read csv
    df = pd.read_csv('./data/state_road_rankings.csv',delimiter=',',nrows=3)
    df.columns = ['overall_rank','year','state','commute_time', 'public_transit','road_quality','bridge_quality']
    for index, row in df.iterrows():
        row_rank = (row.overall_rank, row.year, row.state, row.commute_time, row.public_transit, row.road_quality, row.bridge_quality)
        cur.execute(insert_road_rank, row_rank)
    
def process_accidents_data(cur):
    """ process the data for the accidents table and accidentsAnalysis table
    Args:
        ::cur:: psycopg2 cursor object
    Returns:
        ::None::
    """
    
    # read the accidents csv 1000 rows at a time
    df = pd.read_csv('./data/us_accidents.csv', delimiter=',', chunksize=1000, nrows=1000)
    for chunk in df:
        for index, row in chunk.iterrows():
            
            # Handle null Weather_Timestamp
            weather_ts = pd.to_datetime(row.Weather_Timestamp)
            if pd.isnull(weather_ts):
                weather_ts = None                
            
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
                weather_ts, 
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
            
            def convert_to_time_row(timestamp):
                """ converts 
                Args:
                    ::timestamp:: str value of a timestamp
                Returns:
                    ::None::
                """
                dt = pd.to_datetime(timestamp)
                time_row = (dt,datetime.date(dt.year, dt.month, dt.day), dt.hour,dt.day, dt.month, dt.year, dt.weekday())
                return time_row
                
            dt_start = convert_to_time_row(row.Start_Time)
            dt_end = convert_to_time_row(row.End_Time)
            time_conversions = [dt_start, dt_end]
            
            if weather_ts is not None:
                time_conversions.append(convert_to_time_row(row.Weather_Timestamp)) 
            for time in time_conversions:
                cur.execute(insert_time, time)
            
            
            def retrieve_row_match(cur, select_statement, match):
                """ retrieves first column of the select statement, 
                    otherwise, returns None
                """
                cur.execute(select_statement, match)
                result = cur.fetchone()
                return result[0] if result else None
    
    
            # retrieve the matching holiday id if exists 
            dt_date = (dt_start[1],)
            holiday_id = retrieve_row_match(cur, select_holiday, dt_date)

            # retrieve year and state to match the road ranking id
            ranking_match = (dt_date[0].year, row.State)
            ranking_id = retrieve_row_match(cur, select_rank, ranking_match)

            analysis_row = (row.ID, dt_start[0], row.State, holiday_id, ranking_id)
            cur.execute(insert_analysis, analysis_row)
        
    
def main():
    start_time = time.time()
    print("ETL starting...")
    
    conn, cur = connect_database()    

    process_holidays_data(cur)
    process_road_rankings_data(cur)
    process_accidents_data(cur)
    
    conn.close()
    time_delta = str(time.time() - start_time)
    print('ETL Finished in {} secs'.format(time_delta) )
    
if __name__ == '__main__':
    main()