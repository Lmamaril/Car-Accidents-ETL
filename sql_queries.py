""" SQL Queries

* Create postgreSQL the following tables:
    * accidents
    * roadRankings2020
    * demographics
    * holidays
    * time   
* Insert values into the database 
"""

# DROP TABLES

accidents_drop = "DROP TABLE IF EXISTS accidents;" 
road_rank_drop = "DROP TABLE IF EXISTS roadRankings;"
demographics_drop = "DROP TABLE IF EXISTS demographics;"
holidays_drop = "DROP TABLE IF EXISTS holidays;"
time_drop = "DROP TABLE IF EXISTS time;"
accidents_analysis_drop = "DROP TABLE IF EXISTS accidentsAnalysis;"

drop_table_queries = [accidents_drop, road_rank_drop, demographics_drop, holidays_drop, time_drop, accidents_analysis_drop]

# CREATE TABLE STATEMENTS

create_accidents_table = ("""
    CREATE TABLE IF NOT EXISTS accidents (
        accident_id VARCHAR PRIMARY KEY,
        severity VARCHAR,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        start_lat FLOAT,
        start_long FLOAT,
        description VARCHAR,
        city VARCHAR,
        county VARCHAR,
        state VARCHAR,
        weather_timestamp TIMESTAMP,
        temperature FLOAT,
        wind_chill FLOAT,
        humidity FLOAT,
        pressure FLOAT,
        visibility FLOAT,
        wind_speed FLOAT,
        precipitation FLOAT,
        weather_condition VARCHAR,
        amenity BOOLEAN,
        bump BOOLEAN,
        crossing BOOLEAN,
        giveway BOOLEAN,
        junction BOOLEAN,
        no_exit BOOLEAN,
        railway BOOLEAN,
        roundabout BOOLEAN,
        stop BOOLEAN,
        traffic_calming BOOLEAN,
        traffic_signal BOOLEAN,
        turning_loop BOOLEAN,
        sunrise_sunset VARCHAR,
        civil_twilight VARCHAR,
        naughtical_twilight VARCHAR,
        astronomical_twilight VARCHAR);
    """) 

create_holidays_table = ("""
    CREATE TABLE IF NOT EXISTS holidays (
        holiday_id INT PRIMARY KEY,
        name VARCHAR,
        date DATE);
    """) 

create_time_table = ("""
    CREATE TABLE IF NOT EXISTS time (
        time TIMESTAMP PRIMARY KEY,
        date DATE,
        hour INT,
        day INT,
        month INT,
        year INT,
        day_of_week VARCHAR);
    """)

create_road_rank_table = ("""
    CREATE TABLE IF NOT EXISTS roadRankings (
        road_rank_id SERIAL PRIMARY KEY,
        overall_rank INT,
        year INT,
        state VARCHAR,
        commute_rank INT,
        transit_rank INT,
        road_quality_rank INT,
        bridge_quality_rank INT);
    """)

create_demographics_table = ("""
    CREATE TABLE IF NOT EXISTS demographics (
        demographics_id INT PRIMARY KEY,
        state VARCHAR,
        year INT,
        household_income INT);
    """)

create_table_queries = [create_accidents_table, create_holidays_table,create_time_table,create_road_rank_table, create_demographics_table]

# INSERT VALUES STATEMENTS

insert_accidents = ("""
    INSERT INTO accidents (
        accident_id,
        severity,
        start_time,
        end_time,
        start_lat,
        start_long,
        description,
        city,
        county,
        state,
        weather_timestamp,
        temperature,
        wind_chill,
        humidity,
        pressure,
        visibility,
        wind_speed,
        precipitation,
        weather_condition,
        amenity,
        bump, 
        crossing,
        giveway,
        junction,
        no_exit,
        railway,
        roundabout,
        stop,
        traffic_calming,
        traffic_signal,
        turning_loop,
        sunrise_sunset,
        civil_twilight,
        naughtical_twilight,
        astronomical_twilight)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
       """)

insert_holidays = ("""
    INSERT INTO holidays (holiday_id, name, date) 
    VALUES (%s,%s,%s);
    """) 

insert_time = ("""
    INSERT INTO time (time, date, hour, day, month, year, day_of_week) 
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """)

insert_road_rank = ("""
    INSERT INTO roadRankings (overall_rank, year, state, commute_rank, transit_rank, road_quality_rank, bridge_quality_rank)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """)

insert_demographics = ("""
    INSERT INTO demographics (demographics_id, state, year,household_income);
    VALUES(%s, %s, %s, %s);
    """)

insert_value_queries = [insert_accidents, insert_holidays, insert_time, insert_road_rank, insert_demographics]