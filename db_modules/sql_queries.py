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
holidays_drop = "DROP TABLE IF EXISTS holidays;"
time_drop = "DROP TABLE IF EXISTS time;"
accidents_analysis_drop = "DROP TABLE IF EXISTS accidentsAnalysis;"

drop_table_queries = [accidents_drop, road_rank_drop, holidays_drop, time_drop, accidents_analysis_drop]

# CREATE TABLE STATEMENTS

create_time_table = ("""
    CREATE TABLE IF NOT EXISTS time (
        ev_time TIMESTAMP PRIMARY KEY,
        ev_date DATE,
        ev_hour INT,
        ev_day INT,
        ev_month INT,
        ev_year INT,
        ev_day_of_week VARCHAR);
    """)

create_holidays_table = ("""
    CREATE TABLE IF NOT EXISTS holidays (
        holiday_id INT PRIMARY KEY,
        name VARCHAR,
        ev_date DATE);
    """) 

create_road_rank_table = ("""
    CREATE TABLE IF NOT EXISTS roadRankings (
        road_rank_id SERIAL PRIMARY KEY,
        overall_rank INT,
        rank_year INT,
        state VARCHAR,
        commute_rank INT,
        transit_rank INT,
        road_quality_rank INT,
        bridge_quality_rank INT);
    """)

create_accidents_table = ("""
    CREATE TABLE IF NOT EXISTS accidents (
        accident_id VARCHAR PRIMARY KEY,
        severity INT,
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

create_analysis_table = ("""
    CREATE TABLE IF NOT EXISTS accidentsAnalysis(
        analysis_id SERIAL PRIMARY KEY,
        accident_id VARCHAR,
        "time" TIMESTAMP,
        state VARCHAR, 
        holiday_id INT,
        road_rank_id INT);
    """)

create_table_queries = [create_accidents_table, create_time_table, create_holidays_table,create_road_rank_table, create_analysis_table]

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
    INSERT INTO holidays (holiday_id, name, ev_date) 
    VALUES (%s,%s,%s);
    """) 

insert_time = ("""
    INSERT INTO time (ev_time, ev_date, ev_hour, ev_day, ev_month, ev_year, ev_day_of_week) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (ev_time) 
    DO NOTHING;
    """)

insert_road_rank = ("""
    INSERT INTO roadRankings (overall_rank, rank_year, state, commute_rank, transit_rank, road_quality_rank, bridge_quality_rank)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
    """)

insert_analysis = ("""
    INSERT INTO accidentsAnalysis(accident_id, "time", state,holiday_id, road_rank_id) VALUES (%s, %s, %s, %s, %s); 
    """)

insert_value_queries = [insert_accidents, insert_holidays, insert_time, insert_road_rank, insert_analysis]

# SELECT STATEMENTS

select_holiday = ("""
    SELECT holiday_id FROM holidays
    WHERE ev_date = %s;
    """)
select_rank = ("""
    SELECT road_rank_id FROM roadRankings
    WHERE rank_year = %s and state = %s;
    """)
