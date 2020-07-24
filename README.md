# Car Accidents ETL :blue_car:

ETL that extracts data from various sources to load car accident into a 
PostgreSQL database.  
Packages used include pandas and psycopg2.

Designed a data model based on a star schema design:  
![uml](uml.png)

### Project Structure
```
Car-Accidents-ETL
| README.md
| 
└─── data 
| | state_road_rankings.csv
| | usholidays.csv
└─── etl
| | etl.ipynb # preliminary code to set up ETL
| | etl.py # loads accidents, holidays, time, and demographics data
| | create_tables.py # creates the accidentAnalysis database tables
| | sql_queries.py # contains drop, insert, create statements
```
