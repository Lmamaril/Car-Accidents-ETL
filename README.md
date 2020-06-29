# Car Accidents ETL :blue_car:

ETL that extracts data from various sources to load car accident into a 
PostgreSQL database.

Designed a data model based on a star schema design:
![uml](uml.png)

### Project Structure
```
Car-Accidents-ETL
| README.md
| 
└─── data
|
└─── processing
| | etl.ipynb # preliminary code to set up ETL
|
└─── database
| | sql_queries.py
```
