#README: Data Modeling with Postgres

This Project creates a Postgres database bx using python and the corresponding connection toolkit psycopg2.
The source data consists of log files from a song streaming platform and the goal is to store the available information in a central repository for analytics purposes.

The database architecture follows the star schema and consist of one Fact Table (songplays) and four Dimension Tables (users,songs,artists and time).
The design is used to optimize for read operations and the architecture allows to efficiently query the database for analytics and reporting purposes.

The scripts in the repository are used as described below:

sql_queries.py : This file contains the sql queries used to build, delete and populate the database tables
create_tables.py : This script needs to be run prior the ETL process can be initiated. It builds the database architecture and tables.
etl.ipynb : This notebook is used for debugging purposes and processes and inserts a single entry into the created tables.
etl.py : This file is used to process the source data and populate the tables create by create_tables.py with the source information parsed from the json files

The Database Design:

<img src="./databasedesign.png" width="500" height="500">