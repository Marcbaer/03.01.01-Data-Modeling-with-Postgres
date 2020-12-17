#README: Data Modeling with Postgres

This Project creates a Postgres database bx using python and the corresponding connection toolkit psycopg2.
The source data consists of log files from a song streaming platform and the goal is to store the available information in a central repository for analytics purposes.

The database architecture follows the star schema and consist of one Fact Table (songplays) and four Dimension Tables (users,songs,artists and time).
The design is used to optimize for read operations and the architecture allows to efficiently query the database for analytics and reporting purposes.
