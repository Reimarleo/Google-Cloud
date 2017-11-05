# Google-Cloud

# bq_module
Modue to query Gooble BigQuery. It contains a single function that takes a SQL query as an argument and rerurns an iterator.

# sql_module

Module with functions to query a MySQL database using the pymysql library.

# part_1

Using the modules above, it queries the BigQuery public dataset bigquery-public-data:noaa_gsod and loads into a Google Cloud SQL database a table with minimum and maximum temperatures in degrees Celsius between the years 1990 and 2000 for each US state.

# part_2

Takes a state as input and returns the temperature information about that state from the table created by part_1.

# test_part_2

A limited example of unit testing.
