# Project Overview
This project loads [Million Song Dataset](http://millionsongdataset.com/) into a STAR schema for further analysis. 

## Introduction
A fictional startup Sparkify wants to analyze data on songs and user activity. 
They would like Postgres database with tables designed to optimize queries on song play analysis.

## Design
### Relational Database
* Fact Table
    * songplays
* Dimension Table
    * users
    * songs
    * time
    * artists

### ETL
The python module pandas will be used to extract and transform data from JSON format into tables.

## Files
* conda-requirements.yml
    * Anaconda yaml file to create environment for correct python environment
* src
    * sql_queries.py
        * All SQL quries used for ETL are stored here
    * create_table.py
        * Create or recreate tables and related objects
    * etl.py
        * Used to ETL full dataset in Postgres database
* jupyter_notebook_test
    * etl.ipynb
        * Jupyter Notebook used to develop ETL.
    * test.ipynb
        * Test create and ETL functions
        

## ER Diagram
![Image of ER Diagram](https://www.dropbox.com/s/715b47dmaghzyfe/Blank%20Diagram.jpeg?raw=1)

