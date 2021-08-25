# **Project 1: Data Modeling with PostgreSQL**

## **Table of Contents**
* [Introduction](#Introduction)
* [Database Schema](#Database-Schema)
* [File Structure](#File-Structure)
* [How to Run](#How-to-Run)

## **Introduction**

This project is related to the Udacity course [Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027) inside the Lesson 2: Relational Data Models of the Curriculum Part 2: Data Modeling.

The purpose of this project is to apply what was seen in the course and build and ETL pipeline using Python. Here, i will define fact and dimension tables for a star scheme for a analytic focus relational database for a fictional starup called Sparkfy.

The ETL pipeline is responsible for loading all the information from several files inside two local directories into 5 tables described in the section [Database Schema](#Database-Schema).

## **Database Schema**
The dataset used in this project is a ~~really~~ small subset of real data from the [Million Song Dataset](http://millionsongdataset.com/).

### Fact table
|songplays|||||||||
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
|songplay_id|start_time|user_id|level|song_id|artist_id|session_id|location|user_agent|
|SERIAL|TIMESTAMP|INT|CHAR(4)|VARCHAR(50)|VARCHAR(50)|INT|VARCHAR(50)|VARCHAR(150)|
|PRIMARY&#160;KEY|NOT NULL|NOT&#160;NULL|-|-|-|-|-|-|

### Dimension Tables
|users|||||
|---:|---:|---:|---:|---:|
|user_id|first_name|last_name|gender|level|
|INT|VARCHAR(50)|VARCHAR(50)|CHAR(1)|CHAR(4)|
|PRIMARY&#160;KEY|-|-|-|-|

|songs|||||
|---:|---:|---:|---:|---:|
|song_id|title|artist_id|year|duration|
|VARCHAR(50)|VARCHAR(50)|VARCHAR(50)|INT|FLOAT|
|PRIMARY&#160;KEY|NOT&#160;NULL|NOT&#160;NULL|-|-|

|artists|||||
|---:|---:|---:|---:|---:|
|artist_id|name|location|latitude|longitude|
|VARCHAR(50)|VARCHAR(100)|VARCHAR(50)|FLOAT|FLOAT|
|PRIMARY&#160;KEY|NOT&#160;NULL|-|-|-|

|time|||||||
|---:|---:|---:|---:|---:|---:|---:|
|start_time|hour|day|week|month|year|weekday|
|TIMESTAMP|INT|INT|INT|INT|INT|INT|
|PRIMARY&#160;KEY|NOT&#160;NULL|NOT&#160;NULL|NOT&#160;NULL|NOT&#160;NULL|NOT&#160;NULL|NOT&#160;NULL|

## **File Structure**
The database as converted into json files to make them available locally inside the twos directorys **song_data/** and **log_data/**, which are inside of **./data/**. There are additional folders to sort out the data in accord with its content.

Both .ipynb files are used for easy testing of subparts of etl&period;py

Lastly, we have the three .py files that are responsible for the main etl algorithm.

## **How to Run**
Considering that you have all python installed and dependencies available, the only requisite of this code is that you run it with the commands bellow in the same order:

`python create_tables.py`

`python etl.py`
