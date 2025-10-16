--queries
--Raw csv to stage:
CREATE DATABASE data_design;
USE DATABASE data_design;
CREATE SCHEMA staging;
USE SCHEMA staging;

CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null');

CREATE OR REPLACE STAGE csv_stage 
    FILE_FORMAT = csv_format;

list @csv_stage;

CREATE OR REPLACE TABLE lv_precipitation (
    date DATE,
    precipitation STRING,
    precipitation_normal STRING
);

CREATE OR REPLACE TABLE lv_temperature (
    date DATE,
    min DOUBLE,
    max DOUBLE,
    normal_min DOUBLE,
    normal_max DOUBLE
);

COPY INTO lv_precipitation
FROM @csv_stage/USW00093134-LOS_ANGELES_DOWNTOWN_USC-precipitation-inch.csv.gz
FILE_FORMAT = (FORMAT_NAME = csv_format);

COPY INTO lv_temperature
FROM @csv_stage/USW00093134-temperature-degreeF.csv.gz
FILE_FORMAT = (FORMAT_NAME = csv_format);

select * from lv_temperature;
select * from lv_precipitation;

--Raw Json to Stage:
USE DATABASE data_design;
USE SCHEMA staging;

create or replace file format jsonformat type='JSON' strip_outer_array=true;

create or replace stage json_stage file_format = jsonformat;

list @json_stage;

create or replace table yelp_academic_dataset_business(recordjson variant);
create or replace table yelp_academic_dataset_checkin(recordjson variant);
create or replace table yelp_academic_dataset_covid_features(recordjson variant);
create or replace table yelp_academic_dataset_review(recordjson variant);
create or replace table yelp_academic_dataset_tip(recordjson variant);
create or replace table yelp_academic_dataset_user(recordjson variant);

USE WAREHOUSE my_wh;

COPY INTO yelp_academic_dataset_business
FROM @json_stage/yelp_academic_dataset_business.json.gz
FILE_FORMAT = (FORMAT_NAME = jsonformat);

COPY INTO yelp_academic_dataset_checkin
FROM @json_stage/yelp_academic_dataset_checkin.json.gz
FILE_FORMAT = (FORMAT_NAME = jsonformat);

COPY INTO yelp_academic_dataset_covid_features
FROM @json_stage/yelp_academic_dataset_covid_features.json.gz
FILE_FORMAT = (FORMAT_NAME = jsonformat);

COPY INTO yelp_academic_dataset_review
FROM @json_stage/yelp_academic_dataset_review.json.gz
FILE_FORMAT = (FORMAT_NAME = jsonformat);

COPY INTO yelp_academic_dataset_tip
FROM @json_stage/yelp_academic_dataset_tip.json.gz
FILE_FORMAT = (FORMAT_NAME = jsonformat);

COPY INTO yelp_academic_dataset_user
FROM @json_stage/yelp_academic_dataset_user.json.gz
FILE_FORMAT = (FORMAT_NAME = jsonformat);

select * from yelp_academic_dataset_business limit 50;

SELECT
  recordjson:"business_id"::STRING AS business_id,
  recordjson:"name"::STRING AS name,
  recordjson:"address"::STRING AS address,
  recordjson:"city"::STRING AS city,
  recordjson:"state"::STRING AS state,
  recordjson:"postal_code"::STRING AS postal_code,
  recordjson:"latitude"::FLOAT AS latitude,
  recordjson:"longitude"::FLOAT AS longitude,
  recordjson:"stars"::FLOAT AS stars,
  recordjson:"review_count"::INT AS review_count,
  recordjson:"is_open"::INT AS is_open,
  recordjson:"categories"::STRING AS categories,
  recordjson:"attributes":"ByAppointmentOnly"::STRING AS by_appointment_only
FROM yelp_academic_dataset_business
limit 1;

--Staging Data to ODS:

USE DATABASE data_design;
create schema ODS;
USE SCHEMA ODS;

CREATE OR REPLACE WAREHOUSE my_wh
  WAREHOUSE_SIZE = 'MEDIUM'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  MIN_CLUSTER_COUNT = 1
  MAX_CLUSTER_COUNT = 3
  SCALING_POLICY = 'STANDARD';

USE WAREHOUSE my_wh;

create or replace table "GEOGRAPHY"(
    "geography_id" number identity primary key,
    "address" string,
    "latitude" double,
    "longitude" double,
    "postal_code" string,
    "city" string,
    "state" string
);

insert into "GEOGRAPHY"("address", "latitude", "longitude", "postal_code", "city", "state")
select distinct
    RECORDJSON:address,
    RECORDJSON:latitude,
    RECORDJSON:longitude,
    RECORDJSON:postal_code,
    RECORDJSON:city,
    RECORDJSON:state
from 
    data_design.staging.YELP_ACADEMIC_DATASET_BUSINESS;

create or replace table "BUSINESS"(
    "business_id" string primary key,
    "geography_id" number references data_design.ODS."GEOGRAPHY"("geography_id"),
    "name" string,
    "is_open" string,
    "stars" double
);

insert into "BUSINESS"
select distinct
    RECORDJSON:business_id,
    g."geography_id",
    RECORDJSON:name,
    RECORDJSON:is_open,
    RECORDJSON:stars
from 
    data_design.staging.YELP_ACADEMIC_DATASET_BUSINESS as b
join 
    data_design.ODS."GEOGRAPHY" as g
on
    RECORDJSON:city = g."city" and
    RECORDJSON:address = g."address" and
    RECORDJSON:latitude = g."latitude" and
    RECORDJSON:longitude = g."longitude" and
    RECORDJSON:state = g."state";

create or replace table "CHECKIN"(
    "business_id" string primary key references data_design.staging."BUSINESS"("business_id"),
    "date" string
);

insert into "CHECKIN"
select 
    RECORDJSON:business_id,
    RECORDJSON:date
from 
    data_design.staging.YELP_ACADEMIC_DATASET_CHECKIN;

create or replace table "CUSTOMER"(
    "customer_id" string primary key,
    "average_stars" double,
    "fans" number,
    "review_count" number,
    "name" string
);

insert into "CUSTOMER"
select 
    RECORDJSON:user_id,
    RECORDJSON:average_stars,
    RECORDJSON:fans,
    RECORDJSON:review_count,
    RECORDJSON:name
from 
    data_design.staging.YELP_ACADEMIC_DATASET_USER;

create or replace table "COVID"(
  "business_id" string primary key references data_design.ODS."BUSINESS"("business_id"),
  "call_action" string,
  "covid_banner" string,
  "grubhub" string,
  "request_a_quote" string,
  "temporary_closed" string,
  "virtual_services" string,
  "delivery_or_takeout" string,
  "highlights" string
);

insert into "COVID"
select 
    RECORDJSON:"Call To Action enabled",
    RECORDJSON:"Covid Banner",
    RECORDJSON:"Grubhub enabled",
    RECORDJSON:"Request a Quote Enabled",
    RECORDJSON:"Temporary Closed Until",
    RECORDJSON:"Virtual Services Offered",
    RECORDJSON:business_id,
    RECORDJSON:"delivery or takeout",
    RECORDJSON:highlights
from 
    data_design.staging.YELP_ACADEMIC_DATASET_COVID_FEATURES;

create or replace table "REVIEW"(
    "review_id" string primary key,
    "business_id" string references data_design.ODS."BUSINESS"("business_id"),
    "date" date,
    "cool" number,
    "funny" number,
    "stars" double,
    "useful" double,
    "user_id" string references data_design.ODS."CUSTOMER"("customer_id")
);

insert into "REVIEW"
select
    RECORDJSON:review_id,
    RECORDJSON:business_id,
    RECORDJSON:date::date,
    RECORDJSON:cool,
    RECORDJSON:funny,
    RECORDJSON:stars,
    RECORDJSON:useful,
    RECORDJSON:user_id
from 
    data_design.staging.YELP_ACADEMIC_DATASET_REVIEW;

create or replace table "TIP"(
  "business_id" string primary key references data_design.ODS."BUSINESS"("business_id"),
  "compliment_count" number,
  "date" date,
  "user_id" string references data_design.ODS."CUSTOMER"("customer_id")
);

insert into "TIP"
select 
    RECORDJSON:business_id,
    RECORDJSON:compliment_count,
    RECORDJSON:date,
    RECORDJSON:user_id
from 
    data_design.staging.YELP_ACADEMIC_DATASET_TIP;

create or replace table "TEMPERATURE"(
    "date" date primary key,
    "min_temp" double,
    "max_temp" double,
    "normal_min" double,
    "normal_max" double
);

insert into "TEMPERATURE"(
    "date", "min_temp", "max_temp", "normal_min", "normal_max"
)
select 
    date,
    min,
    max,
    normal_min,
    normal_max
from 
    data_design.staging.lv_temperature;

create or replace table "PRECIPITATION"(
    "date" date primary key,
    "precipitation" string,
    "precipitation_normal" double
);

insert into "PRECIPITATION"(
    "date", "precipitation", "precipitation_normal"
)
select 
    date,
    precipitation,
    precipitation_normal
from 
    data_design.staging.lv_precipitation;

--ODS data to DWH:
Use database data_design;
create schema DWHH;
USE SCHEMA DWHH;
USE WAREHOUSE my_wh;

drop schema DWHH;

create or replace table "DIM_CUSTOMER"(
    "customer_id" string primary key,
    "name" string,
    "average_stars" double,
    "fans" number,
    "review_count" number
);

insert into "DIM_CUSTOMER"
select distinct
    "customer_id",
    "name",
    "average_stars",
    "fans",
    "review_count"
from data_design.ODS."CUSTOMER";

create or replace table "DIM_TEMPERATURE"(
    "date" DATE primary key,
    "min_temp" double,
    "max_temp" double,
    "normal_min" double,
    "normal_max" double
);

insert into "DIM_TEMPERATURE"
select distinct
    "date",
    "min_temp",
    "max_temp",
    "normal_min",
    "normal_max"
from 
    data_design.ODS."TEMPERATURE";

create or replace table "DIM_PRECIPITATION"(
    "date" DATE primary key,
    "precipitation" string,
    "precipitation_normal" string
);

insert into "DIM_PRECIPITATION"
select distinct
    "date",
    "precipitation",
    "precipitation_normal"
from 
    data_design.ODS."PRECIPITATION";

create or replace table "DIM_BUSINESS"(
    "business_id" string primary key,
    "name" string,
    "is_open" number,
    "stars" double,
    "city" string,
    "state" string,
    "postal_code" string,
    "checkin_dates" string
);

insert into "DIM_BUSINESS"(
    "business_id",
    "name",
    "is_open",
    "stars",
    "city",
    "state",
    "postal_code",
    "checkin_dates"
)
select
    b."business_id",
    b."name",
    b."is_open",
    b."stars",
    g."city",
    g."state",
    g."postal_code",
    ch."date"
from data_design.ODS."BUSINESS" as b 
join data_design.ODS."GEOGRAPHY" as g on b."geography_id" =  g."geography_id"
join data_design.ODS."CHECKIN" as ch on b."business_id" = ch."business_id";

create or replace table "FACT"(
    "fact_id" string primary key,
    "business_id" string references data_design.DWH."DIM_BUSINESS"("business_id"),
    "customer_id" string references data_design.DWH."DIM_CUSTOMER"("customer_id"),
    "date" date references data_design.DWH."DIM_TEMPERATURE"("date"),
    "stars" double
);

insert into "FACT"
select
    r."review_id",
    r."business_id",
    r."user_id",
    r."date",
    r."stars"
from
    data_design.ODS."REVIEW" as r;
