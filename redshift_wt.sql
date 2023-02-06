-- create redshift serverless using default settings
-- use the default namespace also named default
-- use the option to create the default iam role
-- use the default workgroup also named default
-- for netowrk and security a vpc and a security group would be created - make note of that - for modifications to be made to allow remote access

-- in the workgroup settings enable public accessibility
-- to connect from agingity workbench or sql workbench, use the endpoint, from the endpoint remove database and port, in use ssl choose prefer, select dev as the database, admin as the user, fill in the admin password and one should be able to connect

-- for redshift
-- create a database
CREATE DATABASE SALESDB;
-- create a user
CREATE USER salesdb_user with PASSWORD 'ABCd4321';
-- redshift schemas
-- After you create a new database, you can create a new schema in the current database. A schema is a namespace that contains named database objects such as tables, views, and user-defined functions (UDFs). A database can contain one or multiple schemas, and each schema belongs to only one database. Two schemas can have different objects that share the same name

--Amazon Redshift automatically creates a schema called public for every new database. When you don't specify the schema name while creating database objects, the objects go into the public schema.

-- To access an object in a schema, qualify the object by using the schema_name.table_name notation.
-- create schema
CREATE SCHEMA SALES AUTHORIZATION GUEST;
-- To view the list of schemas in your database, run the following command.
select * from pg_namespace;

-- Use the GRANT statement to give permissions to users for the schemas.
-- The following example grants privilege to the GUEST user to select data from all tables or views in the SALESCHEMA using a SELECT statement.

GRANT ALL ON DATABASE SALESDB TO  salesdb_user;

-- load sample data from s3
-- create tables
create table if not exists users(
	userid integer not null distkey sortkey,
	username char(8),
	firstname varchar(30),
	lastname varchar(30),
	city varchar(30),
	state char(2),
	email varchar(100),
	phone char(14),
	likesports boolean,
	liketheatre boolean,
	likeconcerts boolean,
	likejazz boolean,
	likeclassical boolean,
	likeopera boolean,
	likerock boolean,
	likevegas boolean,
	likebroadway boolean,
	likemusicals boolean);
            
create table if not exists venue(
	venueid smallint not null distkey sortkey,
	venuename varchar(100),
	venuecity varchar(30),
	venuestate char(2),
	venueseats integer);


create table if not exists category(
	catid smallint not null distkey sortkey,
	catgroup varchar(10),
	catname varchar(10),
	catdesc varchar(50));

create table if not exists date(
	dateid smallint not null distkey sortkey,
	caldate date not null,
	day character(3) not null,
	week smallint not null,
	month character(5) not null,
	qtr character(5) not null,
	year smallint not null,
	holiday boolean default('N'));

create table if not exists event(
	eventid integer not null distkey,
	venueid smallint not null,
	catid smallint not null,
	dateid smallint not null sortkey,
	eventname varchar(200),
	starttime timestamp);

create table if not exists listing(
	listid integer not null distkey,
	sellerid integer not null,
	eventid integer not null,
	dateid smallint not null  sortkey,
	numtickets smallint not null,
	priceperticket decimal(8,2),
	totalprice decimal(8,2),
	listtime timestamp);

create table if not exists sales(
	salesid integer not null,
	listid integer not null distkey,
	sellerid integer not null,
	buyerid integer not null,
	eventid integer not null,
	dateid smallint not null sortkey,
	qtysold smallint not null,
	pricepaid decimal(8,2),
	commission decimal(8,2),
	saletime timestamp);

-- Load sample data from Amazon S3 by using the COPY command
-- download the sample data tickit.zip
-- wget https://docs.aws.amazon.com/redshift/latest/gsg/samples/tickitdb.zip
-- create a bucket to place the tickit data
-- aws s3api create-bucket --bucket tickit-bkt-samar --profile s3user

copy users from 's3://tickit-samar/tickit/allusers_pipe.txt' 
iam_role 'arn:aws:iam::644466320815:role/service-role/AmazonRedshift-CommandsAccessRole-20230127T130200'
delimiter '|' region 'us-east-1';

copy venue from 's3://tickit-samar/tickit/venue_pipe.txt' 
iam_role 'arn:aws:iam::644466320815:role/service-role/AmazonRedshift-CommandsAccessRole-20230127T130200'
delimiter '|' region 'us-east-1';

copy category from 's3://tickit-samar/tickit/category_pipe.txt' 
iam_role 'arn:aws:iam::644466320815:role/service-role/AmazonRedshift-CommandsAccessRole-20230127T130200'
delimiter '|' region 'us-east-1';

copy date from 's3://tickit-samar/tickit/date2008_pipe.txt' 
iam_role 'arn:aws:iam::644466320815:role/service-role/AmazonRedshift-CommandsAccessRole-20230127T130200'
delimiter '|' region 'us-east-1';

copy event from 's3://tickit-samar/tickit/allevents_pipe.txt' 
iam_role 'arn:aws:iam::644466320815:role/service-role/AmazonRedshift-CommandsAccessRole-20230127T130200'
delimiter '|' region 'us-east-1';

copy listing from 's3://tickit-samar/tickit/listings_pipe.txt' 
iam_role 'arn:aws:iam::644466320815:role/service-role/AmazonRedshift-CommandsAccessRole-20230127T130200'
delimiter '|' region 'us-east-1';

copy sales from 's3://tickit-samar/tickit/sales_tab.txt'
iam_role 'arn:aws:iam::644466320815:role/service-role/AmazonRedshift-CommandsAccessRole-20230127T130200'
delimiter '\t' timeformat 'MM/DD/YYYY HH:MI:SS' region 'us-east-1';

-- try queries against the imported data
-- Get definition for the sales table.
SELECT *    
FROM pg_table_def    
WHERE tablename = 'sales';    

-- Find total sales on a given calendar date.
SELECT sum(qtysold) 
FROM   sales, date 
WHERE  sales.dateid = date.dateid 
AND    caldate = '2008-01-05';

-- Find top 10 buyers by quantity.
SELECT firstname, lastname, total_quantity 
FROM   (SELECT buyerid, sum(qtysold) total_quantity
        FROM  sales
        GROUP BY buyerid
        ORDER BY total_quantity desc limit 10) Q, users
WHERE Q.buyerid = userid
ORDER BY Q.total_quantity desc;

-- Find events in the 99.9 percentile in terms of all time gross sales.
SELECT eventname, total_price 
FROM  (SELECT eventid, total_price, ntile(1000) over(order by total_price desc) as percentile 
       FROM (SELECT eventid, sum(pricepaid) total_price
             FROM   sales
             GROUP BY eventid)) Q, event E
       WHERE Q.eventid = E.eventid
       AND percentile = 1
ORDER BY total_price desc;

create external schema spectrum
from data catalog 
database 'sepectrumdb'
iam_role 'arn:aws:iam::644466320815:role/myredshiftfullaccessrole'
create external database if not exists;

aws s3 cp s3://redshift-downloads/tickit/spectrum/sales/ s3://tickit-samar/sales/ --copy-props none --recursive --profile s3user

create external table spectrum.sales(
salesid integer,
listid integer,
sellerid integer,
buyerid integer,
eventid integer,
dateid smallint,
qtysold smallint,
pricepaid decimal(8,2),
commission decimal(8,2),
saletime timestamp)
row format delimited
fields terminated by '\t'
stored as textfile
location 's3://tickit-samar/sales';

select top 10 spectrum.sales.eventid, sum(spectrum.sales.pricepaid) from
spectrum.sales, event
where spectrum.sales.eventid = salesdb.public.event.eventid
and spectrum.sales.pricepaid > 30
group by spectrum.sales.eventid
order by 2 desc;

explain
select top 10 spectrum.sales.eventid, sum(spectrum.sales.pricepaid)
from spectrum.sales, event
where spectrum.sales.eventid = salesdb.public.event.eventid
and spectrum.sales.pricepaid > 30
group by spectrum.sales.eventid