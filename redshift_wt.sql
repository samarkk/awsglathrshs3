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

-- psql -h database-1.cgmmakgnsaxs.us-east-1.rds.amazonaws.com -p 5432 -U postgres -W
create user salesdbuser  with password 'Admin98919';
grant all on database salesdb to salesdbuser;
grant all on all tables in schema public to salesdbuser;
-- -h database-1.cgmmakgnsaxs.us-east-1.rds.amazonaws.com -p 5432 -U salesdb_user -d salesdb -W

create table if not exists category(
	catid smallint not null ,
	catgroup varchar(10),
	catname varchar(10),
	catdesc varchar(50));

\copy category(catid, catgroup, catname, catdesc) from 'D:/dloads/tickit/category_pipe.txt' WITH DELIMITER '|' CSV HEADER;

-- create rds_secret for database using user name and password
-- note down the arn for the secret 
-- arn:aws:secretsmanager:us-east-1:644466320815:secret:rds_postgres_secret-7SLQYB
-- create a policy rdsSecretAccess
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AccessSecret",
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetResourcePolicy",
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret",
                "secretsmanager:ListSecretVersionIds"
            ],
            "Resource": "arn:aws:secretsmanager:us-east-1:644466320815:secret:rds_postgres_secret-7SLQYB"
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetRandomPassword",
                "secretsmanager:ListSecrets"
            ],
            "Resource": "*"
        }
    ]
}
-- attach policy to myredshiftfullaccessrole
-- the arn for myredshiftfullaccessrole
-- arn:aws:iam::644466320815:role/myredshiftfullaccessrole
-- and verify redshift.amazonaws.com in Trusted entities under the Trust Relationships tab
CREATE EXTERNAL SCHEMA apg
FROM POSTGRES
DATABASE 'salesdb' SCHEMA 'tickitpg'
URI 'database-1.cgmmakgnsaxs.us-east-1.rds.amazonaws.com'
IAM_ROLE ' arn:aws:iam::644466320815:role/myredshiftfullaccessrole'
SECRET_ARN 'arn:aws:secretsmanager:us-east-1:644466320815:secret:rds_postgres_secret-7SLQYB';

/*
python3 -m venv rshift
Scripts\activate.bat
pip install psycopg2-binary
pip install boto3
pip install jupyterlab

import psycopg2

Host = 'my-cluster.cun6wpx0iczb.us-east-1.redshift.amazonaws.com'
Port = 5439
Database = 'salesdb'
User = 'awsuser'
Password = 'Admin98919'

conn = psycopg2.connect(host = Host, port = Port, 
database=Database, user=User, password = Password)

cursor = conn.cursor()

query_str = 'SELECT * FROM SALES LIMIT 10'

cursor.execute(query_str)
for rec in cursor:
    print(rec)

cursor.close()

cursor = conn.cursor()
truncate_statement = 'TRUNCATE TABLE category'
cursor.execute(truncate_statement)

aws_access_key_id = 'AKIAZMDJPUWXRRRRPQ5K'
aws_secret_access_key = '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'

import os
os.environ.setdefault('AWS_ACCESS_KEY_ID', aws_access_key_id)
os.environ.setdefault('AWS_SECRET_ACCESS_KEY', aws_secret_access_key)

import boto3
s3_client = boto3.client('s3')
s3_client.list_buckets()
s3_objects = s3_client.list_objects(
    Bucket = 'findbkt', 
    Prefix = 'cm/yr=2022/mnth=11'
)
s3_objects['Contents']
[obj['Key'] for obj in s3_objects['Contents']]

copy_stattement = """
copy category from 's3://tickit-samar/tickit/category_pipe.txt' 
iam_role 'arn:aws:iam::644466320815:role/service-role/AmazonRedshift-CommandsAccessRole-20230127T130200'
delimiter '|' region 'us-east-1'
"""
copy_statement_credentials = """
copy category from 's3://tickit-samar/tickit/category_pipe.txt' 
CREDENTIALS 'aws_access_key_id={aws_access_key_id};aws_secret_acces_key={aws_secret_access_key}'
delimiter '|' region 'us-east-1'
"""
*/

-- using boto3 to query redshift
-- create a secret using secrets manager, choose credentials for other database, enter user name as salesdb_user, password as Admin98919, choose postgresql as the databbase, in the server address enter endpoint without the port and the database, enter the database as salesdb and port as 5439 and save it give it a name
-- note down the secret arn
-- arn:aws:secretsmanager:us-east-1:644466320815:secret:redshift-salesdbuser-secret-164cb2
-- so for the secret we have entered host, port, database, user, password  - thus we should be able to connect to redshift using this secret arn
-- create policy and attach it to user permissions so that the code executed by the user can use the secret created
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AccessSecret",
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetResourcePolicy",
                "secretsmanager:GetSecretValue",
                "secretsmanager:DescribeSecret",
                "secretsmanager:ListSecretVersionIds"
            ],
            "Resource": "arn:aws:secretsmanager:us-east-1:644466320815:secret:redshift_secret-fI7A8Y"
        },
        {
            "Sid": "VisualEditor1",
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetRandomPassword",
                "secretsmanager:ListSecrets"
            ],
            "Resource": "*"
        }
    ]
}

create copy_to_rstable.py to copy the data from files in AWS s3 to Redshift table using Boto3

import os
import boto3
from botocore.waiter import WaiterModel
from botocore.waiter import create_waiter_with_client
from botocore.exceptions import WaiterError


def get_waiter_config(waiter_name):
    delay = int(os.environ.get('REDSHIFT_QUERY_DELAY'))
    max_attempts = int(os.environ.get('REDSHIFT_QUERY_MAX_ATTEMPTS'))

    #Configure the waiter settings
    waiter_config = {
      'version': 2,
      'waiters': {
        'DataAPIExecution': {
          'operation': 'DescribeStatement',
          'delay': delay,
          'maxAttempts': max_attempts,
          'acceptors': [
            {
              "matcher": "path",
              "expected": "FINISHED",
              "argument": "Status",
              "state": "success"
            },
            {
              "matcher": "pathAny",
              "expected": ["PICKED","STARTED","SUBMITTED"],
              "argument": "Status",
              "state": "retry"
            },
            {
              "matcher": "pathAny",
              "expected": ["FAILED","ABORTED"],
              "argument": "Status",
              "state": "failure"
            }
          ],
        },
      },
    }
    return waiter_config


def get_redshift_waiter_client(rsd_client):
    waiter_name = 'DataAPIExecution'

    waiter_config = get_waiter_config(waiter_name)
    waiter_model = WaiterModel(waiter_config)
    return create_waiter_with_client(waiter_name, waiter_model, rsd_client)


def copy_s3_to_rstable(bucket_name, secret_arn, table_name, data_file):
    rsd_client = boto3.client('redshift-data')

    rs_copy_command=f'''
    copy {table_name} from 's3://{bucket_name}/tickit/{data_file}' 
    iam_role 'arn:aws:iam::644466320815:role/service-role/AmazonRedshift-CommandsAccessRole-20230127T130200'
    delimiter '|' region 'us-east-1';
'''
    rs_copy_command_id = rsd_client.execute_statement(
        ClusterIdentifier = 'my-cluster',
        Database = 'salesdb',
        Sql = rs_copy_command,
        SecretArn = secret_arn
    )['Id']
    custom_waiter = get_redshift_waiter_client(rsd_client)
    try:
        custom_waiter.wait(Id=rs_copy_command_id)    
    except WaiterError as e:
        print (e)
    return rsd_client.describe_statement(Id=rs_copy_command_id)['Status']


# add the lambda handler code
import boto3
from copy_to_rstable import copy_s3_to_rstable


def lambda_handler(event, context):
    print(f'boto3 version: {boto3.__version__}')
    try:
        bucket_name = event['Bucket']
        secret_arn = event['SecretArn']
        table_name = event['TableName']
        data_file = event['DataFile']
        copy_res = copy_s3_to_rstable(bucket_name, secret_arn, table_name, data_file)
    except:
        raise
    return  {
        'statusCode': 200,
        'statement_status': copy_res
    }

# go to the lambda configuration tab and add permissions by creating an inline policy and allowing list and read permissions in secrets manager to resource secret arn - get it from the secrets manager or refer to the arn copied above earlier
# attach the AmazonRedshiftDataFullAccessPolicy
