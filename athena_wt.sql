-- create a bucket in s3 named nfindb
-- in that create a folder named cmdata
-- upload cm03JAN2022bhav.csv.gz and cm04JAN2022bhav.csv.gz to that folder
-- in glue add a database by the name nfindb
-- in athena select the nfindb database and run a create cmdtable query

create external table if not exists cmdtable (
  symbol string,
  series string , 
  openpr decimal,
  high decimal, 
  low decimal,
  closepr decimal, 
  last decimal, 
  prevclose decimal,
  tottrdqty int, 
  tottrdval decimal, 
  timetrade string,
  tottrades int, 
  isin string)
  row format delimited
  fields terminated by ','
  location 's3://nfindbkt/cmdata'
  tblproperties ('skip.header.line.count'='1');

-- verify that the table is created and run a select query against it
select * from nfindb.cmdtable limit 5;

-- create a partitioned table
-- add data to a tree like structure with yr 2022, month 1
--  for x in *JAN2022*;do aws s3api put-object --bucket nfindbkt --key cmdpart/yr=2022/mnth=1/$x --body $x --profile s3user;done
-- execute the create table statement given below
create external table if not exists cmdtablepart (
  symbol string,
  series string , 
  openpr decimal,
  high decimal, 
  low decimal,
  closepr decimal, 
  last decimal, 
  prevclose decimal,
  tottrdqty int, 
  tottrdval decimal, 
  timetrade string,
  tottrades int, 
  isin string)
  partitioned by (yr int, mnth int)
  row format delimited
  fields terminated by ','
  location 's3://nfindbkt/cmdpart'
  tblproperties ('skip.header.line.count'='1');

  -- check partitions
  show partitions nfindb.cmdtablepart;
  -- do msck repair table so that partitions show up
  msck repair table nfindb.cmdtablepart;
  -- select and select using partition
  select * from nfindb.cmdtablepart limit 10;
  select * from nfindb.cmdtablepart where yr = 2022 and mnth = 1 limit 10;

  -- add data for month 2 and month 3
  --  for x in *FEB2022*;do aws s3api put-object --bucket nfindbkt --key cmdpart/yr=2022/mnth=2/$x --body $x --profile s3user;done
  --  for x in *MAR2022*;do aws s3api put-object --bucket nfindbkt --key cmdpart/yr=2022/mnth=3/$x --body $x --profile s3user;done

  -- execute msck repair table so that partitions are refreshed
  msck repair table nfindb.cmdtablepart;
  show partitions nfindb.cmdtablepart;
  -- find out month wise counts to confirm
  select mnth, count(*) as mnthrecs from nfindb.cmdtablepart
  group by mnth
  order by mnth;

-- for x in *JAN2022*;do aws s3api put-object --bucket nfindbkt --key cmclbktd/yr=2022/mnth=1/$x --body $x --profile s3user;done
create external table if not exists cmbktbl(
  symbol string,
  series string , 
  openpr decimal,
  high decimal, 
  low decimal,
  closepr decimal, 
  last decimal, 
  prevclose decimal,
  tottrdqty int, 
  tottrdval decimal, 
  timetrade string,
  tottrades int, 
  isin string)
  clustered by (timetrade) into 30 buckets
  row format delimited
  fields terminated by ','
  location 's3://nfindbkt/cmdata'
  tblproperties ('skip.header.line.count'='1');

CREATE TABLE bucketed_table WITH (
  bucketed_by = ARRAY['symbol'], 
  bucket_count = 4, format = 'PARQUET', 
  external_location ='s3://nfindbkt/buckettable'
) AS 
SELECT 
  symbol, sum(tottrdval) as tval
FROM 
  cmdtable
group by symbol;

  