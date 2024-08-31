git clone https://github.com/big-data-europe/docker-hive.git
cd docker-hive

docker-compose up -d

docker cp C:\Work\Spark\HomeWork\3\airports.csv docker-hive-hive-server-1:/opt
docker cp C:\Work\Spark\HomeWork\3\flights.csv docker-hive-hive-server-1:/opt

docker-compose exec hive-server bash

hdfs dfs -put -f /opt/airports.csv /opt/flights.csv /user/hive;

/opt/hive/bin/beeline -u jdbc:hive2://localhost:10000

create database hivetest;

use hivetest;

drop table if exists airports;
CREATE TABLE airports (
airport_id int,
city string,
state string,
name string
)
COMMENT 'Airports table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hive/airports.csv' OVERWRITE INTO TABLE hivetest.airports;

drop table if exists flights;
CREATE TABLE flights (
DayofMonth int,
DayOfWeek int,
Carrier string,
OriginAirportID int,
DestAirportID int,
DepDelay int,
ArrDelay int)
COMMENT 'Airports table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hive/flights.csv' OVERWRITE INTO TABLE hivetest.flights;

drop table if exists CarrierDelay;
CREATE TABLE CarrierDelay (
DayofMonth int,
DayOfWeek int,
Carrier string,
AvgDepDelay int,
AvgArrDelay int)
COMMENT 'Average dalay on Carrier by days'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

insert into CarrierDelay
Select DayofMonth, DayofWeek, Carrier, Avg(DepDelay), Avg(ArrDelay) 
From hivetest.flights
Group by DayofMonth, DayofWeek, Carrier;

drop table if exists InterStateFlights;
CREATE TABLE InterStateFlights (
OriginState string,
DestState string,
allFlights int)
COMMENT 'Amount of interstate flights'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

insert into InterStateFlights
Select a1.state OriginState, a2.state DestState, count(*) allFlights
from hivetest.flights fl
inner join hivetest.airports a1
on fl.OriginAirportID = a1.airport_id
inner join hivetest.airports a2
on fl.DestAirportID = a2.airport_id
where a1.state <> a2.state
Group by a1.state, a2.state;

drop table if exists StateFlights;
CREATE TABLE StateFlights (
state string,
allFlights int)
COMMENT 'Amount of state flights'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

insert into StateFlights
select state, count(*) allFlights
from
(Select a1.state
from hivetest.flights fl
inner join hivetest.airports a1
on fl.OriginAirportID = a1.airport_id
union all
Select a2.state
from hivetest.flights fl
inner join hivetest.airports a2
on fl.DestAirportID = a2.airport_id
) fall
Group by state;

drop table if exists AirportFlights;
CREATE TABLE AirportFlights (
AirportName string,
allFlights int)
COMMENT 'Amount of airport flights'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

insert into AirportFlights
select name, count(*) allFlights
from
(Select a1.name
from hivetest.flights fl
inner join hivetest.airports a1
on fl.OriginAirportID = a1.airport_id
union all
Select a2.name
from hivetest.flights fl
inner join hivetest.airports a2
on fl.DestAirportID = a2.airport_id
) fall
Group by name;

drop view if exists Top80Airports ;
create view Top80Airports 
COMMENT 'TOP 80 percent airports by flights'
as
Select AirportName, allFlights
from (
Select AirportName, allFlights, 
Sum(allFlights) over() totalFlights, 
Sum(allFlights) over(Order by allFlights desc RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) sumFlights
From AirportFlights ) fall
Where sumFlights/totalFlights <= 0.8
Order by allFlights desc;
