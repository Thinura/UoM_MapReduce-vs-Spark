DROP TABLE IF EXISTS DelayedFlights;
SHOW DATABASES;

CREATE TABLE DelayedFlights (
  Index int,
  Year int,
  Month int,
  DayofMonth int,
  DayOfWeek int,
  DepTime int,
  CRSDepTime int,
  ArrTime int,
  CRSArrTime int,
  UniqueCarrier string,
  FlightNum int,
  TailNum string,
  ActualElapsedTime int,
  CRSElapsedTime int,
  AirTime int,
  ArrDelay int,
  DepDelay int,
  Origin string,
  Dest string,
  Distance int,
  TaxiIn int,
  TaxiOut int,
  Cancelled int,
  CancellationCode string,
  Diverted int,
  CarrierDelay int,
  WeatherDelay int,
  NASDelay int,
  SecurityDelay int,
  LateAircraftDelay int
)

ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION 's3://airlinedelay/Input/';

describe DelayedFlights;

LOAD DATA INPATH '/flights/DelayedFlights-updated.csv' OVERWRITE INTO TABLE DelayedFlights;

SELECT * FROM DelayedFlights;
