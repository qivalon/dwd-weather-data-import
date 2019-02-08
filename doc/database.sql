-- Import of weather stations and measurement data from .csv files

set search_path to common, public;

drop table weatherstation cascade;
create table weatherstation (
  stationId serial primary key,
  fromDate date,
  toDate date,
  location geography,
  altitude integer not null,
  name varchar not null,
  state varchar,
  longitude double precision not null,
  latitude double precision not null
);
create index on weatherstation using gist (location);
copy weatherstation (stationId, fromDate, toDate, longitude, latitude, altitude, name, state) from
'/home/dirichs/Work/qivalon/workspace/weather-data-import/weatherstation.csv' with delimiter ';' csv header;
update weatherstation set location = st_point(longitude, latitude);
alter table weatherstation alter column location set not null;
alter table weatherstation drop longitude;
alter table weatherstation drop latitude;

drop table airtemperature;
create table airtemperature (
  stationId int not null,
  measurementTime timestamp not null,
  temperature double precision,
  relativeHumidity smallint,
  constraint airtemperature_stationId_fkey foreign key (stationId) references weatherstation (stationId)
);
create index on airtemperature (stationId);
create index on airtemperature (measurementTime);

copy airtemperature (stationId, measurementTime, temperature, relativeHumidity) from
'/home/dirichs/Work/qivalon/workspace/weather-data-import/air_temperature.csv' with delimiter ';' csv header;

drop table precipitation;
create table precipitation (
  stationId integer not null,
  measurementTime timestamp not null,
  height double precision,
  form smallint,
  constraint precipitation_stationId_fkey foreign key (stationId) references weatherstation (stationId)
);
create index on precipitation (stationId);
create index on precipitation (measurementTime);

copy precipitation (stationId, measurementTime, height, form) from
'/home/dirichs/Work/qivalon/workspace/weather-data-import/precipitation.csv' with delimiter ';' csv header;

drop table pressure;
create table pressure (
  stationId integer not null,
  measurementTime timestamp not null,
  pressureNN double precision,
  pressureStationHeight double precision,
  constraint pressure_stationId_fkey foreign key (stationId) references weatherstation (stationId)
);
create index on pressure (stationId);
create index on pressure (measurementTime);

copy pressure (stationId, measurementTime, pressureNN, pressureStationHeight) from
'/home/dirichs/Work/qivalon/workspace/weather-data-import/pressure.csv' with delimiter ';' csv header;

drop table wind;
create table wind (
  stationId integer not null,
  measurementTime timestamp not null,
  meanWindSpeed double precision,
  meanWindDirection smallint,
  constraint wind_stationId_fkey foreign key (stationId) references weatherstation (stationId)
);
create index on wind (stationId);
create index on wind (measurementTime);

copy wind (stationId, measurementTime, meanWindSpeed, meanWindDirection) from
'/home/dirichs/Work/qivalon/workspace/weather-data-import/wind.csv' with delimiter ';' csv header;

alter table weatherstation owner to dynaserv;
