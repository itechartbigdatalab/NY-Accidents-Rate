create table station
(
    id   int primary key,
    name varchar(255),
    geom geometry(point, 0)
);

CREATE table weather
(
    id          serial primary key,
    station_fk  int    not null references station (id),
    datetime    bigint not null,
    temperature double precision,
    pressure    double precision,
    humidity    double precision,
    phenomenon  varchar(255),
    windspeed   double precision,
    visibility  double precision
);

INSERT into station (id, name, geom)
values (1, 'Linden (airport)', ST_GeomFromText('POINT(40.61680555555556 -74.2352)', 0));

INSERT into station (id, name, geom)
values (2, 'Central Park', ST_GeomFromText('POINT(40.76872222222222 -73.97086111111112)', 0));

INSERT into station (id, name, geom)
values (3, 'Macau (airbase)', ST_GeomFromText('POINT(40.6397 -73.7789)', 0));
