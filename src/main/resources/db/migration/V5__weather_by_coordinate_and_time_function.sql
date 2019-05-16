CREATE OR REPLACE FUNCTION findByCoordinatesAndTime(accidentTime bigint, lat double precision, lon double precision)
    RETURNS table
            (
                temperature DOUBLE PRECISION,
                pressure    DOUBLE PRECISION,
                humidity    DOUBLE PRECISION,
                phenomenon  varchar(255),
                windspeed   DOUBLE PRECISION,
                visibility  DOUBLE PRECISION
            )
AS
$$
DECLARE
    stationId   INT;
    geometryStr VARCHAR(255);
BEGIN
    select concat('SRID=0;POINT(', lat, ' ', lon, ')') into geometryStr;

    SELECT id into stationId
    from station
    order by ST_Distance(
                     station.geom,
                     geometryStr::geometry
                 )
    limit 1;

    RETURN QUERY SELECT weather.temperature,
                        weather.pressure,
                        weather.humidity,
                        weather.phenomenon,
                        weather.windspeed,
                        weather.visibility
                 FROM (
                          (
                              SELECT *
                              FROM weather
                              WHERE datetime >= accidentTime
                                and weather.station_fk = stationId
                              ORDER BY ABS(datetime - accidentTime)
                              LIMIT 1
                          )
                          UNION ALL
                          (
                              SELECT *
                              FROM weather
                              WHERE datetime < accidentTime
                                and weather.station_fk = stationId
                              ORDER BY ABS(datetime - accidentTime)
                              LIMIT 1
                          )
                      ) AS weather
                 ORDER BY ABS(datetime - accidentTime)
                 LIMIT 1;

END;
$$ LANGUAGE 'plpgsql';
