ALTER TABLE district
  ALTER COLUMN geom TYPE geometry(MULTIPOLYGON, 2163)
    USING ST_SetSRID(geom, 2163);

ALTER TABLE station
    ALTER COLUMN geom TYPE geometry(point, 2163)
    USING ST_SetSRID(geom, 2163);