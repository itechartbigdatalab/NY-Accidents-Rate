CREATE OR REPLACE FUNCTION check_weather()
    RETURNS trigger AS $weather$
BEGIN
    IF new.phenomenon IS NULL OR new.phenomenon LIKE '' THEN
        new.phenomenon = 'Clear';
    END IF;

    RETURN new;
END;
$weather$ LANGUAGE 'plpgsql';

DROP TRIGGER check_weather ON weather;
CREATE TRIGGER check_weather
    BEFORE INSERT
    ON weather
FOR EACH ROW EXECUTE PROCEDURE check_weather();