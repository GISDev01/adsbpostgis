SELECT *
FROM find_pattern_num('A8D33A');
-- A8D33A, ADAFB5

SELECT *
FROM aircraftreports
WHERE mode_s_hex = 'ADAFB5'
ORDER BY report_epoch, 2;

SELECT COUNT(*)
FROM aircraftreports;

SELECT COUNT(*)
FROM aircraftreports
WHERE altitude < 1000;

SELECT COUNT(*)
FROM aircraftreports
WHERE altitude > 40000;

SELECT COUNT(*)
FROM aircraftreports
WHERE SUBSTR(mode_s_hex, 1, 1) = '~';

SELECT *
FROM aircraftreports
WHERE aircraftreports.report_location
      &&
      ST_MakeEnvelope(
          40, -40,
          80, 40);


SELECT
  report_location :: GEOMETRY AS geom,
  report_epoch,
  row_number()
  OVER (
    ORDER BY report_epoch )   AS rownum
FROM adsb.public.aircraftreports
WHERE mode_s_hex = 'ADAFB5'
ORDER BY report_epoch;

