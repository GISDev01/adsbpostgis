
SELECT * FROM find_pattern_num('ADAFB5');
SELECT COUNT(*) FROM aircraftreports WHERE mode_s_hex='ADAFB5';

SELECT COUNT(*) FROM aircraftreports;
SELECT COUNT(*) FROM aircraftreports WHERE altitude < 1000;
SELECT COUNT(*) FROM aircraftreports WHERE altitude > 40000;
SELECT COUNT(*) FROM aircraftreports WHERE SUBSTR(mode_s_hex, 1, 1) = '~';

SELECT *
FROM   aircraftreports
WHERE  aircraftreports.report_location
    &&
    ST_MakeEnvelope (
        40, -40,
        80, 40)
