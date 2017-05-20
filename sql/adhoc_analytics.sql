SELECT COUNT(*) FROM aircraftreports;
SELECT COUNT(*) FROM aircraftreports WHERE altitude < 1000;
SELECT COUNT(*) FROM aircraftreports WHERE altitude > 40000;
SELECT COUNT(*) FROM aircraftreports WHERE SUBSTR(mode_s_hex, 1, 1) = '~';

