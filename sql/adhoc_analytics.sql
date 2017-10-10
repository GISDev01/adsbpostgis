SELECT COUNT(*) FROM aircraftreports50k;
SELECT COUNT(*) FROM aircraftreports50k WHERE altitude < 1000;
SELECT COUNT(*) FROM aircraftreports50k WHERE altitude > 40000;
SELECT COUNT(*) FROM aircraftreports50k WHERE SUBSTR(mode_s_hex, 1, 1) = '~';

