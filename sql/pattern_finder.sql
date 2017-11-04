DROP FUNCTION adsb.public.find_pattern_count(flightid int);

create function adsb.public.find_pattern_count(
    IN  flightid     int,
    OUT patternnumber   int,
    OUT patterngeometry geometry,
    OUT patternstartend geometry,
    OUT patterncentroid geometry
    )
  RETURNS SETOF record AS
$BODY$

-- partially from https://gis.stackexchange.com/questions/206815/seeking-algorithm-to-detect-circling-and-beginning-and-end-of-circle
-- iterate through the points, building a line as we go
--   If the line creates a loop then we count a loop and start over building a new line
--     add the intersection point to the returning recordset
--     add the centroid of the loop to the resulting recordset
-- pass in the flight ID of the flight that you wish to count its loops for example:
--   SELECT * FROM find_pattern_count(flightid);

