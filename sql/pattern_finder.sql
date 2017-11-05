DROP FUNCTION adsb.public.find_pattern_num(modeshex CHAR );

CREATE FUNCTION adsb.public.find_pattern_num(
  IN  modeshex        CHAR,
  OUT patternnumber   INT,
  OUT patterngeometry GEOMETRY,
  OUT patternstartend GEOMETRY,
  OUT patterncentroid GEOMETRY
)
  RETURNS SETOF RECORD AS
$BODY$

-- Iterate through the points for this specific mode_s_hex_code, building a line as we go
-- we build the line using subsegments that are created between every pair of neighboring points
-- If the line creates a pattern then we count a it and start over building a new line
--     add the intersection point to the returning recordset
--     add the centroid of the pattern to the resulting recordset
-- pass in the ID of the mode s that we want to calc its patterns
--   SELECT * FROM find_pattern_count(mode_s);


DECLARE
  currentSubsegment        RECORD;
  fullSegment              GEOMETRY = NULL;
  prevPoint                GEOMETRY = NULL;
  patternPoly              GEOMETRY = NULL;
  intersectPtOnFullSegment GEOMETRY = NULL;
  patternPolyCentroid      GEOMETRY = NULL;
  numPatternsDetected      INTEGER := 0;
BEGIN
  FOR currentSubsegment IN
  WITH
      pts AS (
        SELECT
          report_location :: GEOMETRY AS geom,
          report_epoch,
          row_number()
          OVER (
            ORDER BY report_epoch )   AS rownum
        FROM adsb.public.aircraftreports
        WHERE mode_s_hex = modeshex)

  SELECT
    ST_AsText(ST_MakeLine(ARRAY [a.geom, b.geom])) AS geom,
    a.rownum,
    b.rownum
  FROM pts AS a,
    pts AS b
  -- Work-around to get the current point and the next point in order, since rownum sorted by timestamp
  WHERE a.rownum = b.rownum - 1
        AND
        b.rownum > 1


  LOOP
    RAISE NOTICE 'Current 2-pt sub-segment: %', currentSubsegment;

    -- if this is the start of a new full segment
    -- then start the full segment, otherwise add the point to the existing full segment
    IF fullSegment IS NULL
    THEN
      -- add this point as the first point of this new full segment
      fullSegment = currentSubsegment.geom;


    ELSEIF currentSubsegment.geom :: GEOMETRY = prevPoint :: GEOMETRY
      THEN
      -- in the middle of creating a segment, and this will detect 2 identical points in succession
      -- that have the same geometry, so we skip this point (don't add to this full segment)

    ELSE
      -- a full segment is already in progress, so we add this sub-segment to the full segment
      fullSegment = ST_Makeline(fullSegment, currentSubsegment.geom);
    END IF;

    -- ST_BuildArea will only return true if the full line segment is noded and closed
    patternPoly = ST_BuildArea(ST_Node(ST_Force2D(fullSegment)));


    IF patternPoly IS NOT NULL AND ST_Numpoints(fullSegment) > 10
    THEN
      -- we found the pattern that we're checking for as we iterate through the points
      numPatternsDetected:=numPatternsDetected + 1;

      -- get the intersection point (start/end) along the full segment when it self-intersected
      intersectPtOnFullSegment = ST_Intersection(fullSegment :: GEOMETRY, currentSubsegment.geom :: GEOMETRY);

      -- get the centroid of the pattern that is detected
      patternPolyCentroid = ST_Centroid(patternPoly);

      RAISE NOTICE 'patternPolyCentroid: %, %', ST_X(patternPolyCentroid), ST_Y(patternPolyCentroid);

      -- reset the full segment and start building a new full line segment with the next point in line
      fullSegment = NULL;

      PATTERNNUMBER   := numPatternsDetected;
      PATTERNGEOMETRY := patternPoly;
      PATTERNSTARTEND := intersectPtOnFullSegment;
      PATTERNCENTROID := patternPolyCentroid;

      RETURN NEXT;
    END IF;

    -- keep track of previous point processed
    prevPoint = currentSubsegment.geom;
  END LOOP;

  RAISE NOTICE 'Total patterns detected: %.', numPatternsDetected;
END;
$BODY$

LANGUAGE plpgsql STABLE
COST 100
ROWS 1000;