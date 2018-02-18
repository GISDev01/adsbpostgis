DROP FUNCTION adsb.public.find_pattern_num(itineraryid CHAR );

-- Example usage: pass in the ID of the itinerary to check for patterns in the point data
--   SELECT * FROM find_pattern_count(itineraryid);

CREATE FUNCTION adsb.public.find_pattern_num(
  IN  itineraryid             CHAR,
  OUT patternnumber           INT,
  OUT patterngeometry         GEOMETRY,
  OUT patternstartend         GEOMETRY,
  OUT patterncentroid         GEOMETRY,
  OUT patternnumvertices      INT
)
  RETURNS SETOF RECORD AS
$BODY$

-- Iterate through all of the points for this specific mode_s_hex_code, building a line as we go.
-- The line is built using subsegments that are created between every pair of neighboring points.
-- If the line creates a pattern then we count it and start over building a new line from the next point

-- Each time a pattern is detected, 2 points are calculated and stored:
--     The intersection point at which the last point of the segment hit the line of the pattern.
--     The centroid of the pattern.


DECLARE
  currentSubsegment        RECORD;
  fullSegment              GEOMETRY = NULL;
  prevPoint                GEOMETRY = NULL;
  patternPoly              GEOMETRY = NULL;
  intersectPtOnFullSegment GEOMETRY = NULL;
  patternPolyCentroid      GEOMETRY = NULL;
  numVertices              INTEGER := 0;
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
        WHERE itinerary_id = itineraryid)

  SELECT
    ST_AsText(ST_MakeLine(ARRAY [a.geom, b.geom])) AS geom,
    a.rownum,
    b.rownum
  FROM pts AS a,
    pts AS b
  -- Work-around to get the current point and the next point in the correct order,
  -- since rownum sorted by timestamp
  WHERE a.rownum = b.rownum - 1
        AND
        b.rownum > 1


  LOOP
    RAISE NOTICE 'Current 2-pt sub-segment: %', currentSubsegment;

    -- If this is the start of a new full segment
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

    -- Only try to check for the pattern poly if we've built a segment of at least 50 points
    IF ST_Numpoints(fullSegment) > 50
    THEN
      -- ST_BuildArea will only return true if the full line segment is noded and closed
      -- Force 2D to remove the elevation (Z-) component during this pattern check
      patternPoly = ST_BuildArea(ST_Node(ST_Force2D(fullSegment)));

      IF patternPoly IS NOT NULL
      THEN
        -- we found the pattern that we're checking for as we iterate through the points
        numPatternsDetected:=numPatternsDetected + 1;

        -- get the intersection point (start/end) along the full segment when it self-intersected
        intersectPtOnFullSegment = ST_Intersection(fullSegment :: GEOMETRY, currentSubsegment.geom :: GEOMETRY);

        -- get the centroid of the pattern that is detected
        patternPolyCentroid = ST_Centroid(patternPoly);

        RAISE NOTICE 'patternPolyCentroid: %, %', ST_X(patternPolyCentroid), ST_Y(patternPolyCentroid);

        numVertices := ST_Numpoints(fullSegment);
        -- reset the full segment and start building a new full line segment with the next point in line
        fullSegment = NULL;

        PATTERNNUMBER           := numPatternsDetected;
        PATTERNGEOMETRY         := patternPoly;
        PATTERNSTARTEND         := intersectPtOnFullSegment;
        PATTERNCENTROID         := patternPolyCentroid;
        PATTERNNUMVERTICES      := numVertices;

        RETURN NEXT;
      END IF;
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