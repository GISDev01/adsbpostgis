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
  currentSubsegment       RECORD;
  gSegment           GEOMETRY = NULL;
  gLastPoint         GEOMETRY = NULL;
  gPatternPolygon    GEOMETRY = NULL;
  gIntersectionPoint GEOMETRY = NULL;
  gPatternCentroid   GEOMETRY = NULL;
  iPatterns          INTEGER := 0;
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
  --AND
  --report_epoch < 1508975975)
  --ORDER BY report_epoch)

  SELECT
    ST_AsText(ST_MakeLine(ARRAY [a.geom, b.geom])) AS geom,
    a.rownum,
    b.rownum
  FROM pts AS a,
    pts AS b
  WHERE a.rownum = b.rownum - 1
        AND
        b.rownum > 1
    --ORDER BY a.report_epoch


  LOOP
    RAISE NOTICE 'Current 2-pt sub-segment: %', currentSubsegment;

    -- if this is the start of a new line
    -- then start the segment, otherwise add the point to the existing segment
    IF gSegment IS NULL
    THEN
      -- add this point as the first point of this new segment
      gSegment = currentSubsegment.geom;

      -- in the middle of creating a segment, and this will detect 2 points in succession
      -- that have the same geometry - skip this point (don't add to this segment) if this happens
    ELSEIF currentSubsegment.geom :: GEOMETRY = gLastPoint :: GEOMETRY
      THEN
    -- do not add this point to the segment because it is at the same location as the last point in the segment

    ELSE
      -- a segment is in progress, so we add this point to the line
      gSegment = ST_Makeline(gSegment, currentSubsegment.geom);
    END IF;

    -- ST_BuildArea will return true if the line segment is noded and closed
    -- we must also flatten the line to 2D
    -- let's also make sure that there are more than three points in our line in order to define a pattern
    --gPatternPolygon=ST_BuildArea(ST_Node(ST_Force2D(gSegment)));
    gPatternPolygon = ST_BuildArea(ST_Node(ST_Force2D(gSegment)));


    IF gPatternPolygon IS NOT NULL AND ST_Numpoints(gSegment) > 10
    THEN
      -- we found the pattern that we're checking for as we loop through the points
      iPatterns:=iPatterns + 1;

      -- get the intersection point (start/end)
      gIntersectionPoint = ST_Intersection(gSegment :: GEOMETRY, currentSubsegment.geom :: GEOMETRY);

      -- get the centroid of the pattern
      gPatternCentroid = ST_Centroid(gPatternPolygon);
      RAISE NOTICE 'gPatternCentroid: %, %', ST_X(gPatternCentroid), ST_Y(gPatternCentroid);

      -- start building a new line segment
      gSegment = NULL;

      PATTERNNUMBER   := iPatterns;
      PATTERNGEOMETRY := gPatternPolygon;
      PATTERNSTARTEND := gIntersectionPoint;
      PATTERNCENTROID := gPatternCentroid;

      RETURN NEXT;
    END IF;

    -- keep track of last point processed
    gLastPoint = currentSubsegment.geom;
  END LOOP;

  RAISE NOTICE 'Total patterns detected: %.', iPatterns;
END;
$BODY$

LANGUAGE plpgsql STABLE
COST 100
ROWS 1000;