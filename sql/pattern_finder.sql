DROP FUNCTION adsb.public.find_pattern_num(modeshex CHAR);

create function adsb.public.find_pattern_num(
    IN  modeshex        CHAR,
    OUT patternnumber   int,
    OUT patterngeometry geometry,
    OUT patternstartend geometry,
    OUT patterncentroid geometry
    )
  RETURNS SETOF record AS
$BODY$

-- Iterate through the points for this specific mode_s_hex_code, building a line as we go
-- If the line creates a pattern then we count a it and start over building a new line
--     add the intersection point to the returning recordset
--     add the centroid of the pattern to the resulting recordset
-- pass in the ID of the mode s that we want to calc its patterns
--   SELECT * FROM find_pattern_count(mode_s);


DECLARE
    rPoint              RECORD;
    gSegment            geometry = NULL;
    gLastPoint          geometry = NULL;
    gPatternPolygon     geometry = NULL;
    gIntersectionPoint  geometry = NULL;
    gPatternCentroid    geometry = NULL;
    iPatterns           integer := 0;
BEGIN
    -- for each line segment in Point Path
    FOR rPoint IN
        WITH
            pts as (
            -- cast geography to geometry
                SELECT report_location::geometry as geom,row_number() OVER () as rownum
                FROM adsb.public.aircraftreports
                WHERE mode_s_hex=modeshex
                -- get 2 points at a time to for each line segment over the path(s)
                ORDER BY 2)
            --
            SELECT ST_AsText(ST_MakeLine(ARRAY[a.geom, b.geom])) AS geom, a.rownum, b.rownum
            FROM pts as a, pts as b
            WHERE a.rownum = b.rownum-1 AND b.rownum > 1

        LOOP
        raise notice 'rPoint: %', rPoint;

        -- if this is the start of a new line
        -- then start the segment, otherwise add the point to the existing segment
        if gSegment is null then
            -- add this point as the first point of this new segment
            gSegment=rPoint.geom;

        -- in the middle of creating a segment, and this will detect 2 points in succession
        -- that have the same geometry - skip this point (don't add to this segment) if this happens
        elseif rPoint.geom::geometry=gLastPoint::geometry then
          -- do not add this point to the segment because it is at the same location as the last point in the segment

        else
          -- a segment is in progress, so we add this point to the line
          gSegment=ST_Makeline(gSegment,rPoint.geom);
        end if;

        -- ST_BuildArea will return true if the line segment is noded and closed
        -- we must also flatten the line to 2D
        -- let's also make sure that there are more than three points in our line in order to define a pattern
        gPatternPolygon=ST_BuildArea(ST_Node(ST_Force2D(gSegment)));


        if gPatternPolygon is not NULL and ST_Numpoints(gSegment) > 3 then
          -- we found the pattern that we're checking for as we loop through the points
          iPatterns:=iPatterns+1;

          -- get the intersection point (start/end)
          gIntersectionPoint=ST_Intersection(gSegment::geometry,rPoint.geom::geometry);

          -- get the centroid of the pattern
          gPatternCentroid=ST_Centroid(gPatternPolygon);
          RAISE NOTICE 'gPatternCentroid: %.', gPatternCentroid;


          -- start building a new line
          gSegment=null;

          PATTERNNUMBER   := iPatterns;
          PATTERNGEOMETRY := gPatternPolygon;
          PATTERNSTARTEND := gIntersectionPoint;
          PATTERNCENTROID := gPatternCentroid;

          RETURN NEXT;
        end if;
        -- keep track of last segment
        gLastPoint=rPoint.geom;
    END LOOP;
    RAISE NOTICE 'Total patterns detected: %.', iPatterns;
END;
$BODY$
  LANGUAGE plpgsql STABLE
  COST 1000
  ROWS 10000;