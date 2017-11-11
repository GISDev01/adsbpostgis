CREATE TABLE aircraftpatterns (
  mode_s_hex              TEXT,
  pattern_centroid        GEOGRAPHY(Point, 4326),
  pattern_cent_long       DOUBLE PRECISION,
  pattern_cent_lat        DOUBLE PRECISION,
  report_epoch            INTEGER,
  reporter                TEXT,
  itinerary_id            TEXT
);


ALTER TABLE aircraftpatterns
  OWNER TO postgres;


CREATE INDEX mode_s_hex_idx
  ON aircraftpatterns USING BTREE (mode_s_hex);

CREATE INDEX pr_epoch
  ON aircraftpatterns USING BTREE (report_epoch);

CREATE INDEX pat_cent_loc
  ON aircraftpatterns USING GIST (pattern_centroid);

GRANT ALL ON TABLE aircraftpatterns TO postgres;

