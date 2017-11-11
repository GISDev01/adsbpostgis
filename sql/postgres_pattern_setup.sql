CREATE TABLE aircraftpatterns (
  mode_s_hex      TEXT,
  report_location GEOGRAPHY(Point, 4326),
  longitude83     DOUBLE PRECISION,
  latitude83      DOUBLE PRECISION,
  report_epoch    INTEGER,
  reporter        TEXT,
  itinerary_id    TEXT
);


ALTER TABLE aircraftpatterns
  OWNER TO postgres;