CREATE DATABASE "AircraftReports"
WITH
OWNER = postgres
ENCODING = 'UTF8'
CONNECTION LIMIT = -1;

CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;

--
-- Name: aircraftreports; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE aircraftreports (
  hex             CHARACTER(6),
  squawk          CHARACTER(6),
  flight          CHARACTER(8),
  is_metric       BOOLEAN,
  is_MLAT         BOOLEAN,
  altitude        DOUBLE PRECISION,
  speed           DOUBLE PRECISION,
  vert_rate       DOUBLE PRECISION,
  bearing         INTEGER,
  messages_sent   INTEGER,
  report_location GEOGRAPHY(Point, 4326),
  longitude83     DOUBLE PRECISION,
  latitude83      DOUBLE PRECISION,
  report_epoch    INTEGER,
  reporter        CHARACTER(10),
  rssi            DOUBLE PRECISION,
  nucp            INTEGER,
  is_ground       BOOLEAN
);


ALTER TABLE aircraftreports
  OWNER TO postgres;

--
-- Name: TABLE aircraftreports; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON TABLE aircraftreports IS 'Reports of a plane''s position.';

--
-- Name: COLUMN aircraftreports.hex; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN aircraftreports.hex IS 'ICAO24 code that uniquely identifies aircraft.';

--
-- Name: COLUMN aircraftreports.squawk; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN aircraftreports.squawk IS 'Code used by local air traffic controllers to communicate with the planes they are controllong.';

--
-- Name: COLUMN aircraftreports.flight; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN aircraftreports.flight IS 'Flight number assigned to aircraft for this particular route/time';

--
-- Name: COLUMN aircraftreports."is_metric"; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN aircraftreports.is_metric IS 'Does this record have metric units (metres/kmh) for altitude & speed';

--
-- Name: COLUMN aircraftreports.is_MLAT; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN aircraftreports.is_MLAT IS 'Was this position report derived from multilateration';

--
-- Name: COLUMN aircraftreports.altitude; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN aircraftreports.altitude IS 'Height of aircraft';

--
-- Name: COLUMN aircraftreports.speed; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN aircraftreports.speed IS 'Speed of aircraft';

--
-- Name: COLUMN aircraftreports.vert_rate; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN aircraftreports.vert_rate IS 'Rate of descent/ascent in metres/feet per minute';

--
-- Name: COLUMN aircraftreports.bearing; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN aircraftreports.bearing IS 'Direction aircraft is heading';

--
-- Name: COLUMN aircraftreports.messages_sent; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN aircraftreports.messages_sent IS 'Number of messages received from this aircraft at the time of this report';

--
-- Name: COLUMN aircraftreports.report_location; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN aircraftreports.report_location IS 'Encoded lat/lon of report (for use in postgis functions)';

--
-- Name: COLUMN aircraftreports.report_epoch; Type: COMMENT; Schema: public; Owner: postgres
--

COMMENT ON COLUMN aircraftreports.report_epoch IS 'Timestamp of report as seconds from epoch.';

--
-- Name: hex_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX hex_idx
  ON aircraftreports USING BTREE (hex);

--
-- Name: pr_epoch; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX pr_epoch
  ON aircraftreports USING BTREE (report_epoch);

--
-- Name: rep_loc; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX rep_loc
  ON aircraftreports USING GIST (report_location);


GRANT ALL ON TABLE aircraftreports TO postgres;
-- GRANT SELECT, INSERT, DELETE, UPDATE ON TABLE aircraftreports TO aircraftreportadmin;
