--
-- PostgreSQL database dump
--

\restrict 1nNQVc4HURSBTblY3LGDGgZ3raUbbnLKDaq1GdAxptnUMsaJ6BU71AqcWdb9LPV

-- Dumped from database version 16.16 (Debian 16.16-1.pgdg13+1)
-- Dumped by pg_dump version 16.16 (Debian 16.16-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: la_requests; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.la_requests (
    source_city text,
    source_request_id text,
    record_id text NOT NULL,
    created_at timestamp without time zone,
    closed_at timestamp without time zone,
    status_original text,
    status_group text,
    category_original text,
    category_detail_original text,
    intake_channel_original text,
    responsible_unit_original text,
    address_text text,
    postal_code text,
    latitude double precision,
    longitude double precision,
    location_wkt text,
    resolution_time_hours double precision,
    temporal_order_valid boolean,
    closed_date_missing_for_closed_status boolean,
    date_parse_error boolean,
    coordinate_parse_error boolean,
    coordinates_within_world_bounds boolean,
    srnumber text,
    createddate timestamp without time zone,
    updateddate timestamp without time zone,
    actiontaken text,
    owner text,
    requesttype text,
    status text,
    requestsource text,
    createdbyuserorganization text,
    mobileos text,
    anonymous text,
    assignto text,
    servicedate timestamp without time zone,
    closeddate timestamp without time zone,
    addressverified text,
    approximateaddress text,
    address text,
    housenumber text,
    direction text,
    streetname text,
    suffix text,
    zipcode text,
    location text,
    tbmpage text,
    tbmcolumn text,
    tbmrow text,
    apc text,
    cd text,
    cdmember text,
    nc text,
    ncname text,
    policeprecinct text
);


--
-- Name: merged_requests; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.merged_requests (
    source_city text,
    record_id text NOT NULL,
    created_at timestamp without time zone,
    closed_at timestamp without time zone,
    status text,
    resolution_hours double precision,
    responsible_organization text,
    request_type text,
    submission_channel text,
    address text,
    council_district text,
    postal_code text,
    latitude double precision,
    longitude double precision,
    location text
);


--
-- Name: nyc_requests; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.nyc_requests (
    source_city text,
    source_request_id text,
    record_id text NOT NULL,
    created_at timestamp without time zone,
    closed_at timestamp without time zone,
    status_original text,
    status_group text,
    category_original text,
    category_detail_original text,
    intake_channel_original text,
    responsible_unit_original text,
    address_text text,
    postal_code text,
    latitude double precision,
    longitude double precision,
    location_wkt text,
    resolution_time_hours double precision,
    temporal_order_valid boolean,
    closed_date_missing_for_closed_status boolean,
    date_parse_error boolean,
    coordinate_parse_error boolean,
    coordinates_within_world_bounds boolean,
    unique_key text,
    created_date timestamp without time zone,
    closed_date timestamp without time zone,
    agency text,
    agency_name text,
    complaint_type text,
    descriptor text,
    descriptor_2 text,
    location_type text,
    incident_zip text,
    incident_address text,
    street_name text,
    cross_street_1 text,
    cross_street_2 text,
    intersection_street_1 text,
    intersection_street_2 text,
    address_type text,
    city text,
    landmark text,
    facility_type text,
    status text,
    due_date timestamp without time zone,
    resolution_description text,
    resolution_action_updated_date timestamp without time zone,
    community_board text,
    council_district text,
    police_precinct text,
    bbl text,
    borough text,
    x_coordinate_state_plane double precision,
    y_coordinate_state_plane double precision,
    open_data_channel_type text,
    park_facility_name text,
    park_borough text,
    vehicle_type text,
    taxi_company_borough text,
    taxi_pick_up_location text,
    bridge_highway_name text,
    bridge_highway_direction text,
    road_ramp text,
    bridge_highway_segment text,
    location text
);


--
-- Name: la_requests la_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.la_requests
    ADD CONSTRAINT la_requests_pkey PRIMARY KEY (record_id);


--
-- Name: merged_requests merged_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.merged_requests
    ADD CONSTRAINT merged_requests_pkey PRIMARY KEY (record_id);


--
-- Name: nyc_requests nyc_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.nyc_requests
    ADD CONSTRAINT nyc_requests_pkey PRIMARY KEY (record_id);


--
-- PostgreSQL database dump complete
--

\unrestrict 1nNQVc4HURSBTblY3LGDGgZ3raUbbnLKDaq1GdAxptnUMsaJ6BU71AqcWdb9LPV

