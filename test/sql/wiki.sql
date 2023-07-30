
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS lanterndb;

\qecho
\set ON_ERROR_STOP on

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
-- sets default schema
SELECT pg_catalog.set_config('search_path', 'public', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

CREATE TABLE tsv_data (
    language text,
    page_url text,
    image_url text,
    page_title text,
    section_title text,
    hierarchical_section_title text,
    caption_reference_description text,
    caption_attribution_description text,
    caption_alt_text_description text,
    mime_type text,
    original_height integer,
    original_width integer,
    is_main_image boolean,
    attribution_passes_lang_id boolean,
    page_changed_recently boolean,
    context_page_description text,
    context_section_description text,
    id integer NOT NULL,
    context_page_description_ai vector(512),
    image_ai vector(512)
);

CREATE SEQUENCE tsv_data_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER TABLE ONLY tsv_data ALTER COLUMN id SET DEFAULT nextval('tsv_data_id_seq'::regclass);

ALTER TABLE ONLY tsv_data
    ADD CONSTRAINT tsv_data_pkey PRIMARY KEY (id);

\copy tsv_data FROM '/tmp/lanterndb/vector_datasets/tsv_wiki_sample.csv' DELIMITER E'\t';

-- introduce a WITH statement to round returned distances AFTER a lookup
with t as (select id, page_title,  context_page_description_ai <-> (select context_page_description_ai from tsv_data where id = 81386) as dist
 from tsv_data order by dist
 limit 10) select id, page_title, ROUND( dist::numeric, 2) from t;
CREATE INDEX index1 ON tsv_data USING hnsw (context_page_description_ai vector_l2_ops);
CREATE INDEX ON tsv_data USING hnsw (context_page_description_ai) with (ef = 100, ef_construction=150 , M=11, alg="hnswlib");
set enable_seqscan=false;

-- todo:: find a different way to ensure that the index used. "\set enable_seqscan=false;" is not enough
-- and, the following produces a different output on pg11
-- explain with t as (select id, page_title, context_page_description_ai <-> (select context_page_description_ai from tsv_data where id = 81386) as dist
--  from tsv_data order by dist limit 10) select id, page_title, ROUND( dist::numeric, 2) from t;

-- introduce a WITH statement to round returned distances AFTER a lookup so the index can be used
with t as (select id, page_title, context_page_description_ai <-> (select context_page_description_ai from tsv_data where id = 81386) as dist
 from tsv_data order by dist limit 10) select id, page_title, ROUND( dist::numeric, 2) from t;

-- test additional inserts on wiki table
drop index index1;
select count(*) from tsv_data;
INSERT INTO tsv_data(context_page_description_ai)
SELECT context_page_description_ai FROM tsv_data WHERE context_page_description_ai IS NOT NULL LIMIT 444;
select count(*) from tsv_data;