SET enable_seqscan = off;

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS lanterndb;

\qecho
\set ON_ERROR_STOP on


CREATE TABLE t (val vector(3));
-- todo::

DROP TABLE t;
