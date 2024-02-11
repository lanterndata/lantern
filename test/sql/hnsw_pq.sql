\ir utils/sift1k_array.sql
\ir utils/sift1k_array_query.sql
\ir utils/random_array.sql
\ir utils/calculate_recall.sql

SET client_min_messages=ERROR;

SELECT '{84,1,4,128,255}'::pqvec;
SELECT '{84,1,4,128,255}'::pqvec::INT[];
SELECT '{84,1,4,128,255}'::INT[]::pqvec;

\set ON_ERROR_STOP off
-- Test PQVec type
SELECT '{84,1,4,128,256}'::pqvec;
-- Verify wrong argument assertions
SELECT _lantern_internal.create_pq_codebook('sift_base1k'::regclass, 'nonexistant', 10, 32, 'l2sq', 0);
SELECT _lantern_internal.create_pq_codebook('sift_base1k'::regclass, 'v', 1001, 32, 'l2sq', 0);
SELECT _lantern_internal.create_pq_codebook('sift_base1k'::regclass, 'v', 10, 33, 'l2sq', 0);
SELECT _lantern_internal.create_pq_codebook('sift_base1k'::regclass, 'v', 10, 32, 'l2sqz', 0);
SELECT _lantern_internal.create_pq_codebook('sift_base1k'::regclass, 'v', 257, 32, 'l2sq', 0);
SELECT _lantern_internal.create_pq_codebook('sift_base1k'::regclass, 'v', 257, 0, 'l2sq', 0);
SELECT _lantern_internal.create_pq_codebook('sift_base1k'::regclass, 'v', 256, 0, 'l2sq', 10);
\set ON_ERROR_STOP on

-- This should create codebook[1][1][128]
SELECT _lantern_internal.create_pq_codebook('sift_base1k'::regclass, 'v', 1, 1, 'l2sq', 0) as codebook \gset
SELECT array_length(:'codebook'::REAL[][][], 1);
SELECT array_length(:'codebook'::REAL[][][], 2);
SELECT array_length(:'codebook'::REAL[][][], 3);

-- This should create codebook[1][10][128]
SELECT _lantern_internal.create_pq_codebook('sift_base1k'::regclass, 'v', 10, 1, 'l2sq', 0) as codebook \gset
SELECT array_length(:'codebook'::REAL[][][], 1);
SELECT array_length(:'codebook'::REAL[][][], 2);
SELECT array_length(:'codebook'::REAL[][][], 3);

-- This should create codebook[32][10][4]
SELECT _lantern_internal.create_pq_codebook('sift_base1k'::regclass, 'v', 10, 32, 'l2sq', 0) as codebook \gset
SELECT array_length(:'codebook'::REAL[][][], 1);
SELECT array_length(:'codebook'::REAL[][][], 2);
SELECT array_length(:'codebook'::REAL[][][], 3);


-- This should create codebook _lantern_internal._codebook_sift_base1k_v and add v_pq column in sift_base1k table with compressed vectors
-- The codebook will be codebook[32][50][4], so in the table there should be 32 distinct subvector ids each with 50 centroid ids
SELECT quantize_table('sift_base1k'::regclass, 'v', 50, 32, 'l2sq');
SELECT COUNT(DISTINCT subvector_id) FROM _lantern_internal._codebook_sift_base1k_v;
SELECT COUNT(DISTINCT centroid_id) FROM _lantern_internal._codebook_sift_base1k_v;
SELECT COUNT(*) FROM _lantern_internal._codebook_sift_base1k_v;
SELECT array_length(c, 1) FROM _lantern_internal._codebook_sift_base1k_v LIMIT 1;

-- Validate that table is readonly
\set ON_ERROR_STOP off
DELETE FROM _lantern_internal._codebook_sift_base1k_v WHERE centroid_id=1;
UPDATE _lantern_internal._codebook_sift_base1k_v SET centroid_id=2 WHERE centroid_id=1;
INSERT INTO _lantern_internal._codebook_sift_base1k_v (subvector_id, centroid_id, c) VALUES (1, 1, '{1,2,3,4}');

-- Validate that compressing invalid vector raises an error
SELECT dequantize_vector('{}'::pqvec, '_lantern_internal._codebook_sift_base1k_v'::regclass);
SELECT dequantize_vector('{1,2,3}'::pqvec, '_lantern_internal._codebook_sift_base1k_v'::regclass);
\set ON_ERROR_STOP on

-- Compression and Decompression
-- Verify that vector was compressed correctly when generating quantized column
SELECT v as v1 FROM sift_base1k WHERE id=1 \gset
SELECT v_pq as v1_pq FROM sift_base1k WHERE id=1 \gset
SELECT quantize_vector(:'v1', '_lantern_internal._codebook_sift_base1k_v'::regclass, 'l2sq') as compressed \gset
SELECT dequantize_vector(:'v1_pq', '_lantern_internal._codebook_sift_base1k_v'::regclass) as decompressed_1 \gset
SELECT dequantize_vector(:'compressed', '_lantern_internal._codebook_sift_base1k_v'::regclass) as decompressed_2 \gset
SELECT l2sq_dist(:'decompressed_1'::real[], :'decompressed_2'::real[]);
-- Vector operators work as usual on decompressed vectors:
SELECT dequantize_vector(:'v1_pq', '_lantern_internal._codebook_sift_base1k_v'::regclass) <-> dequantize_vector(:'compressed', '_lantern_internal._codebook_sift_base1k_v'::regclass);

-- Test recall for quantized vs non quantized vectors
ALTER TABLE sift_base1k ADD COLUMN v_pq_dec REAL[];
UPDATE sift_base1k SET v_pq_dec=dequantize_vector(v_pq, '_lantern_internal._codebook_sift_base1k_v');
-- Calculate recall over original vector
SELECT (calculate_table_recall('sift_base1k', 'sift_query1k', 'sift_truth1k', 'v', 10, 100) -
       calculate_table_recall('sift_base1k', 'sift_query1k', 'sift_truth1k', 'v_pq_dec', 10, 100)) as recall_diff \gset

SELECT :'recall_diff' < 0.2 as recall_diff_meets_threshold;

-- Verify that column triggers for insert and update are working correctly
INSERT INTO sift_base1k(id, v) VALUES (1001, random_array(128, 0.0, 5.0));
SELECT id FROM sift_base1k WHERE v_pq IS NULL;
SELECT v_pq::TEXT as old_pq FROM sift_base1k WHERE id=1001 \gset
UPDATE sift_base1k SET v=(SELECT v FROM sift_base1k WHERE id=1) WHERE id=1001;
SELECT v_pq::TEXT as new_pq FROM sift_base1k WHERE id=1001 \gset
SELECT :'old_pq' <> :'new_pq' as is_updated;
SELECT :'new_pq' = (SELECT v_pq::TEXT FROM sift_base1k WHERE id=1) as is_updated;

-- Verify that compressed column size is smaller than regular integer
SELECT pg_column_size(v_pq) as compressed_size, pg_column_size(v_pq::int[]) as int_size FROM sift_base1k LIMIT 1;

-- Verify that table can have multiple quantized vectors
SELECT quantize_table('sift_base1k'::regclass, 'v_pq_dec', 10, 32, 'l2sq');
SELECT COUNT(DISTINCT subvector_id) FROM _lantern_internal._codebook_sift_base1k_v_pq_dec;
SELECT COUNT(DISTINCT centroid_id) FROM _lantern_internal._codebook_sift_base1k_v_pq_dec;
SELECT COUNT(*) FROM _lantern_internal._codebook_sift_base1k_v_pq_dec;
SELECT array_length(c, 1) FROM _lantern_internal._codebook_sift_base1k_v_pq_dec LIMIT 1;

-- Test that resources are being cleared correctly
SELECT drop_quantization('sift_base1k'::regclass, 'v');
SELECT drop_quantization('sift_base1k'::regclass, 'v_pq_dec');
SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'sift_base1k';
SELECT table_name FROM information_schema.tables WHERE table_schema = '_lantern_internal';

-- Test quantization over subset of data
SELECT quantize_table('sift_base1k'::regclass, 'v', 10, 32, 'l2sq', 500);
SELECT COUNT(DISTINCT subvector_id) FROM _lantern_internal._codebook_sift_base1k_v;
SELECT COUNT(DISTINCT centroid_id) FROM _lantern_internal._codebook_sift_base1k_v;
SELECT COUNT(*) FROM _lantern_internal._codebook_sift_base1k_v;
SELECT array_length(c, 1) FROM _lantern_internal._codebook_sift_base1k_v LIMIT 1;

-- Test quantization with mixed case and schema qualified table name
SELECT id, v AS "v_New" into "sift_Base1k_NEW" FROM sift_base1k;
SELECT quantize_table('"public"."sift_Base1k_NEW"'::regclass, 'v_New', 10, 32, 'l2sq');
SELECT array_length(
              dequantize_vector(
                     quantize_vector(
                       (SELECT "v_New" FROM "sift_Base1k_NEW" WHERE id=1), 
                       '_lantern_internal."_codebook_sift_Base1k_NEW_v_New"'::regclass, 
                       'l2sq'),  
              '_lantern_internal."_codebook_sift_Base1k_NEW_v_New"'::regclass),
              1
       );
SELECT drop_quantization('"sift_Base1k_NEW"'::regclass, 'v_New');
