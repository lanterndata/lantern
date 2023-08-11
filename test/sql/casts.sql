CREATE EXTENSION IF NOT EXISTS lanterndb;

\qecho
\set ON_ERROR_STOP on

-- uvec8
SELECT '{1,1,1}'::uvec8, '{0,0,0}'::uvec8, '{-1, 0, 1, -1, 000}'::uvec8;

SELECT '{0.111,0.222222,-0.33333333, -0.42424242424242}'::float4[];
SELECT '{0.111,0.222222,-0.33333333, -0.42424242424242}'::uvec8;
SELECT '{0.111,0.22,0.33}'::uvec8(3);
SELECT '{-0.42,0.0000001,0.00002}'::float[3]::uvec8(3);
SELECT '{-0.42,0.0000001,0.00002}'::float[3]::uvec8(3)::float4[3];

-- vec8
SELECT '{.1, 0.33, .42, .55, -.42, -0.00001, -.1234567}'::vec8, '{.1, 0.33, .42, .55, -.42, -0.00001, -.1234567}'::uvec8;

SELECT ARRAY[1,.2,.3]::uvec8;
SELECT ARRAY(SELECT ROUND(RANDOM()) FROM generate_series(1,65534))::uvec8 \gset

\set ON_ERROR_STOP off
SELECT '{1,2,3}'::uvec8;
SELECT '{}'::uvec8;
SELECT 'abra'::uvec8;
SELECT '{"haha"}'::uvec8;
SELECT '{{.1,.2,.3},{.4,.5,.6}}'::uvec8;
SELECT '{{.1,.2,.3},{.4,.5,.6}}'::uvec8(4);
-- todo:: the next one gives a funky error message. Make them more informative
SELECT '{{.1,.2,.3},{.4,.5,.6}}'::uvec8[4];

SELECT '{0.111,NULL}'::uvec8(2);
SELECT '{.1, .2}'::uvec8(-2);
SELECT ARRAY(SELECT ROUND(RANDOM()) FROM generate_series(1,65536))::uvec8;

SELECT '{0.1,0.2,0.3}'::uvec8(3,3);
SELECT '{0.1,0.2,0.3}'::uvec8(2);
SELECT '{0.1,0.2,0.3}'::uvec8(3)::uvec8(2);
SELECT '{1,1,1}'::int[3]::uvec8(4);
SELECT '{1,1,1}'::int[3]::uvec8(3)::uvec8(4);
\set ON_ERROR_STOP on
