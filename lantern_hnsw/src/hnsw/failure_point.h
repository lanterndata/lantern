#ifndef LDB_HNSW_FAILURE_POINT_H
#define LDB_HNSW_FAILURE_POINT_H

/*
 * Failure points implementation.
 *
 * An example on how to use from test/sql/hnsw_failure_point.sql.
 *
 * 1) Add this to CreateBlockMapGroup():
 *
      LDB_FAILURE_POINT_CRASH_IF_ENABLED("crash_after_buf_allocation");
 *
 * 2) Enable the failure point somewhere in the test:
 *
 *    SELECT _lantern_internal.failure_point_enable('CreateBlockMapGroup', 'crash_after_buf_allocation', 0);
 *
 * 3) Trigger the failure point, the output looks like this:
 *
 *    INFO:  Failure point (func=CreateBlockMapGroup name=crash_after_buf_allocation) has been triggered.
 *
 * 4) Now check that the failure actually happens, for example with validate_index():
 *
 *    SELECT _lantern_internal.validate_index('small_world_v_idx', false);
 *
 * 5) The output tells that the block is allocated, but it's not being used:
 *
 *    INFO:  validate_index() start for small_world_v_idx
 *    ERROR:  vi_blocks[48].vp_type == LDB_VI_BLOCK_UNKNOWN (but it should be known now)
 *
 *
 * Limitations
 *
 * 1) A single static per-process variable holds the state.
 * 2) Only one failure point active at a time is supported.
 * 3) The API is not thread-safe.
 */

#define LDB_FAILURE_POINT_IS_ENABLED(_name) \
    (LANTERN_FAILURE_POINTS_ARE_ENABLED && ldb_failure_point_is_enabled(__func__, (_name)))
#define LDB_FAILURE_POINT_CRASH_IF_ENABLED(_name) \
    if(LDB_FAILURE_POINT_IS_ENABLED(_name)) ldb_failure_point_crash()

void ldb_failure_point_enable(const char *func, const char *name, uint32 dont_trigger_first_nr);
bool ldb_failure_point_is_enabled(const char *func, const char *name);
void ldb_failure_point_crash(void);

#endif  // LDB_HNSW_FAILURE_POINT_H
