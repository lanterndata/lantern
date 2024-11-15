use crate::bloom::Bloom as PGBloom;
use crate::{BM25_DEFAULT_APPROXIMATION_THRESHHOLD, BM25_DEFAULT_B, BM25_DEFAULT_K1};
use fastbloom::BloomFilter;
use lantern_cli::utils::quote_ident;
use std::collections::HashMap;

use pgrx::datum::Internal;
use pgrx::{pg_sys, prelude::*, AnyElement};

#[derive(Copy, Clone, Default, Debug)]
#[allow(non_camel_case_types)]
pub struct bm25_agg_base;

const HASHMAP_DEFAULT_CAPACITY: usize = 1000;
// type used in doc_ids, fqs, lens array computations
// all arrays are cast to this type before computations
// todo:: q:: making this i64 does not seem to make things any slower.
// I would expect it to significantly slow things down because of the extra copy/cast/write cycle
type TargetInteger = i32;
#[derive(Debug)]
struct BM25InternalState {
    data: Option<HashMap<TargetInteger, f32>>,
    corpus_size: Option<u64>,
    avg_doc_len: Option<f32>,
    limit: Option<usize>,
    // bm25 algorithm constants
    k1: f32,
    b: f32,
    // approximate bm25 parameters
    blooms: Vec<(f32, BloomFilter)>,
}

impl Default for BM25InternalState {
    fn default() -> Self {
        BM25InternalState {
            data: Some(HashMap::with_capacity(HASHMAP_DEFAULT_CAPACITY)),
            limit: None,
            corpus_size: None,
            avg_doc_len: None,
            k1: BM25_DEFAULT_K1.get() as f32,
            b: BM25_DEFAULT_B.get() as f32,
            blooms: Vec::new(),
        }
    }
}

extension_sql!(
    "\
CREATE TYPE bm25result AS (
    doc_id int8,
    bm25 float4
);",
    name = "create_bm25result_type",
    bootstrap
);
// we want to avoid repeating this in every overload before
// but at the same time type Alias = ... does not seem to work with composite_type!
const BM25RESULT_COMPOSITE_TYPE: &str = "bm25result";
type BM25ResultSQLType = Vec<Option<pgrx::composite_type!('static, BM25RESULT_COMPOSITE_TYPE)>>;

#[pg_extern(immutable, parallel_safe)]
fn bm25_score(table_fqn: String, document: String, query: String) -> f32 {
    let document_stemmed =
        Spi::get_one::<Vec<String>>(&format!("SELECT text_to_stem_array('{}')", document))
            .expect("Failed to get stemmed document")
            .expect("Stemmed document was NULL");
    let table_fqn = table_fqn.to_string();
    let table_bm25_fqn = format!("{}_bm25", table_fqn);
    let (corpus_size, avg_doc_len) = match Spi::get_two::<i32, f32>(&format!("SELECT term_freq AS corpus_size, (doc_ids_len / 100.0)::real AS avg_doc_len FROM {} WHERE term IS NULL;",
        quote_ident(&table_bm25_fqn)))
        .expect("Failed to get corpus size") {
        (Some(corpus_size), Some(avg_doc_len)) => (corpus_size as u64, avg_doc_len),
        _ => panic!("Failed to get corpus size and avg doc len"),
    };
    let term_data: Vec<(String, f32)> = Spi::connect(|client| {
        client
            .select(
                &format!(
                    "SELECT term, term_freq FROM {} WHERE term = ANY(text_to_stem_array('{}'));",
                    quote_ident(&table_bm25_fqn),
                    query
                ),
                None,
                None,
            )
            .expect("Failed to select from _bm25 table")
            .into_iter()
            .map(|row| {
                let term = row
                    .get::<String>(1)
                    .expect("Failed to get term from _bm25 table")
                    .expect("term in _bm25 table was NULL");

                let term_freq =
                    row.get::<i32>(2)
                        .expect("Failed to get term_freq from _bm25 table")
                        .expect("term_freq in _bm25 table was NULL") as f32;
                (term, term_freq)
            })
            .collect()
    });
    let mut bm25 = 0.0;
    for (word, term_freq) in term_data {
        let mut fq = document_stemmed.iter().filter(|&x| x == &word).count() as f32;
        let mut doc_len = document_stemmed.len() as f32;

        if term_freq as i32 > BM25_DEFAULT_APPROXIMATION_THRESHHOLD.get() {
            fq = if fq > 1. { 1. } else { fq };
            doc_len = avg_doc_len;
        }
        bm25 += calculate_bm25(
            doc_len,
            fq,
            term_freq,
            corpus_size as u64,
            avg_doc_len,
            BM25_DEFAULT_K1.get() as f32,
            BM25_DEFAULT_B.get() as f32,
        );
    }
    bm25
}

#[inline(always)]
fn calculate_bm25(
    doc_len: f32,
    fq: f32,
    term_freq: f32,
    // passing these from the caller in place of reading from self since the caller can unwrap
    // once and avoid the cost of unwrapping in a potentially hot loop
    // this actualy makes a measureable difference in performance
    corpus_size: u64,
    avg_doc_len: f32,
    bm25_k1: f32,
    bm25_b: f32,
) -> f32 {
    let doc_len = doc_len as f32;
    let fq = fq as f32;
    let idf = ((corpus_size as f32 - term_freq + 0.5) / (term_freq + 0.5) + 1.).ln();
    assert!(idf >= 0.0, "idf must be non-negative");
    let bm25: f32 = idf
        * (
            (fq * (bm25_k1 + 1.0))
                / (fq + bm25_k1 * (1.0 - bm25_b + bm25_b * (doc_len / avg_doc_len)))
            //
        );
    if bm25.is_nan() {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_FLOATING_POINT_EXCEPTION,
            "Encountered NaN in BM25 score calculation",
            format!(
                "Error happenned when calculating bm25 with doc_len: {} fq: {}, idf: {}, ",
                doc_len, fq, idf
            )
        );
    }
    bm25
}

impl BM25InternalState {
    fn maybe_use_bloom_filter(
        &self,
        heap_row: &pgrx::heap_tuple::PgHeapTuple<AllocatedByRust>,
        avg_doc_len: f32,
        corpus_size: u64,
        term_freq: f32,
    ) -> Option<(f32, BloomFilter)> {
        // we need to try to read pgbloom first, since the mere act of trying to get an
        // array via get_by_index causes pgrx to detoast the row, which is expensive
        if let Ok(Some(pgbloom)) = heap_row.get_by_name::<PGBloom>("doc_ids_bloom") {
            let bloom: BloomFilter = pgbloom.into();

            let fq = 1.;
            let doc_len = avg_doc_len;
            let bm25 = calculate_bm25(
                doc_len,
                fq,
                term_freq,
                corpus_size,
                avg_doc_len,
                self.k1,
                self.b,
            );
            return Some((bm25, bloom));
        }
        return None;
    }

    fn state_base(&mut self, row: AnyElement) {
        match row.oid() {
            pg_sys::RECORDOID => {
                let heap_row =
                    unsafe { pgrx::heap_tuple::PgHeapTuple::from_datum(row.datum(), false) }
                        .unwrap();

                let avg_doc_len = self.avg_doc_len.unwrap();
                let corpus_size = self.corpus_size.unwrap();

                // TODO: if the column is not available, fall back to using the length of the
                // doc_ids column
                let term_freq =
                            heap_row.get_by_name::<i32>("term_freq")
                            .expect("column term_freq must be present. Required for efficiency, to avoid detoasting doc_ids")
                            .expect("column term_freq cannot be null") as f32;

                if self.data.as_mut().unwrap().len() > 100 {
                    // switch to bloom filter on common words, but only if we have collected some
                    // relevant data IDs as a baseline already
                    if let Some(bloom_tuple) =
                        self.maybe_use_bloom_filter(&heap_row, avg_doc_len, corpus_size, term_freq)
                    {
                        self.blooms.push(bloom_tuple);
                        return;
                    }
                }

                let mut column_arrays: Vec<pgrx::Array<TargetInteger>> =
                    ["doc_ids", "fqs", "doc_lens"]
                        .iter()
                        .map(|name| {
                            let toid = heap_row
                                .get_attribute_by_name(name)
                                .unwrap_or_else(|| panic!("Failed to get {}", name))
                                .1
                                .atttypid;

                            if toid != pg_sys::INT4ARRAYOID {
                                // todo:: make sure we limit number of times this is printed per query
                                warning!(
                                "bm25 row type causes a type cast, potentially hurting performance"
                            );
                            }

                            match toid {
                                // pg_sys::INT2ARRAYOID => h
                                //     .get_by_name::<pgrx::Array<i16>>(name)
                                //     .expect("Failed to get Vec<i16>")
                                //     .unwrap()
                                //     .into_iter()
                                //     .map(|x| x.unwrap() as TargetInteger)
                                //     .collect(),
                                pg_sys::INT4ARRAYOID => heap_row
                                    .get_by_name::<pgrx::Array<i32>>(name)
                                    .expect("Failed to get Vec<i32>")
                                    .unwrap(),
                                // .into_iter()
                                // .map(|x| x.unwrap() as TargetInteger)
                                // .collect(),
                                // pg_sys::INT8ARRAYOID => h
                                //     .get_by_name::<pgrx::Array<i64>>(name)
                                //     .expect("Failed to get Vec<i64>")
                                //     .unwrap()
                                //     .into_iter()
                                //     .map(|x| x.unwrap() as TargetInteger)
                                //     .collect(),
                                _ => panic!("Unexpected data type for {}", name),
                            }
                        })
                        .collect();
                let doc_ids = column_arrays.remove(0);
                let fqs = column_arrays.remove(0);
                let doc_lens = column_arrays.remove(0);

                self.data.as_mut().unwrap().reserve(doc_ids.len());

                let _word = heap_row
                    .get_by_name::<String>("term")
                    .expect("Failed to get term")
                    .unwrap();

                for (doc_id, (fq, doc_len)) in doc_ids
                    .iter_deny_null()
                    .zip(fqs.iter_deny_null().zip(doc_lens.iter_deny_null()))
                {
                    let bm25 = calculate_bm25(
                        doc_len as f32,
                        fq as f32,
                        term_freq as f32,
                        corpus_size,
                        avg_doc_len,
                        self.k1,
                        self.b,
                    );

                    self.data
                        .as_mut()
                        .unwrap()
                        .entry(doc_id)
                        .and_modify(|e| *e += bm25)
                        .or_insert(bm25);
                }
            }
            _ => error!("bm25_agg aggregate called with non-record type"),
        }
    }
    // currently never called since parallel implementation of the aggregate is not complete
    // fn combine(
    //     mut first: Self::State,
    //     mut second: Self::State,
    //     _fcinfo: pg_sys::FunctionCallInfo,
    // ) -> Self::State {
    //     let first_inner = unsafe { first.get_or_insert_default::<HashMap<i32, f32>>() };
    //     let second_inner = unsafe { second.get_or_insert_default::<HashMap<i32, f32>>() };
    //
    //     for (k, v) in second_inner.iter() {
    //         first_inner.entry(*k).and_modify(|e| *e += *v).or_insert(*v);
    //     }
    //     Internal::new(first_inner)
    // }
    fn finalize_base(&mut self) -> BM25ResultSQLType {
        let results = if let Some(limit) = self.limit {
            use binary_heap_plus::*;
            let bloom_limit = limit * 10;

            let mut heap =
                BinaryHeap::with_capacity_by(bloom_limit, |p1: &(i32, f32), p2: &(i32, f32)| {
                    p2.1.partial_cmp(&p1.1).unwrap_or_else(|| {
                        ereport!(
                            ERROR,
                            PgSqlErrorCode::ERRCODE_FLOATING_POINT_EXCEPTION,
                            "Encountered NaN in BM25 score calculation",
                            format!("Error happenned when comparing {:?}, {:?}", p1, p2)
                        );
                    })
                });

            self.data.take().unwrap().into_iter().for_each(|e| {
                if heap.len() < bloom_limit {
                    heap.push(e);
                } else if heap.peek().unwrap().1 < e.1 {
                    heap.pop();
                    heap.push(e);
                }
            });

            // TODO: why is this printed twice when I expect finalize to be called only once?
            // info!("avg bm25 is {}", avg_bm25);

            let mut results = heap
                .into_iter_sorted()
                .map(|(doc_id, bm25)| {
                    (
                        doc_id,
                        bm25 + self
                            .blooms
                            .iter()
                            .filter(|(_, b)| b.contains(&doc_id))
                            .map(|(bm25, _)| *bm25)
                            .sum::<f32>(),
                    )
                })
                .map(|(a, b)| (Some(a), Some(b)))
                .collect::<Vec<_>>();

            results.sort_unstable_by(|a, b| {
                b.1.unwrap().partial_cmp(&a.1.unwrap()).unwrap_or_else(|| {
                    ereport!(
                        ERROR,
                        PgSqlErrorCode::ERRCODE_FLOATING_POINT_EXCEPTION,
                        "Encountered NaN in BM25 score calculation",
                        format!("Error happenned when comparing {:?}, {:?}", a, b)
                    );
                })
            });
            results.truncate(limit);
            results
        } else {
            let mut results: Vec<_> = self
                .data
                .take()
                .unwrap()
                .into_iter()
                .map(|(doc_id, bm25)| (Some(doc_id), Some(bm25)))
                .collect();

            results.sort_unstable_by(|a, b| {
                b.1.unwrap().partial_cmp(&a.1.unwrap()).unwrap_or_else(|| {
                    ereport!(
                        ERROR,
                        PgSqlErrorCode::ERRCODE_FLOATING_POINT_EXCEPTION,
                        "Encountered NaN in BM25 score calculation",
                        format!("Error happenned when comparing {:?}, {:?}", a, b)
                    );
                })
            });
            results
        };

        return results
            .into_iter()
            .map(|(a, b)| {
                let mut bm25result =
                    PgHeapTuple::new_composite_type(BM25RESULT_COMPOSITE_TYPE).unwrap();
                bm25result.set_by_name("doc_id", a.unwrap() as i64).unwrap();
                bm25result.set_by_name("bm25", b.unwrap()).unwrap();
                Some(bm25result)
            })
            .collect();
    }
}
/// Calculate BM25 score for a given word and return the top 10 documents with the highest BM25 score.
/// The function takes Postgres rows of a specific format as input, representing frequency of a
/// particular word in the underlying data corpus.
/// Each row must contain the following fields:
/// - `word`: the word to calculate BM25 score for
/// todo:: get rid of doc_count argument since it can be trivially computed
/// - `doc_count`: the total number of documents containing the word
/// - `doc_ids`: an array of length `doc_count` of document IDs
/// - `fqs`: an array of of length `doc_count` of term frequencies
/// - `doc_lens`: an array of length `doc_count` of document lengths
/// The function also takes 3 additional arguments:
/// - `limit`: the number of top documents to return. The function is lot more performant if this
/// limit is spacefied and ones does not rely solely on outer SQL LIMIT query. Specifying this
/// argument allows the function optimize and materialize only the necessary number of results.
/// - `bm25_k1`: the BM25 k1 parameter
/// - `bm25_b`: the BM25 b parameter
/// The function returns an array of bm25result composite type (bm25result[]). The composite type
/// is defined as (bigint, real) and represents two columns: `doc_id` and `bm25`.
/// The `doc_id` column contains the document ID, and the `bm25` column contains the BM25 score.
/// The array is sorted by the BM25 score in descending order, and only the top `limit` documents are returned, if `limit` is specified.
///
// todo:: implement parallel version of this function similar to array_agg and string_agg
// done here: https://git.postgresql.org/gitweb/?p=postgresql.git;a=commitdiff;h=16fd03e956540d1b47b743f6a84f37c54ac93dd4
// Relevant docs on aggregates:
// https://www.postgresql.org/docs/current/sql-createaggregate.html
// https://www.postgresql.org/docs/current/xaggr.html
//
// Note: we need 3 overloaded versions below because aggregates do not support default arguments
#[pg_aggregate]
impl Aggregate for bm25_agg_base {
    type Args = AnyElement;
    type State = Internal;

    type Finalize = Vec<Option<pgrx::composite_type!('static, BM25RESULT_COMPOSITE_TYPE)>>;
    // type Finalize = BM25ResultSQLType;
    const PARALLEL: Option<ParallelOption> = Some(ParallelOption::Safe);
    // const ORDERED_SET: bool = true;
    const NAME: &'static str = "bm25_agg";

    #[pgrx(parallel_safe, immutable)]
    fn state(
        mut current: Self::State,
        args: Self::Args,
        _fcinfo: pg_sys::FunctionCallInfo,
    ) -> Self::State {
        let _a: pgrx::composite_type!('static, BM25RESULT_COMPOSITE_TYPE);
        let inner = unsafe { current.get_or_insert_default::<BM25InternalState>() };

        let row = args;

        inner.state_base(row);

        current
    }

    fn finalize(
        mut current: Self::State,
        _direct_arg: Self::OrderedSetArgs,
        _fcinfo: pg_sys::FunctionCallInfo,
    ) -> Self::Finalize {
        let state = unsafe { current.get_or_insert_default::<BM25InternalState>() };
        state.finalize_base()
    }
}

#[derive(Copy, Clone, Default, Debug)]
#[allow(non_camel_case_types)]
pub struct bm25_agg_limit;
#[pg_aggregate]
impl Aggregate for bm25_agg_limit {
    // Named arguments in postgres aggregates are not suported. See the link for details:
    // https://github.com/postgres/postgres/blob/65c310b310a613d86c1ba94891fa9972587e09fd/src/backend/parser/parse_func.c#L801-L817
    // tldr; is: problems/confusion with parser and planner
    //          (row, limit, corpus_size, avg_doc_len)
    type Args = (AnyElement, i32, i32, f32);
    type State = Internal;
    type Finalize = Vec<Option<pgrx::composite_type!('static, BM25RESULT_COMPOSITE_TYPE)>>;
    const PARALLEL: Option<ParallelOption> = Some(ParallelOption::Safe);
    // const ORDERED_SET: bool = true;
    const NAME: &'static str = "bm25_agg";

    #[pgrx(parallel_safe, immutable)]
    fn state(
        mut current: Self::State,
        args: Self::Args,
        _fcinfo: pg_sys::FunctionCallInfo,
    ) -> Self::State {
        let inner = unsafe { current.get_or_insert_default::<BM25InternalState>() };

        let (row, limit_count, corpus_size, avg_doc_len) = args;
        if limit_count < 0 {
            error!("bm25_agg aggregate called with negative limit");
        }
        if corpus_size <= 0 {
            error!("bm25_agg aggregate called with negative corpus_size");
        }
        if avg_doc_len < 0. {
            error!("bm25_agg aggregate called with negative avg_doc_len");
        }
        if avg_doc_len.is_nan() {
            error!("bm25_agg aggregate called with NaN avg_doc_len");
        }

        inner.limit = Some(limit_count as usize);
        inner.corpus_size = Some(corpus_size as u64);
        inner.avg_doc_len = Some(avg_doc_len);
        inner.state_base(row);

        current
    }

    fn finalize(
        mut current: Self::State,
        _direct_arg: Self::OrderedSetArgs,
        _fcinfo: pg_sys::FunctionCallInfo,
    ) -> Self::Finalize {
        let state = unsafe { current.get_or_insert_default::<BM25InternalState>() };
        state.finalize_base()
    }
}

#[derive(Copy, Clone, Default, Debug)]
#[allow(non_camel_case_types)]
pub struct bm25_agg_limit_bm25params;
#[pg_aggregate]
impl Aggregate for bm25_agg_limit_bm25params {
    // Named arguments in postgres aggregates are not suported. See the link for details:
    // https://github.com/postgres/postgres/blob/65c310b310a613d86c1ba94891fa9972587e09fd/src/backend/parser/parse_func.c#L801-L817
    // tldr; is: problems/confusion with parser and planner
    //          (row, limit, corpus_size, avg_doc_len, k1, b)
    type Args = (AnyElement, i32, i32, f32, f32, f32);
    type State = Internal;
    type Finalize = Vec<Option<pgrx::composite_type!('static, BM25RESULT_COMPOSITE_TYPE)>>;
    // type Finalize = BM25ResultSQLType;
    const PARALLEL: Option<ParallelOption> = Some(ParallelOption::Safe);
    // const ORDERED_SET: bool = true;
    const NAME: &'static str = "bm25_agg";

    #[pgrx(parallel_safe, immutable)]
    fn state(
        mut current: Self::State,
        args: Self::Args,
        _fcinfo: pg_sys::FunctionCallInfo,
    ) -> Self::State {
        let inner = unsafe { current.get_or_insert_default::<BM25InternalState>() };

        let (row, limit_count, corpus_size, avg_doc_len, k1, b) = args;
        if limit_count < 0 {
            error!("bm25_agg aggregate called with negative limit");
        }
        if corpus_size <= 0 {
            error!("bm25_agg aggregate called with negative corpus_size");
        }
        if avg_doc_len < 0. {
            error!("bm25_agg aggregate called with negative avg_doc_len");
        }
        if avg_doc_len.is_nan() {
            error!("bm25_agg aggregate called with NaN avg_doc_len");
        }

        inner.limit = Some(limit_count as usize);
        inner.corpus_size = Some(corpus_size as u64);
        inner.avg_doc_len = Some(avg_doc_len);
        inner.limit = Some(limit_count as usize);
        inner.k1 = k1;
        inner.b = b;
        inner.state_base(row);

        current
    }

    fn finalize(
        mut current: Self::State,
        _direct_arg: Self::OrderedSetArgs,
        _fcinfo: pg_sys::FunctionCallInfo,
    ) -> Self::Finalize {
        let state = unsafe { current.get_or_insert_default::<BM25InternalState>() };
        state.finalize_base()
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use itertools::Itertools;
    use pgrx::prelude::*;

    #[pg_test]
    fn test_bm25_agg_and_api() -> spi::Result<()> {
        // Step 1: Create the documents table
        Spi::run(
            "CREATE TEMP TABLE documents (
                doc_id INT,
                content TEXT
            );",
        )?;

        Spi::run(
            "INSERT INTO documents (doc_id, content) VALUES
                (1, 'apple banana orange'),
                (2, 'apple apple banana'),
                (3, 'banana banana orange'),
                (4, 'kiwi pineapple banana');",
        )?;

        // step the text column using the rust stemmer
        Spi::run(
            "ALTER TABLE documents ADD COLUMN stemmed_content TEXT[];
             UPDATE documents SET stemmed_content = text_to_stem_array(content);",
        )?;

        Spi::run(
            "SELECT create_bm25_table(
                table_name => 'documents',
                id_column => 'doc_id',
                index_columns => ARRAY['stemmed_content']
            );",
        )?;

        // Compute corpus size and average document length
        let corpus_size = Spi::get_one::<i64>("SELECT COUNT(*) FROM documents;")?.unwrap();
        let avg_doc_len =
            Spi::get_one::<f32>("SELECT AVG(cardinality(stemmed_content))::real FROM documents;")?
                .unwrap();
        // Set the limit for BM25 results
        let limit = 10;

        let bm25_query = "apple banana";
        // Now, execute the BM25 query
        assert!(corpus_size > 0, "Corpus size must be positive.");
        assert!(
            avg_doc_len > 0.0,
            "Average document length must be positive."
        );
        let sql_query = format!(
        "
        WITH terms AS (
            SELECT * FROM documents_bm25 WHERE term = ANY(text_to_stem_array('{bm25_query}')) ORDER BY cardinality(doc_ids) DESC
        ),
        agg AS (
            SELECT (unnest(bm25_agg(
                terms.*,
                {limit},
                {corpus_size},
                {avg_doc_len},
                1.2,   -- k1 parameter
                0.75   -- b parameter
                ORDER BY doc_ids_len ASC
            ))).* AS res FROM terms
        )
        SELECT agg.doc_id::INT, agg.bm25::REAL FROM agg ORDER BY bm25 DESC;
        ",
        bm25_query = bm25_query,
        limit = limit,
        corpus_size = corpus_size,
        avg_doc_len = avg_doc_len,
    );

        // Fetch results
        let results = Spi::get_two::<i32, f32>(sql_query.as_str())?;

        Spi::connect(|client| {
            // for execute
            use spi::Query;
            let mut res = client
                .prepare(sql_query.as_str(), None)
                .expect("Failed to prepare query")
                .execute(&client, None, None)
                .expect("Failed to execute query");
            while let Some(row) = res.next() {
                let doc_id = row.get::<i32>(1).unwrap_or(Some(42)).unwrap_or(44);
                let bm25 = row.get::<f32>(2).unwrap().unwrap();
                info!("doc_id: {}, bm25: {}", doc_id, bm25);
            }
        });

        // Perform assertions to check correctness
        // Since we know the test data and the expected behavior, we can write specific assertions
        // For example, we expect document 2 to have the highest BM25 score for 'apple banana'
        assert_eq!(
            results.0.unwrap(),
            2,
            "Expected Doc ID 2 to have the highest BM25 score."
        );
        assert!(results.1.unwrap() > 0.0, "BM25 score must be positive.");

        // insert more documents and check reindexing
        let content = "pomegranate pomegranate pomegranate";

        Spi::run_with_args(
            "INSERT INTO documents (doc_id, content, stemmed_content) VALUES
                (5, $1, text_to_stem_array($1::text));",
            Some(vec![(PgBuiltInOids::TEXTOID.oid(), content.into_datum())]),
        )?;

        let prev_len = Spi::get_one::<i64>("SELECT COUNT(*) FROM documents_bm25;")?.unwrap();
        Spi::run(
            "SELECT create_bm25_table(
                table_name => 'documents',
                id_column => 'doc_id',
                index_columns => ARRAY['stemmed_content'],
                drop_if_exists => TRUE
            );",
        )?;

        let new_len = Spi::get_one::<i64>("SELECT COUNT(*) FROM documents_bm25;")?.unwrap();
        // the added row had one new word, so the bm25 table must grow by 1
        assert_eq!(new_len, prev_len + 1);

        // Test some axuilary _bm25 table invariants

        let doc_ids_invariant = Spi::get_one::<bool>(
            "SELECT NOT EXISTS (SELECT 1 FROM documents_bm25 WHERE NOT cardinality(doc_ids) = cardinality((SELECT array_agg(DISTINCT e) FROM unnest( doc_ids) e)))",
        )?;
        assert!(
            doc_ids_invariant.unwrap(),
            "doc_ids must contain unique doc ids"
        );

        // Test for term frequency invariants
        let term_freq_invariant = Spi::get_one::<bool>(
            "SELECT NOT EXISTS (
                SELECT 1
                FROM (
                    SELECT term, cardinality(array_agg(DISTINCT term_freq)) AS uniq_term_freq_must_be_1,
                           (array_agg(DISTINCT term_freq))[1] AS any_term_freq_must_be_equal_to_next_col,
                           SUM(cardinality(doc_ids))
                    FROM documents_bm25
                    WHERE term_freq != cardinality(doc_ids)
                    GROUP BY term
                ) AS subquery
                WHERE uniq_term_freq_must_be_1 != 1
            );",
        )?;

        // note: this invariant needs to be modified once insertions become allowed
        assert!(
            term_freq_invariant.unwrap(),
            "Each term must have a unique term frequency equal to the cardinality of doc_ids"
        );
        // Test the bm25_score function
        // The bm25_score function calculates the BM25 score for the given query in the given
        // document
        Spi::connect(|client| {
            let q = client
                .select(
                    "SELECT bm25_score('documents', 'apple banana kiwi', 'apple'),
                            bm25_score('documents', 'apple banana kiwi', 'kiwi'),
                            bm25_score('documents', 'nonexistent_document', 'nonexistent_document');",
                    None,
                    None,
                )
                .expect("Failed to select from _bm25 table")
                .into_iter()
                .next()
                .expect("Failed to get result");

            let (bm25_apple, bm25_kiwi, bm25_nonexistent) = (1..=3)
                .map(|ind| {
                    q.get::<f32>(ind)
                        .expect("Failed to get bm25 score")
                        .expect("Bm25 is null")
                })
                .collect_tuple()
                .unwrap();
            info!(
                "values: {}, {}, {}",
                bm25_apple, bm25_kiwi, bm25_nonexistent
            );
            assert!(bm25_apple > 0.0, "BM25 score must be positive.");
            assert!(
                bm25_kiwi > bm25_apple,
                "term kiwi is more rare, so should have higher score"
            );
            assert!(
                bm25_nonexistent == 0.0,
                " bm25 score of a term that is not in the corpus should be zero"
            );
        });

        Ok(())
    }
}
