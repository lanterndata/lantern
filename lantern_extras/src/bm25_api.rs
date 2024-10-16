use pgrx::extension_sql_file;

extension_sql_file!("./bm25_api.sql", requires = [Bloom]);

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
pub mod tests {

    use pgrx::prelude::*;
    #[pg_test]
    fn test_bm25_api() -> spi::Result<()> {
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

        // Step 5: Verify that the BM25 table has been created
        Spi::run("SELECT * FROM documents_bm25;")?;

        // Now, test for the term 'apple' (stemmed to 'appl')
        let (term_freq_appl, doc_ids_appl, fqs_appl) = Spi::get_three::<i32, Vec<i32>, Vec<i32>>(
            "SELECT term_freq, doc_ids, fqs FROM documents_bm25 WHERE term = 'appl';",
        )?;

        // Assertions for 'apple'
        assert_eq!(term_freq_appl.unwrap(), 2); // 'appl' appears in two documents
        assert_eq!(doc_ids_appl.unwrap(), vec![1, 2]);
        assert_eq!(fqs_appl.unwrap(), vec![1, 2]);

        // Now, test for the term 'banana' (already stemmed as 'banana')
        let (term_freq_banana, doc_ids_banana, fqs_banana) =
            Spi::get_three::<i32, Vec<i32>, Vec<i32>>(
                "SELECT term_freq, doc_ids, fqs FROM documents_bm25 WHERE term = 'banana';",
            )?;

        // We expect 'banana' to appear in all 4 documents
        assert_eq!(term_freq_banana.unwrap(), 4); // 'banana' appears in four documents
        assert_eq!(doc_ids_banana.unwrap(), vec![1, 2, 3, 4]);
        assert_eq!(fqs_banana.unwrap(), vec![1, 1, 2, 1]);

        // Now, test for the term 'kiwi' (already stemmed as 'kiwi')
        let (term_freq_kiwi, doc_ids_kiwi, fqs_kiwi) = Spi::get_three::<i32, Vec<i32>, Vec<i32>>(
            "SELECT term_freq, doc_ids, fqs FROM documents_bm25 WHERE term = 'kiwi';",
        )?;

        // 'kiwi' appears in one document
        assert_eq!(term_freq_kiwi.unwrap(), 1); // 'kiwi' appears in one document
        assert_eq!(doc_ids_kiwi.unwrap(), vec![4]);
        assert_eq!(fqs_kiwi.unwrap(), vec![1]);

        let doc_lens = Spi::get_one::<i32>(
            "SELECT DISTINCT flat_doc_len FROM documents_bm25, UNNEST(doc_lens) AS flat_doc_len;",
        )?;
        assert_eq!(doc_lens.unwrap(), 3);

        // Verify that there's only one distinct document length
        let num_distinct_doc_lens = Spi::get_one::<i64>(
            "SELECT COUNT(DISTINCT doc_len) FROM documents_bm25, UNNEST(doc_lens) AS doc_len;",
        )?;
        assert_eq!(num_distinct_doc_lens.unwrap(), 1);

        //TODO: Add insert triggers on source table and test consolidate function

        Ok(())
    }
}
