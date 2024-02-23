use crate::lantern_logger::Logger;
use crate::lantern_utils::quote_ident;
use postgres::Transaction;

use super::{AnyhowVoidResult, LANTERN_INTERNAL_SCHEMA_NAME};

// Will create a codebook table add neccessary indexes and add PQVEC column into target table
pub fn setup_tables<'a>(
    transaction: &mut Transaction<'a>,
    full_table_name: &str,
    full_codebook_table_name: &str,
    pq_column_name: &str,
    logger: &Logger,
) -> AnyhowVoidResult {
    transaction.batch_execute(&format!(
        "
             CREATE UNLOGGED TABLE {full_codebook_table_name} (subvector_id INT, centroid_id INT, c REAL[]);
             ALTER TABLE {full_table_name} ADD COLUMN {pq_column_name} PQVEC;
             CREATE INDEX ON {full_codebook_table_name} USING BTREE(subvector_id, centroid_id);
             CREATE INDEX ON {full_codebook_table_name} USING BTREE(centroid_id);
        ",
        pq_column_name = quote_ident(&pq_column_name)
    ))?;
    logger.info(&format!(
        "{full_codebook_table_name} table and {pq_column_name} column created successfully"
    ));
    Ok(())
}

// Setup triggers to autoamtically compress new inserted/updated vectors
pub fn setup_triggers<'a>(
    transaction: &mut Transaction<'a>,
    full_table_name: &str,
    full_codebook_table_name: &str,
    pq_column: &str,
    column: &str,
    distance_metric: &str,
    splits: usize,
) -> AnyhowVoidResult {
    // Setup triggers for new data
    let name_hash = md5::compute(format!("{}{}", full_table_name, pq_column));
    let insert_trigger_name = format!("_pq_trigger_in_{:x}", name_hash);
    let update_trigger_name = format!("_pq_trigger_up_{:x}", name_hash);
    let trigger_fn_name = format!("{LANTERN_INTERNAL_SCHEMA_NAME}._set_pq_col_{:x}", name_hash);

    transaction.batch_execute(&format!("
      DROP TRIGGER IF EXISTS {insert_trigger_name} ON {full_table_name};
      DROP TRIGGER IF EXISTS {update_trigger_name} ON {full_table_name};

      CREATE OR REPLACE FUNCTION {trigger_fn_name}()
          RETURNS trigger
          LANGUAGE plpgsql AS
      $body$
        BEGIN
          IF NEW.{column} IS NULL THEN
            NEW.{pq_column} := NULL;
          ELSE
            NEW.{pq_column} := {LANTERN_INTERNAL_SCHEMA_NAME}.quantize_vector(NEW.{column}, {splits}, '{full_codebook_table_name}'::regclass, '{distance_metric}');
          END IF;
          RETURN NEW;
        END
      $body$;

      CREATE TRIGGER {insert_trigger_name} BEFORE INSERT ON {full_table_name} FOR EACH ROW EXECUTE FUNCTION {trigger_fn_name}();
      CREATE TRIGGER {update_trigger_name} BEFORE UPDATE OF {column} ON {full_table_name} FOR EACH ROW EXECUTE FUNCTION {trigger_fn_name}();
    ", pq_column=quote_ident(pq_column), column=quote_ident(column) ))?;
    Ok(())
}

pub fn make_codebook_logged_and_readonly<'a>(
    transaction: &mut Transaction<'a>,
    full_codebook_table_name: &str,
) -> AnyhowVoidResult {
    transaction.batch_execute(&format!("
    ALTER TABLE {full_codebook_table_name} SET LOGGED;
    CREATE TRIGGER readonly_guard BEFORE INSERT OR UPDATE OR DELETE ON {full_codebook_table_name} EXECUTE PROCEDURE {LANTERN_INTERNAL_SCHEMA_NAME}.forbid_table_change();
    "))?;
    Ok(())
}
