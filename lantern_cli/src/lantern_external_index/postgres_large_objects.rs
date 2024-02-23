use crate::types::AnyhowVoidResult;
use crate::lantern_utils::quote_ident;
use postgres::Transaction;
use postgres_types::Oid;
use std::{cmp, io};

pub struct LargeObject<'a> {
    pub transaction: Option<Transaction<'a>>,
    fd: Option<i32>,
    pub oid: Option<Oid>,
    index_path: String,
}

impl<'a> LargeObject<'a> {
    pub fn new(transaction: Transaction<'a>, index_path: &str) -> LargeObject<'a> {
        LargeObject {
            transaction: Some(transaction),
            oid: None,
            fd: None,
            index_path: index_path.to_owned(),
        }
    }

    pub fn create(&mut self) -> AnyhowVoidResult {
        let transaction = self.transaction.as_mut().unwrap();
        let lo_oid = transaction.query_one("SELECT pg_catalog.lo_create(0)", &[])?;
        let lo_oid: Oid = lo_oid.get(0);
        let fd = transaction.query_one("SELECT pg_catalog.lo_open($1, 131072)", &[&lo_oid])?;
        let fd: i32 = fd.get(0);
        self.fd = Some(fd);
        self.oid = Some(lo_oid);
        Ok(())
    }

    pub fn finish(
        self,
        table_name: &str,
        column_name: &str,
        index_name: Option<&str>,
        op_class: &str,
        ef: usize,
        ef_construction: usize,
        dim: usize,
        m: usize,
        pq: bool,
    ) -> AnyhowVoidResult {
        let mut transaction = self.transaction.unwrap();
        transaction.execute(
            "SELECT pg_catalog.lo_export($1, $2)",
            &[&self.oid.unwrap(), &self.index_path],
        )?;
        transaction.execute("SELECT pg_catalog.lo_unlink($1)", &[&self.oid.unwrap()])?;

        let mut idx_name = "".to_owned();

        if let Some(name) = index_name {
            idx_name = quote_ident(name);
            transaction.execute(&format!("DROP INDEX IF EXISTS {idx_name}"), &[])?;
        }

        transaction.execute(
            &format!("CREATE INDEX {idx_name} ON {table_name} USING lantern_hnsw({column_name} {op_class}) WITH (_experimental_index_path='{index_path}', pq={pq}, ef={ef}, dim={dim}, m={m}, ef_construction={ef_construction});", 
            index_path=self.index_path),
            &[],
        )?;

        transaction.batch_execute(&format!(
            "
                CREATE TEMPORARY TABLE _rm_lantern_index_output(output TEXT);
                COPY _rm_lantern_index_output FROM PROGRAM 'rm -rf {path}'",
            path = &self.index_path
        ))?;

        transaction.commit()?;
        Ok(())
    }
}

impl<'a> io::Write for LargeObject<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let cap = cmp::min(buf.len(), i32::MAX as usize);
        let transaction = self.transaction.as_mut().unwrap();
        let res = transaction.execute(
            "SELECT pg_catalog.lowrite($1, $2)",
            &[&self.fd.unwrap(), &&buf[..cap]],
        );

        if let Err(e) = res {
            return Err(io::Error::new(io::ErrorKind::Other, e));
        }
        Ok(cap)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
