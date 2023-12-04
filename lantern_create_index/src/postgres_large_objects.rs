use lantern_utils::quote_ident;
use postgres::{Client, Transaction};
use postgres_types::Oid;
use std::{cmp, io};

pub struct LargeObject<'a> {
    transaction: Option<Transaction<'a>>,
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

    pub fn create(&mut self) -> crate::AnyhowVoidResult {
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
    ) -> crate::AnyhowVoidResult {
        let transaction = self.transaction;
        let mut transaction = transaction.unwrap();
        transaction.execute(
            "SELECT pg_catalog.lo_export($1, $2)",
            &[&self.oid.unwrap(), &self.index_path],
        )?;

        let mut idx_name = "".to_owned();

        if let Some(name) = index_name {
            idx_name = quote_ident(name);
        }

        transaction.execute(
            &format!("CREATE INDEX {idx_name} ON {table_name} USING hnsw({column_name}) WITH (_experimental_index_path='{index_path}');", index_path=self.index_path),
            &[],
        )?;

        transaction.commit()?;
        Ok(())
    }

    pub fn remove_from_remote_fs(
        client: &mut Client,
        oid: Oid,
        path: &str,
    ) -> crate::AnyhowVoidResult {
        let mut transaction = client.transaction()?;
        let fd = transaction.query_one("SELECT pg_catalog.lo_open($1, 131072)", &[&oid])?;
        let fd: i32 = fd.get(0);
        transaction.execute("SELECT pg_catalog.lo_truncate($1, 0)", &[&fd])?;
        transaction.execute("SELECT pg_catalog.lo_export($1, $2)", &[&oid, &path])?;
        transaction.execute("SELECT pg_catalog.lo_unlink($1)", &[&oid])?;
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
