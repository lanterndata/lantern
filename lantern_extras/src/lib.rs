use pgrx::prelude::*;

use flate2::read::GzDecoder;
use ftp::FtpStream;
use lantern_embeddings_core;
use lantern_external_index::cli::{CreateIndexArgs, UMetricKind};
use tar::Archive;

pgrx::pg_module_magic!();
pub mod dotvecs;

fn notice_fn(text: &str) {
    notice!("{}", text);
}

fn validate_index_param(param_name: &str, param_val: i32, min: i32, max: i32) {
    if param_val < min || param_val > max {
        error!("{param_name} should be in range [{min}, {max}]");
    }
}

#[pg_extern(immutable, parallel_unsafe)]
fn lantern_create_external_index<'a>(
    column: &'a str,
    table: &'a str,
    schema: default!(&'a str, "'public'"),
    metric_kind: default!(&'a str, "'l2sq'"),
    dim: default!(i32, 0),
    m: default!(i32, 16),
    ef_construction: default!(i32, 16),
    ef: default!(i32, 16),
    index_name: default!(&'a str, "''"),
) -> Result<(), anyhow::Error> {
    validate_index_param("ef", ef, 1, 400);
    validate_index_param("ef_construction", ef_construction, 1, 400);
    validate_index_param("ef_construction", ef_construction, 1, 400);
    validate_index_param("m", m, 2, 128);

    if dim != 0 {
        validate_index_param("dim", dim, 1, 2000);
    }

    let (db, user, socket_path, port) = Spi::connect(|client| {
        let row = client
            .select(
                "
           SELECT current_database()::text AS db,
           current_user::text AS user,
           (SELECT setting::text FROM pg_settings WHERE name = 'unix_socket_directories') AS socket_path,
           (SELECT setting::text FROM pg_settings WHERE name = 'port') AS port",
                None,
                None,
            )?
            .first();

        let db = row.get_by_name::<String, &str>("db")?.unwrap();
        let user = row.get_by_name::<String, &str>("user")?.unwrap();
        let socket_path = row.get_by_name::<String, &str>("socket_path")?.unwrap();
        let port = row.get_by_name::<String, &str>("port")?.unwrap();

        Ok::<(String, String, String, String), anyhow::Error>((db, user, socket_path, port))
    })?;

    let connection_string = format!("dbname={db} host={socket_path} user={user} port={port}");

    let index_name = if index_name == "" {
        None
    } else {
        Some(index_name.to_owned())
    };

    let res = lantern_external_index::create_usearch_index(
        &CreateIndexArgs {
            import: true,
            out: "/tmp/index.usearch".to_owned(),
            table: table.to_owned(),
            schema: schema.to_owned(),
            metric_kind: UMetricKind::from(metric_kind)?,
            efc: ef_construction as usize,
            ef: ef as usize,
            m: m as usize,
            uri: connection_string,
            column: column.to_owned(),
            dims: dim as usize,
            index_name,
        },
        None,
        None,
        None,
    );

    if let Err(e) = res {
        error!("{e}");
    }

    Ok(())
}
#[pg_schema]
mod lantern_extras {
    use crate::lantern_create_external_index;
    use pgrx::prelude::*;
    use pgrx::{PgBuiltInOids, PgRelation, Spi};

    #[pg_extern(immutable, parallel_unsafe)]
    fn _reindex_external_index<'a>(
        index: PgRelation,
        metric_kind: &'a str,
        dim: i32,
        m: i32,
        ef_construction: i32,
        ef: i32,
    ) -> Result<(), anyhow::Error> {
        let index_name = index.name().to_owned();
        let schema = index.namespace().to_owned();
        let (table, column) = Spi::connect(|client| {
            let rows = client.select(
                "
                SELECT idx.indrelid::regclass::text   AS table_name,
                       att.attname::text              AS column_name
                FROM   pg_index AS idx
                       JOIN pg_attribute AS att
                         ON att.attrelid = idx.indrelid
                            AND att.attnum = ANY(idx.indkey)
                WHERE  idx.indexrelid = $1",
                None,
                Some(vec![(
                    PgBuiltInOids::OIDOID.oid(),
                    index.oid().into_datum(),
                )]),
            )?;

            if rows.len() == 0 {
                error!("Index with oid {:?} not found", index.oid());
            }

            let row = rows.first();

            let table = row.get_by_name::<String, &str>("table_name")?.unwrap();
            let column = row.get_by_name::<String, &str>("column_name")?.unwrap();
            Ok::<(String, String), anyhow::Error>((table, column))
        })?;

        drop(index);
        lantern_create_external_index(
            &column,
            &table,
            &schema,
            metric_kind,
            dim,
            m,
            ef_construction,
            ef,
            &index_name,
        )
    }
}

#[pg_extern(immutable, parallel_safe)]
fn clip_text<'a>(text: &'a str) -> Vec<f32> {
    let res = lantern_embeddings_core::clip::process(
        "clip/ViT-B-32-textual",
        &vec![text],
        Some(&(notice_fn as lantern_embeddings_core::LoggerFn)),
        None,
        true,
    );
    if let Err(e) = res {
        error!("{}", e);
    }

    return res.unwrap()[0].clone();
}

#[pg_extern(immutable, parallel_safe)]
fn text_embedding<'a>(model_name: &'a str, text: &'a str) -> Vec<f32> {
    let res = lantern_embeddings_core::clip::process(
        model_name,
        &vec![text],
        Some(&(notice_fn as lantern_embeddings_core::LoggerFn)),
        None,
        true,
    );
    if let Err(e) = res {
        error!("{}", e);
    }

    return res.unwrap()[0].clone();
}

#[pg_extern(immutable, parallel_safe)]
fn image_embedding<'a>(model_name: &'a str, path_or_url: &'a str) -> Vec<f32> {
    let res = lantern_embeddings_core::clip::process(
        model_name,
        &vec![path_or_url],
        Some(&(notice_fn as lantern_embeddings_core::LoggerFn)),
        None,
        true,
    );
    if let Err(e) = res {
        error!("{}", e);
    }

    return res.unwrap()[0].clone();
}

#[pg_extern(immutable, parallel_safe)]
fn clip_image<'a>(path_or_url: &'a str) -> Vec<f32> {
    let res = lantern_embeddings_core::clip::process(
        "clip/ViT-B-32-visual",
        &vec![path_or_url],
        Some(&(notice_fn as lantern_embeddings_core::LoggerFn)),
        None,
        true,
    );
    if let Err(e) = res {
        error!("{}", e);
    }

    return res.unwrap()[0].clone();
}

#[pg_extern(immutable, parallel_safe)]
fn get_available_models() -> String {
    return lantern_embeddings_core::clip::get_available_models(None).0;
}

#[pg_extern]
fn get_vectors<'a>(gzippath: &'a str) -> String {
    let url = url::Url::parse(gzippath).unwrap();
    if url.scheme() == "ftp" {
        match download_gzipped_ftp(url) {
            Ok(data) => {
                return data
                    .map(|b| b.unwrap().to_string())
                    .take(10)
                    .collect::<Vec<String>>()
                    .join(" ");
            }
            Err(e) => {
                return e.to_string();
            }
        }
    }
    return "not supported".to_string();
}

fn download_gzipped_ftp(
    url: url::Url,
) -> Result<impl Iterator<Item = Result<u8, std::io::Error>>, Box<dyn std::error::Error>> {
    use std::io::prelude::*;
    assert!(url.scheme() == "ftp");
    let domain = url.host_str().expect("no host");
    let port = url.port().unwrap_or(21);
    let pathurl = url.join("./")?;
    let path = pathurl.path();
    let filename = url
        .path_segments()
        .expect("expected path segments in an ftp url")
        .last()
        .unwrap();

    let mut ftp_stream = FtpStream::connect(format!("{}:{}", domain, port))?;
    ftp_stream
        .login("anonymous", "anonymous")
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::PermissionDenied, e.to_string()))?;
    ftp_stream.cwd(path)?;
    let file = ftp_stream.get(filename)?;

    let dd = GzDecoder::new(file);
    if false {
        return Ok(dd.bytes());
    }
    let mut a = Archive::new(dd);
    // a.unpack("/tmp/rustftp")?;
    a.entries()
        .unwrap()
        .map(|entry| match entry {
            Ok(e) => {
                let s = String::new();
                notice!("entry name {}", e.path().unwrap().display());
                Ok(s)
            }
            Err(e) => Err(e),
        })
        .for_each(|e| match e {
            Ok(s) => {
                notice!("entry: {}", s);
            }
            Err(e) => {
                notice!("entry: {}", e);
            }
        });
    return Err("not implemented".into());
}

// fn read_file_stream(pathlike: String) -> std::io::Result<std::io::Bytes<dyn std::io::Read>>{
//     use std::io::prelude::*;

//     let res = get(gzippath);
//     if res.is_err() {
//         return res.err().unwrap().to_string();
//     }
//     let resp = res.unwrap();
//     let mut d = GzDecoder::new(resp);

// }

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    // use pgrx::prelude::*;
    use pgrx::pg_test;

    #[pg_test]
    fn test_hello_lantern_extras() {
        assert_eq!("Hello, lantern_extras", crate::get_vectors("invalid path"));
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
