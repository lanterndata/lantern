use lantern_external_index::cli::{CreateIndexArgs, UMetricKind};
use pgrx::prelude::*;
use rand::Rng;

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

    let (db, user, socket_path, port, data_dir) = Spi::connect(|client| {
        let row = client
            .select(
                "
           SELECT current_database()::text AS db,
           current_user::text AS user,
           (SELECT setting::text FROM pg_settings WHERE name = 'unix_socket_directories') AS socket_path,
           (SELECT setting::text FROM pg_settings WHERE name = 'port') AS port,
           (SELECT setting::text FROM pg_settings WHERE name = 'data_directory') as data_dir",
                None,
                None,
            )?
            .first();

        let db = row.get_by_name::<String, &str>("db")?.unwrap();
        let user = row.get_by_name::<String, &str>("user")?.unwrap();
        let socket_path = row.get_by_name::<String, &str>("socket_path")?.unwrap();
        let port = row.get_by_name::<String, &str>("port")?.unwrap();
        let data_dir = row.get_by_name::<String, &str>("data_dir")?.unwrap();

        Ok::<(String, String, String, String, String), anyhow::Error>((
            db,
            user,
            socket_path,
            port,
            data_dir,
        ))
    })?;

    let connection_string = format!("dbname={db} host={socket_path} user={user} port={port}");

    let index_name = if index_name == "" {
        None
    } else {
        Some(index_name.to_owned())
    };

    let mut rng = rand::thread_rng();
    let index_path = format!("{data_dir}/ldb-index-{}.usearch", rng.gen_range(0..1000));

    let res = lantern_external_index::create_usearch_index(
        &CreateIndexArgs {
            import: true,
            out: index_path,
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
            remote_database: false,
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
    use super::lantern_create_external_index;
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
