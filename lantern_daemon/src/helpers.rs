use crate::types::{AnyhowVoidResult, JobInsertNotification, JobUpdateNotification};
use futures::StreamExt;
use lantern_logger::Logger;
use lantern_utils::get_full_table_name;
use lantern_utils::quote_ident;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio_postgres::AsyncMessage;
use tokio_postgres::Client;

pub async fn check_table_exists(client: Arc<Client>, table: &str) -> AnyhowVoidResult {
    // verify that table exists
    if let Err(_) = client
        .execute(&format!("SELECT ctid FROM {} LIMIT 1", table), &[])
        .await
    {
        anyhow::bail!("Table {table} does not exist");
    }

    Ok(())
}

pub async fn db_notification_listener(
    db_uri: String,
    notification_channel: &'static str,
    insert_queue_tx: Sender<JobInsertNotification>,
    update_queue_tx: Option<Sender<JobUpdateNotification>>,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    let (client, mut connection) = tokio_postgres::connect(&db_uri, tokio_postgres::NoTls).await?;

    let client = Arc::new(client);
    let client_ref = client.clone();
    // spawn new task to handle notifications
    let task = tokio::spawn(async move {
        let mut stream = futures::stream::poll_fn(move |cx| connection.poll_message(cx));
        logger.info("Lisening for notifications");

        while let Some(message) = stream.next().await {
            if let Err(e) = &message {
                logger.error(&format!("Failed to get message from db: {}", e));
            }

            let message = message.unwrap();

            if let AsyncMessage::Notification(not) = message {
                let parts: Vec<&str> = not.payload().split(':').collect();

                if parts.len() < 2 {
                    logger.error(&format!("Invalid notification received {}", not.payload()));
                    continue;
                }

                let action: &str = parts[0];
                let id = i32::from_str_radix(parts[1], 10).unwrap();

                match action {
                    "insert" => {
                        insert_queue_tx
                            .send(JobInsertNotification {
                                id,
                                init: true,
                                generate_missing: false,
                                row_id: None,
                                filter: None,
                                limit: None,
                            })
                            .await
                            .unwrap();
                    }
                    "update" => {
                        if let Some(update_tx) = &update_queue_tx {
                            update_tx
                                .send(JobUpdateNotification {
                                    id,
                                    generate_missing: true,
                                })
                                .await
                                .unwrap();
                        }
                    }
                    _ => logger.error(&format!("Invalid notification received {}", not.payload())),
                }
            }
        }
        drop(client_ref);
    });

    client
        .batch_execute(&format!("LISTEN {notification_channel};"))
        .await?;

    task.await?;
    Ok(())
}

pub async fn startup_hook(
    client: Arc<Client>,
    table: &str,
    schema: &str,
    lock_table_schema: Option<&str>,
    lock_table_name: Option<&str>,
    channel: &str,
    logger: Arc<Logger>,
) -> AnyhowVoidResult {
    logger.info("Setting up environment");
    // verify that table exists
    let full_table_name = get_full_table_name(schema, table);
    check_table_exists(client.clone(), &full_table_name).await?;

    let insert_function_name = quote_ident(&format!("notify_insert_{table}"));
    let update_function_name = quote_ident(&format!("notify_update_{table}"));
    let insert_trigger_name = quote_ident(&format!("trigger_insert_{table}"));
    let update_trigger_name = quote_ident(&format!("trigger_update_{table}"));
    // Set up trigger on table insert
    client
        .batch_execute(&format!(
            "
            CREATE OR REPLACE FUNCTION {insert_function_name}() RETURNS TRIGGER AS $$
              BEGIN
                PERFORM pg_notify('{channel}', 'insert:' || NEW.id::TEXT);
                RETURN NULL;
              END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE FUNCTION {update_function_name}() RETURNS TRIGGER AS $$
              BEGIN
                IF (NEW.canceled_at IS NULL AND OLD.canceled_at IS NOT NULL) 
                OR (NEW.canceled_at IS NOT NULL AND OLD.canceled_at IS NULL)
                THEN
                     PERFORM pg_notify('{channel}', 'update:' || NEW.id::TEXT);
	            END IF;
                RETURN NEW;
              END;
            $$ LANGUAGE plpgsql;

            CREATE OR REPLACE TRIGGER {insert_trigger_name}
            AFTER INSERT 
            ON {full_table_name}
            FOR EACH ROW
            EXECUTE PROCEDURE {insert_function_name}();

            CREATE OR REPLACE TRIGGER {update_trigger_name}
            AFTER UPDATE 
            ON {full_table_name}
            FOR EACH ROW
            EXECUTE PROCEDURE {update_function_name}();
        "
        ))
        .await?;

    if lock_table_name.is_some() && lock_table_schema.is_some() {
        let lock_table_name =
            get_full_table_name(lock_table_schema.unwrap(), lock_table_name.unwrap());
        client
            .batch_execute(&format!(
                "
            -- Create Lock Table
            CREATE SCHEMA IF NOT EXISTS {lock_table_schema};
            CREATE UNLOGGED TABLE IF NOT EXISTS {lock_table_name} (
              job_id INTEGER NOT NULL,
              row_id TEXT NOT NULL,
              CONSTRAINT ldb_lock_jobid_rowid UNIQUE (job_id, row_id)
            );
        ",
                lock_table_schema = quote_ident(lock_table_schema.as_deref().unwrap())
            ))
            .await?;
    }

    Ok(())
}
