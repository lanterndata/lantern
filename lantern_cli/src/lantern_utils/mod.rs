pub fn quote_ident(str: &str) -> String {
    format!("\"{}\"", str.replace("\"", "\"\""))
}

pub fn get_full_table_name(schema: &str, table: &str) -> String {
    let schema = quote_ident(schema);
    let table = quote_ident(table);
    format!("{schema}.{table}")
}

pub fn append_params_to_uri(db_uri: &str, params: &str) -> String {
    let parts: Vec<&str> = db_uri.split("/").collect();
    let mut db_uri_string = db_uri.to_string();
    let mut has_params = false;

    if let Some(last_part) = parts.last() {
        has_params = last_part.contains("?");
    }

    if has_params {
        db_uri_string.push_str(&format!("&{params}"));
    } else {
        db_uri_string.push_str(&format!("?{params}"));
    }

    db_uri_string
}
