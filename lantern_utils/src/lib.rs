pub fn quote_ident(str: &str) -> String {
    format!("\"{}\"", str.replace("\"", "\"\""))
}

pub fn get_full_table_name(schema: &str, table: &str) -> String {
    let schema = quote_ident(schema);
    let table = quote_ident(table);
    format!("{schema}.{table}")
}
