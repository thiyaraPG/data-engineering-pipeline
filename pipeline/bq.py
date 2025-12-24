from pipeline.log import log

def apply_sql(client, sql_path, project, dataset, location, dry_run):
    if dry_run:
        log("INFO", "dry_run_sql", file=sql_path)
        return

    with open(sql_path) as f:
        sql = f.read()

    sql = (
        sql.replace("${project}", project)
           .replace("${dataset}", dataset)
           .replace("${location}", location)
    )

    client.query(sql).result()

def load_dataframe(client, df, table, dry_run):
    if dry_run:
        log("INFO", "dry_run_load", table=table, rows=len(df))
        return

    job = client.load_table_from_dataframe(
        df,
        table,
        job_config={"write_disposition": "WRITE_TRUNCATE"}
    )
    job.result()
