def apply_sql(client, sql_path, project, dataset, location, dry_run):
    if dry_run or client is None:
        return

    with open(sql_path) as f:
        sql = f.read()

    sql = (
        sql.replace("${project}", project)
           .replace("${dataset}", dataset)
           .replace("${location}", location)
    )

    client.query(sql).result()
