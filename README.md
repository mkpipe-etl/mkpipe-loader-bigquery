# mkpipe-loader-bigquery

Google BigQuery loader plugin for [MkPipe](https://github.com/mkpipe-etl/mkpipe). Writes Spark DataFrames into BigQuery tables using the `spark-bigquery-connector`.

## Documentation

For more detailed documentation, please visit the [GitHub repository](https://github.com/mkpipe-etl/mkpipe).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

---

## Connection Configuration

```yaml
connections:
  bq_target:
    variant: bigquery
    database: my-gcp-project
    schema: my_dataset
    credentials_file: /path/to/service-account.json
    extra:
      temp_gcs_bucket: my-temp-bucket
```

> **Note:** `temp_gcs_bucket` is required for write operations. The connector stages data in GCS before loading into BigQuery.

---

## Table Configuration

```yaml
pipelines:
  - name: pg_to_bq
    source: pg_source
    destination: bq_target
    tables:
      - name: public.events
        target_name: stg_events
        replication_method: full
```

---

## Write Strategy

Control how data is written to BigQuery:

```yaml
      - name: public.events
        target_name: stg_events
        write_strategy: upsert       # append | replace | upsert | merge
        write_key: [id]              # required for upsert/merge
```

| Strategy | BigQuery Behavior |
|---|---|
| `append` | Insert via Spark BigQuery connector (default for incremental) |
| `replace` | Overwrite table via Spark BigQuery connector (default for full). With `if_exists: append`: delete all rows + insert (preserves schema) |
| `upsert` | Write to temp table, then `MERGE INTO target USING temp ON ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...` |
| `merge` | Same as upsert for BigQuery |

> **Note:** `upsert`/`merge` requires `write_key`. The loader creates a temp table in the same dataset, writes data there, executes a MERGE statement via BigQuery SQL, then drops the temp table.

---

## Write Parallelism

`write_partitions` coalesces the DataFrame to N partitions before writing. Each partition is staged as a separate file in the GCS temp bucket:

```yaml
      - name: public.events
        target_name: stg_events
        replication_method: full
        write_partitions: 8
```

### Performance Notes

- BigQuery load jobs are fast regardless of partition count — the bottleneck is usually GCS staging throughput.
- `write_partitions` is most useful when reducing a very high partition count (prevents too many small GCS files).
- For large datasets, keeping more partitions (parallelizing GCS writes) is generally better.

---

## All Table Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Source table name |
| `target_name` | string | required | BigQuery destination table name |
| `replication_method` | `full` / `incremental` | `full` | Replication strategy |
| `write_partitions` | int | — | Coalesce DataFrame to N partitions before writing |
| `write_strategy` | string | — | `append`, `replace`, `upsert`, `merge` |
| `write_key` | list | — | Key columns for upsert/merge (required) |
| `if_exists` | string | — | `replace` (drop+create) or `append` (preserve table, delete+insert). Inherits from settings |
| `dedup_columns` | list | — | Columns used for `mkpipe_id` hash deduplication |
| `tags` | list | `[]` | Tags for selective pipeline execution |
| `pass_on_error` | bool | `false` | Skip table on error instead of failing |
