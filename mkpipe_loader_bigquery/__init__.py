import gc
from datetime import datetime
from typing import List

from mkpipe.exceptions import ConfigError, LoadError
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig, WriteStrategy
from mkpipe.spark.base import BaseLoader
from mkpipe.spark.columns import normalize_column_names
from mkpipe.strategy import resolve_write_strategy
from mkpipe.utils import get_logger
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

JAR_PACKAGES = ['com.google.cloud.spark:spark-bigquery-with-dependencies_2.13:0.44.1']

logger = get_logger(__name__)


class BigQueryLoader(BaseLoader, variant='bigquery'):
    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.project = connection.database
        self.dataset = connection.schema
        self.credentials_file = connection.credentials_file
        self.temp_gcs_bucket = (
            connection.extra.get('temp_gcs_bucket', '') if connection.extra else ''
        )
        self._billing_project_id = (
            connection.extra.get('billing_project') if connection.extra else None
        )

    def _billing_project(self) -> str:
        """Resolve the billing/quota project for BigQuery API calls.

        Priority: connection.extra.billing_project > credentials JSON project_id > self.project
        """
        if self._billing_project_id:
            return self._billing_project_id

        if self.credentials_file:
            import json
            from pathlib import Path

            creds_path = Path(self.credentials_file)
            if creds_path.exists():
                try:
                    with open(creds_path) as f:
                        creds = json.load(f)
                    project_id = creds.get('project_id')
                    if project_id:
                        return project_id
                except Exception:
                    pass

        return self.project

    def _full_table(self, target_name: str) -> str:
        return f'{self.project}.{self.dataset}.{target_name}'

    def _write_df(self, df, write_mode: str, target_name: str) -> None:
        writer = (
            df.write.format('bigquery')
            .option('table', self._full_table(target_name))
            .option('temporaryGcsBucket', self.temp_gcs_bucket)
            .option('parentProject', self._billing_project())
            .mode(write_mode)
        )
        if self.credentials_file:
            writer = writer.option('credentialsFile', self.credentials_file)
        writer.save()

    def _build_merge_sql(
        self,
        temp_table: str,
        target_table: str,
        write_key: List[str],
        columns: List[str],
        update_columns: List[str],
    ) -> str:
        join_cond = ' AND '.join(f't.`{k}` = s.`{k}`' for k in write_key)
        insert_cols = ', '.join(f'`{c}`' for c in columns)
        insert_vals = ', '.join(f's.`{c}`' for c in columns)
        update_set = ', '.join(f'`{c}` = s.`{c}`' for c in update_columns)
        return (
            f'MERGE `{target_table}` AS t '
            f'USING `{temp_table}` AS s ON {join_cond} '
            f'WHEN MATCHED THEN UPDATE SET {update_set} '
            f'WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})'
        )

    def _upsert(self, df, target_name: str, write_key: List[str], spark) -> None:
        temp_name = f'_mkpipe_tmp_{target_name}'
        temp_full = self._full_table(temp_name)
        target_full = self._full_table(target_name)
        try:
            self._write_df(df, 'overwrite', temp_name)
            non_key_cols = [c for c in df.columns if c not in write_key]
            sql = self._build_merge_sql(
                temp_full, target_full, write_key, df.columns, non_key_cols,
            )
            logger.debug({'upsert_sql': sql})
            spark.conf.set('viewsEnabled', 'true')
            spark.read.format('bigquery') \
                .option('parentProject', self._billing_project()) \
                .option('query', sql) \
                .load()
        finally:
            try:
                spark.read.format('bigquery') \
                    .option('parentProject', self._billing_project()) \
                    .option('query', f'DROP TABLE IF EXISTS `{temp_full}`') \
                    .load()
            except Exception:
                logger.warning("Failed to drop temp table '%s'", temp_full)

    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        target_name = table.target_name
        df = data.df

        if df is None:
            logger.info(
                {'table': target_name, 'status': 'skipped', 'reason': 'no data'}
            )
            return

        col_name = self.ingested_at_column
        etl_time = datetime.now()
        if col_name in df.columns:
            df = df.drop(col_name)
        df = df.withColumn(col_name, F.lit(etl_time).cast(TimestampType()))
        df = normalize_column_names(df, self.column_name_case)

        if table.write_partitions:
            df = df.coalesce(table.write_partitions)

        strategy = resolve_write_strategy(table, data)

        logger.info(
            {
                'table': target_name,
                'status': 'loading',
                'write_strategy': strategy.value,
            }
        )

        try:
            match strategy:
                case WriteStrategy.APPEND:
                    self._write_df(df, 'append', target_name)
                case WriteStrategy.REPLACE:
                    mode = 'append' if self.if_exists == 'append' else 'overwrite'
                    self._write_df(df, mode, target_name)
                case WriteStrategy.UPSERT | WriteStrategy.MERGE:
                    if not table.write_key:
                        raise ConfigError(
                            f"write_strategy '{strategy.value}' requires write_key "
                            f"for table '{target_name}'"
                        )
                    self._upsert(df, target_name, table.write_key, spark)
                case _:
                    raise ConfigError(
                        f"BigQuery loader does not support write_strategy: {strategy.value}"
                    )
        except (ConfigError, LoadError):
            raise
        except Exception as e:
            raise LoadError(f"Failed to write '{target_name}': {e}") from e

        df.unpersist()
        gc.collect()

        logger.info({'table': target_name, 'status': 'loaded'})
