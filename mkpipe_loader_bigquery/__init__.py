import gc
from datetime import datetime

from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
from mkpipe.spark.base import BaseLoader
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

    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        target_name = table.target_name
        write_mode = data.write_mode
        df = data.df

        if df is None:
            logger.info(
                {'table': target_name, 'status': 'skipped', 'reason': 'no data'}
            )
            return

        etl_time = datetime.now()
        if 'etl_time' in df.columns:
            df = df.drop('etl_time')
        df = df.withColumn('etl_time', F.lit(etl_time).cast(TimestampType()))

        if table.write_partitions:
            df = df.coalesce(table.write_partitions)

        logger.info(
            {
                'table': target_name,
                'status': 'loading',
                'write_mode': write_mode,
            }
        )

        writer = (
            df.write.format('bigquery')
            .option('table', f'{self.project}.{self.dataset}.{target_name}')
            .option('temporaryGcsBucket', self.temp_gcs_bucket)
            .option('parentProject', self._billing_project())
            .mode(write_mode)
        )

        if self.credentials_file:
            writer = writer.option('credentialsFile', self.credentials_file)

        writer.save()
        df.unpersist()
        gc.collect()

        logger.info({'table': target_name, 'status': 'loaded'})
