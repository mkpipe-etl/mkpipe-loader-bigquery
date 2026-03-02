import gc
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

from mkpipe.spark.base import BaseLoader
from mkpipe.models import ConnectionConfig, ExtractResult, TableConfig
from mkpipe.utils import get_logger

logger = get_logger(__name__)


class BigQueryLoader(BaseLoader, variant='bigquery'):
    def __init__(self, connection: ConnectionConfig):
        self.connection = connection
        self.project = connection.database
        self.dataset = connection.schema
        self.credentials_file = connection.credentials_file
        self.temp_gcs_bucket = connection.extra.get('temp_gcs_bucket', '')

    def load(self, table: TableConfig, data: ExtractResult, spark) -> None:
        target_name = table.target_name
        write_mode = data.write_mode
        df = data.df

        if df is None:
            logger.info({'table': target_name, 'status': 'skipped', 'reason': 'no data'})
            return

        etl_time = datetime.now()
        if 'etl_time' in df.columns:
            df = df.drop('etl_time')
        df = df.withColumn('etl_time', F.lit(etl_time).cast(TimestampType()))

        logger.info({
            'table': target_name,
            'status': 'loading',
            'write_mode': write_mode,
        })

        writer = (
            df.write.format('bigquery')
            .option('table', f'{self.project}.{self.dataset}.{target_name}')
            .option('temporaryGcsBucket', self.temp_gcs_bucket)
            .mode(write_mode)
        )

        if self.credentials_file:
            writer = writer.option('credentialsFile', self.credentials_file)

        writer.save()
        df.unpersist()
        gc.collect()

        logger.info({'table': target_name, 'status': 'loaded'})
