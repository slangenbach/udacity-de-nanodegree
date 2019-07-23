import logging
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.check_operator import CheckOperator


class DataQualityOperator(BaseOperator):
    """
    Custom operator to run data quality check against a list of tables.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 check_tables: list,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_tables = check_tables

    def execute(self, context):

        # run quality checks
        for table in self.check_tables:
            logging.info(f"Running data quality check for table: {table}")

            # check if data has been loaded into dimensions tables
            CheckOperator(f"SELECT COUNT(*) FROM {table}", conn_id=self.redshift_conn_id)
