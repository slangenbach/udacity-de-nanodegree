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
                 check_sql: str,
                 *args, **kwargs):
        """
        :param redshift_conn_id: Connection to Redshift database
        :param check_tables: List of Redshift tables to run data quality checks against
        :param check_sql: Custom SQL statement specifying data quality check (use "has_rows" to use built-in check)
        :param args: Additional arguments
        :param kwargs: Additional keyword arguments
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_tables = check_tables
        self.check_sql = check_sql

    def execute(self, context):

        # run quality checks
        for table in self.check_tables:

            # run built-in quality checks
            if self.check_sql == "has_rows":
                logging.info(f"Running data quality check for table: {table}")

                # check if data has been loaded into dimensions tables
                CheckOperator(f"SELECT COUNT(*) FROM {table}", conn_id=self.redshift_conn_id)

            # run custom quality check
            else:
                logging.info("Running data quality check")
                CheckOperator(self.check_sql, conn_id=self.redshift_conn_id)
