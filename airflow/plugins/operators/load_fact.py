import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Custom operator to load data from staging tables into fact table via SQL.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 fact_table: str,
                 fact_cols: str,
                 sql: str,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table = fact_table
        self.fact_cols = fact_cols
        self.sql = sql

    def execute(self, context):

        # connect to redshift
        logging.debug("Connecting to Redshift")
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # load data from staging into fact table
        logging.info(f"Loading data into fact table {self.fact_table}")
        redshift_hook.run(f"INSERT INTO {self.fact_table} ({self.fact_cols}) {self.sql}")
