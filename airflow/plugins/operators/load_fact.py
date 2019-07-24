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
                 truncate: bool,
                 *args, **kwargs):
        """
        :param redshift_conn_id: Connection to Redshift database
        :param fact_table: Name of fact table
        :param fact_cols: List of column names of fact table as string
        :param sql: SQL statement used to select data that is inserted into fact table
        :param truncate: If True, clear fact table before inserting new data
        :param args: Additional arguments
        :param kwargs: Additional keyword arguments
        """

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.fact_table = fact_table
        self.fact_cols = fact_cols
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):

        # connect to redshift
        logging.debug("Connecting to Redshift")
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # clear fact table if desired
        if self.truncate:
            redshift_hook.run(f"TRUNCATE {self.fact_table}")

        # load data from staging into fact table
        logging.info(f"Loading data into fact table {self.fact_table}")
        redshift_hook.run(f"INSERT INTO {self.fact_table} ({self.fact_cols}) {self.sql}")
