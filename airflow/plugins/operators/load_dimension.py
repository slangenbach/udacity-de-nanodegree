import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Custom operator to load data from stage table into dimension table via SQL.
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str,
                 dim_table: str,
                 dim_cols: str,
                 sql: str,
                 truncate: bool,
                 *args, **kwargs):
        """
        :param redshift_conn_id: Connection to Redshift database
        :param dim_table: Name of dimension table
        :param dim_cols: List of column names of dimension table as string
        :param sql: SQL statement used to select data that is inserted into dimension table
        :param truncate: If True, clear dimension table before inserting new data
        :param args: Additional arguments
        :param kwargs: Additional keyword arguments
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dim_table = dim_table
        self.dim_cols = dim_cols
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):

        # connect to redshift
        logging.debug("Connecting to Redshift")
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # clear fact table if desired
        if self.truncate:
            redshift_hook.run(f"TRUNCATE {self.dim_table}")

        # load data from staging into fact table
        logging.info(f"Loading data into dimension table {self.dim_table}")
        redshift_hook.run(f"INSERT INTO {self.dim_table} ({self.dim_cols}) {self.sql}")
