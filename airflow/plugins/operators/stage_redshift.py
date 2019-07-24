import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    Custom operator capable of loading CSV/JSON-formatted files from AWS S3 to AWS Redshift.
    """
    ui_color = '#358140'
    template_fields = ("aws_iam_role", "s3_bucket", "s3_key", "s3_region", "redshift_table", "json_path")

    @apply_defaults
    def __init__(self,
                 aws_iam_role: str,
                 redshift_conn_id: str,
                 s3_bucket: str,
                 s3_key: str,
                 s3_region: str,
                 redshift_table: str,
                 json_path: str,
                 *args, **kwargs):
        """
        :param aws_iam_role: AWS IAM role required for COPY command
        :param redshift_conn_id: Connection to Redshift database
        :param s3_bucket: S3 bucket to load data from
        :param s3_key: Path within S3 bucket where data is located
        :param s3_region: AWS region, e.g. us-west-2, where the bucket is located
        :param redshift_table: Name of Redshift table in which data is loaded
        :param json_path: Path to JSON file containing information how to handle data in S3 bucket
        :param args: Additional arguments
        :param kwargs: Additional keyword arguments
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_iam_role = aws_iam_role
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket =  s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region
        self.redshift_table = redshift_table
        self.json_path = json_path

    def execute(self, context):

        # define sql template
        sql_template = """
            COPY {redshift_table}
            FROM 's3://{s3_bucket}/{s3_key}'
            CREDENTIALS 'aws_iam_role={aws_iam_role}'
            REGION '{s3_region}'
            COMPUPDATE OFF
            STATUPDATE OFF
            JSON '{json_path}'
            TRUNCATECOLUMNS
            """

        # connect to redshift
        logging.debug("Connecting to Redshift")
        redshift_hook = PostgresHook(self.redshift_conn_id)

        # copy data from s3 to redshift
        sql = sql_template.format(
            redshift_table = self.redshift_table,
            aws_iam_role = self.aws_iam_role,
            s3_bucket = self.s3_bucket,
            s3_key = self.s3_key,
            s3_region = self.s3_region,
            json_path = self.json_path
        )

        logging.info(f"Staging data from {self.s3_bucket}/{self.s3_key} to {self.redshift_table}")
        redshift_hook.run(sql)
