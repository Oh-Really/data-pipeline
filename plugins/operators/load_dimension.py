from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION \'us-east-1\'
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 data_format="",
                 #delimiter="",
                 #ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table        
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.data_format = data_format
        #self.delimiter = delimiter
        #self.ignore_headers = ignore_headers
        

    def execute(self, context):
        self.log.info('StageToRedshiftOperator kicking off...')
        # This get any saved creds from airflow
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}/".format(self.s3_bucket, rendered_key)
        if self.data_format != 'auto':
            self.data_format = "s3://{}/{}".format(self.s3_bucket, self.data_format)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,            
            self.data_format
            #self.ignore_headers,
            #self.delimiter
        )
        redshift.run(formatted_sql)
