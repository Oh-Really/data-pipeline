from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {}
        {};
    """

    truncate_sql = """
        TRUNCATE TABLE {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_stmnt="",
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmnt = sql_stmnt
        self.truncate_table = truncate_table


    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Truncating dimension tables:')

        if self.truncate_table:
            self.log.info(f"Truncating dimension table: {self.table}")
            redshift.run(LoadDimensionOperator.truncate_sql.format(self.table))

        self.log.info("Loading dimension table: {self.table}")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql_stmnt
        )

        self.log.info(f"Executing Query: {formatted_sql}")
        redshift.run(formatted_sql)
