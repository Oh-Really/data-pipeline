from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks


    def execute(self, context):
        if len(self.checks) == 0:
            self.log.info("No data quality checks provided")
            return
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        failed_checks = []
        for check in self.checks:
            sql = check.get('check_sql')
            expected_result = check.get('expected_result')
            results = []

            try:
                self.log.info(f"Running query: {sql}")
                result = redshift.get_records(sql)[0][0]
                results.append(result)
            except Exception as e:
                self.log.info(f"Query failed with exception: {e}")

            if result != expected_result:
                self.log.info("Some tests failed")
                self.log.info(f"Actual result: {result}, expected result {expected_result}")
                failed_checks.append(sql)

        
        if failed_checks:
            self.log.info('Tests that failed: \n')
            self.log.info(failed_checks)
            raise ValueError('Data quality check failed')
