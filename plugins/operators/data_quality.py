from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



class DataQualityOperator(BaseOperator):
    """
    Data quality: check by passing test SQL query and expected result
    
    *redshift_conn_id: Redshift connection ID
    *test_query: Query to run on Redshift data warehouse
    *expected_result: Result to match of test_query
    """
    
    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 test_query="",
                 expected_result="",
                 *args, **kwargs):
        
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_query = test_query
        self.expected_result = expected_result
    
    def execute(self, context):

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Running data quality")
        records = redshift_hook.get_records(self.test_query)
        if records[0][0] != self.expected_result:
            raise ValueError(f"""
                Data quality: Failed. \
                {records[0][0]} doesn't equal {self.expected_result}
            """)
        else:
            self.log.info("Data quality: Passed")