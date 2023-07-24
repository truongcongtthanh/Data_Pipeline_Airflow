from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    """
    Loads dimension table in Redshift from s3
    
    *redshift_conn_id: Redshift connection ID
    *table: Target table in Redshift to load
    *query_insert: SQL query for getting data to load into target table
    """
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query_insert="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query_insert = query_insert

        def execute(self, context):
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            self.log.info(f"Load dimension table {self.table} in Redshift")
            query_format = """
            TRUNCATE TABLE {};
            INSERT INTO {}
            {};
            COMMIT;
            """
            sql_query = LoadDimensionOperator.query_format.format(
                self.table,
                self.table,
                self.query_insert
            )
            redshift.run(sql_query)