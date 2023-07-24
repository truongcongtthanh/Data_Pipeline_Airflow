from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



class LoadFactOperator(BaseOperator):
    """
    Loads fact table in Redshift from s3
    
    *redshift_conn_id: Redshift connection ID
    *table: Target table in Redshift to load
    *query_insert: query for getting data to load into target table
    """    
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query_insert="",
                 *args, **kwargs):



        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query_insert = query_insert

    def execute(self, context):
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Load fact table in Redshift")
        query_format = """
        INSERT INTO {}
        {};
        COMMIT;
        """
        sql_query = LoadFactOperator.query_format.format(
            self.table,
            self.query_insert
        )
        redshift.run(sql_query)        