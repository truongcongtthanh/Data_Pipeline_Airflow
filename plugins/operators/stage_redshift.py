from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook



class StageToRedshiftOperator(BaseOperator):
    """
    Copies data from S3 in Redshift
    
    *redshift_conn_id: Redshift connection ID
    *aws_credentials_id: AWS credentials ID
    *table: Target staging table in Redshift to copy data into
    *s3_bucket: S3 bucket where JSON data resides
    *s3_key: Path in S3 bucket
    *copy_json_option: Other Paths file 
    *region: AWS Region
    """
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 copy_json_option="auto",
                 region="",
                 *args, **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_json_option = copy_json_option
        self.region = region
    
    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        redshift.run("DELETE FROM {}".format(self.table))
        self.log.info("Clear data in Redshift")
                
        key = self.s3_key.format(**context)
        path_to_s3 = "s3://{}/{}".format(self.s3_bucket, key)
        query_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT as json '{}'
    """
        formatted_sql = StageToRedshiftOperator.query_sql.format(
            self.table,
            path_to_s3,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.copy_json_option
        )
        redshift.run(formatted_sql)
        self.log.info("Copy data from S3 to Redshift AWS.")

