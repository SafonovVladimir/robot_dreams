from airflow.models import Variable

BUCKET_NAME = Variable.get("BUCKET_NAME")
GCP_CONN_ID = Variable.get("GCP_CONN_ID")
AWS_S3_CONN_ID = Variable.get("AWS_S3_CONN_ID")
LOCAL_ROOT_DIRECTORY = Variable.get("LOCAL_ROOT_DIRECTORY")
UPLOAD_DATE = Variable.get("upload_date")
