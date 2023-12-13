import psycopg2
import psycopg2.extras
import time
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os

db_params = {
        'database': os.getenv("AZURE_POSTGRESQL_DATABSE"),
        'user': os.getenv("AZURE_POSTGRESQL_USER"),
        'password': os.getenv("AZURE_POSTGRESQL_PASSWORD"),
        'host': os.getenv("AZURE_POSTGRESQL_HOST"),
        'port': os.getenv("AZURE_POSTGRESQL_PORT"),
    }
    
conn = psycopg2.connect(**db_params)
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


def get_client():
    account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL")
    shared_access_key = os.getenv("AZURE_STORAGE_ACCESS_KEY")
    credential = shared_access_key

    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient(account_url, credential=credential)

    return blob_service_client

def get_properties(blob_service_client: BlobServiceClient, blobname):
    blob_client = blob_service_client.get_blob_client(container='iraya-file', blob=blobname)

    properties = blob_client.get_blob_properties()

    return properties

blob_service_client = get_client()

def do_some_work(job_data):
    try:
        properties = get_properties(blob_service_client,job_data['id'])
        print(properties.creation_time)
        print(properties.metadata["uploader"])
        print(properties.metadata["filename"])

        sql = """
            INSERT INTO public."Metadata"
            (id, uploader, "uploadTime", filename, uuid)
            VALUES(nextval('"Metadata_id_seq"'::regclass),%s , %s, %s , %s);
        """
        cur.execute(sql, (properties.metadata["uploader"],properties.creation_time,properties.metadata["filename"],job_data['id'],))

    except Exception as e:
        print(e)
        raise e    
def process_job():
    
    sql = """
    WITH _queue_ids AS (
        SELECT id FROM postgres.public."Upload" u 
        WHERE processed is not true
        ORDER BY id ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    ) UPDATE postgres.public."Upload"
    SET processed = true 
    WHERE id = ANY(SELECT id FROM _queue_ids)
    RETURNING *
    """
    cur.execute(sql)
    queue_item = cur.fetchone()
    if queue_item:
        try:
            do_some_work(queue_item)
        except Exception as e:
            sql = """UPDATE postgres.public."Upload"
                    SET processed = false 
                    WHERE id =%s;"""
            # if we want the job to run again, insert a new item to the message queue with this job id
            cur.execute(sql, (queue_item['id'],))
    else:
        print('no job found')
    conn.commit()
while 1==1:
    process_job()
    time.sleep(5)
cur.close()
conn.close()