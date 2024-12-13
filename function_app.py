import azure.functions as func
import logging
from azure.storage.blob import ContainerClient
import os
from azure.identity import DefaultAzureCredential

creds = DefaultAzureCredential()

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

in_container_client = ContainerClient(
    account_url=os.environ["source_connection__blobServiceUri"],
    container_name = os.environ["source_container"],
    credential=creds
    )
out_container_client = ContainerClient(
    account_url=os.environ["dest_connection__blobServiceUri"],
    container_name = os.environ["dest_container"],
    credential=creds
    )

@app.blob_trigger(arg_name="inputblob", path=os.environ["source_container"],
                               connection="source_connection")
def blob_trigger(inputblob: func.InputStream):
    logging.info(f"Processing blob {inputblob.name}")
    path = inputblob.name[inputblob.name.index("/")+1:] # remove container from path
    logging.info(f"using relative path {path}")


    # since this is a POC lets assume the file will fit in available memory for now
    # write initial blob assuming it doesn't already exist
    # it should throw an error if it already exists
    with out_container_client.get_blob_client(blob = path) as blob_client:
        part_num = 1 # keep track if what buffer number we are on
        buffer = inputblob.read()
        logging.info(f"uploading {path} part {part_num}, size = {len(buffer)}")
        blob_client.upload_blob(buffer)
    inputblob.close()
    logging.info(f"wrote to dest container {path}")

    # now delete it from the source container
    with in_container_client.get_blob_client(blob = path) as blob_client:
        blob_client.delete_blob()
    logging.info(f"deleted from source container {path}")
    

@app.route(route="health")
def health(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Checking source blob connection')
    logging.info(list(in_container_client.list_blob_names()))

    logging.info('Checking dest blob connection')
    logging.info(list(out_container_client.list_blob_names()))

    return func.HttpResponse(f"Triggered Successfully",status_code=200)
