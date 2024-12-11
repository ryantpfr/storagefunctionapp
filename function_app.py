import azure.functions as func
import logging
from azure.storage.blob import ContainerClient
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

IN_CONTAINER = "input-container"
OUT_CONTAINER = "output-container"

container_client = ContainerClient(
    account_url=os.environ["permanatSAS"],
    container_name = OUT_CONTAINER,
    )

@app.blob_trigger(arg_name="inputblob", path=IN_CONTAINER,
                               connection="temporarysource_STORAGE")
def blob_trigger(inputblob: func.InputStream):
    logging.info(f"Processing blob {inputblob.name}")
    path = inputblob.name[inputblob.name.index("/")+1:] # remove container from path
    logging.info(path)


    # since this is a POC lets assume the file will fit in available memory for now
    # write initial blob assuming it doesn't already exist
    # it should throw an error if it already exists
    with container_client.get_blob_client(blob = path) as blob_client:
        part_num = 1 # keep track if what buffer number we are on
        buffer = inputblob.read()
        logging.info(f"uploading {path} part {part_num}, size = {len(buffer)}")
        blob_client.upload_blob(buffer)

    inputblob.close()