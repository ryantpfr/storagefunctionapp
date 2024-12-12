import azure.functions as func
import logging
from azure.storage.blob import ContainerClient
import os

app = func.FunctionApp()

IN_CONTAINER = "input-container"
OUT_CONTAINER = "output-container"

in_container_client = ContainerClient(
    account_url=os.environ["temp_SAS"],
    container_name = IN_CONTAINER,
    )
out_container_client = ContainerClient(
    account_url=os.environ["permanat_SAS"],
    container_name = OUT_CONTAINER,
    )

@app.blob_trigger(arg_name="inputblob", path=IN_CONTAINER,
                               connection="temporarysource_STORAGE")
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
