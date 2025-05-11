import json
from tqdm import tqdm
from math import ceil
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance
from typing import Generator
from config.config import FILE_EXTRACT, COLLECTION_NAME
from config.logging_config.modern_log import LoggingConfig
import numpy as np
# ---------------------------------------------------------------------------- #
#                                LOGGING CONFIG                                #
# ---------------------------------------------------------------------------- #
logger = LoggingConfig(level="INFO").get_logger("qdrant_upload")
# ---------------------------------------------------------------------------- #
    
class VectorUploader:
    def __init__(self, json_path: str, collection_name: str, model: object, qdrant_host: str = "http://localhost:6333"):
        """
        Initialize a VectorUploader instance.

        Args:
            json_path (str): The path to the JSON lines file containing the data to upload.
            collection_name (str): The name of the Qdrant collection to upload vectors to.
            model (object): A sentence transformer model to encode data into vectors.
            qdrant_host (str, optional): The address of the Qdrant server. Defaults to "http://localhost:6333".
        """
        self.json_path = json_path
        self.collection_name = collection_name
        self.model = model
        self.client = QdrantClient(qdrant_host)

    def load_data(self) -> list:
        """
        Load data from a JSON lines file specified by the json_path attribute.

        Returns:
            list: A list of dictionaries, where each dictionary is a parsed JSON object
            from a line in the input file.
        """
        data = []
        with open(self.json_path, encoding="utf-8") as f:
            for line in f:
                data.append(json.loads(line))
        logger.info(f"Loaded {len(data)} vectors from {self.json_path}")
        return data

    def batch_encode(self, texts, batch_size=64) -> Generator[np.ndarray, None, None]:
        """
        Encode a list of text strings in batches.

        Args:
            texts (list): A list of text strings to be encoded.
            batch_size (int, optional): The number of text strings to encode in each batch. Defaults to 64.

        Yields:
            numpy.ndarray: A batch of encoded vectors for the input text strings.
        """
        for i in range(0, len(texts), batch_size):
            yield self.model.encode(texts[i:i + batch_size], show_progress_bar=False)

    def upload(self) -> None:
        """
        Uploads vector data to a Qdrant collection. This method performs the following steps:
        1. Loads data from a JSON lines file.
        2. Deletes the existing collection if it exists.
        3. Creates a new collection with specified vector configuration.
        4. Encodes the content data into vectors in batches.
        5. Uploads batches of vectors along with their payload to the Qdrant collection.

        The method ensures that the collection is replaced with fresh data each time it is called.

        Raises:
            Exception: If there is an issue with creating or uploading to the Qdrant collection.

        Logs:
            Info level log indicating the number of vectors successfully uploaded.
        """
        data = self.load_data()
        contents = [item["content"] for item in data]
        payloads = data

        # remove collection if exists
        if self.client.collection_exists(self.collection_name):
            self.client.delete_collection(self.collection_name)

        # create new collection 
        self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config=VectorParams(size=1024, distance=Distance.COSINE),
        )

        ids_counter = 0
        batch_size = 256

        total_batches = ceil(len(contents) / batch_size)
        for i, vectors in enumerate(tqdm(self.batch_encode(contents, batch_size), total=total_batches)):
            payload_batch = payloads[i * batch_size : (i + 1) * batch_size]
            self.client.upload_collection(
                collection_name=self.collection_name,
                vectors=vectors,
                payload=payload_batch,
                ids=None,
                batch_size=batch_size,
            )
            ids_counter += len(vectors)

        logger.info(f"Successfully Uploaded {ids_counter} vectors.")

if __name__ == "__main__":
    json_path = FILE_EXTRACT
    collection_name = COLLECTION_NAME
    model = SentenceTransformer("BAAI/bge-m3", device="cuda")
    qdrant_host = "http://localhost:6333"

    uploader = VectorUploader(
        json_path=json_path,
        collection_name=collection_name,
        model=model,
        qdrant_host=qdrant_host,
    )
    
    uploader.upload()