from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

from config.config import COLLECTION_NAME

class NeuralSearcher:
    def __init__(self, collection_name: str, model: object, qdrant_host: str = "http://localhost:6333"):   
        """
        Args:
            collection_name (str): The name of the collection to search in.
            model (object): A sentence transformer model to convert text to vectors.
            qdrant_host (str, optional): The address of the Qdrant server. Defaults to "http://localhost:6333".
        """
        self.collection_name = collection_name
        self.model = model
        self.qdrant_client = QdrantClient(qdrant_host)

    def search(self, query: str, top: int = 5) -> list:
        """
        Search for the most similar items to the given text in the collection.

        Args:
            query (str): The text to search for.
            top (int, optional): The number of results to return. Defaults to 5.

        Returns:
            list: A list of payloads (dictionaries) of the most similar items.
        """

        vector = self.model.encode(query).tolist()

        search_result = self.qdrant_client.query_points(
            collection_name=self.collection_name,
            query=vector,
            query_filter=None, 
            limit=top,  # 5 the most closest results is enough
        ).points
        payloads = [
            {"payload": hit.payload, "score": hit.score}
            for hit in search_result
        ]
        return payloads
    
if __name__ == "__main__":
    collection_name = COLLECTION_NAME
    model = SentenceTransformer("BAAI/bge-m3", device="cuda")
    qdrant_host = "http://localhost:6333"

    neural_searcher = NeuralSearcher(
        collection_name=collection_name,
        model=model,
        qdrant_host=qdrant_host,
    )
    
    # print(neural_searcher.search("อาชีพผู้ดูแลระบบ"))