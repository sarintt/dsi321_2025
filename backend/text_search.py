from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchText

from config.config import COLLECTION_NAME

class TextSearcher:
    def __init__(self, collection_name: str, qdrant_host: str = "http://localhost:6333"):
        """
        Args:
            collection_name (str): The name of the collection to search in.
            qdrant_host (str, optional): The address of the Qdrant server. Defaults to "http://localhost:6333".
        """
        self.collection_name = collection_name
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

        search_result = self.qdrant_client.scroll(
            collection_name=self.collection_name,
            scroll_filter=Filter(
                must=[
                    FieldCondition(
                        key="content",
                        match=MatchText(text=query),
                    )
                ]),
            with_payload=True,
            with_vectors=False,
            limit=top
        )
        payloads = [
            {"payload": hit.payload, "score": "N/A"}
            for hit in search_result[0]
        ]
        return payloads

if __name__ == "__main__":
    collection_name = COLLECTION_NAME
    qdrant_host = "http://localhost:6333"
    searcher = TextSearcher(
        collection_name=collection_name,
        qdrant_host=qdrant_host
    )
    # print(searcher.search("อาชีพ"))