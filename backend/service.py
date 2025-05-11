import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sentence_transformers import SentenceTransformer

from config.config import COLLECTION_NAME
from backend.nerual_search import NeuralSearcher
from backend.text_search import TextSearcher

from config.logging_config.modern_log import LoggingConfig

# ---------------------------------------------------------------------------- #
#                                LOGGING CONFIG                                #
# ---------------------------------------------------------------------------- #
logger = LoggingConfig(level="INFO").get_logger()
# ---------------------------------------------------------------------------- #

model = SentenceTransformer("BAAI/bge-m3", device="cuda")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


neural_searcher = NeuralSearcher(
    collection_name=COLLECTION_NAME,
    model=model,
)
text_searcher = TextSearcher(
    collection_name=COLLECTION_NAME,
)


@app.get("/api/search")
async def read_item(q: str, neural: bool = True, top: int = 10):
    return {
        "result": neural_searcher.search(query=q, top=top)
        if neural else text_searcher.search(query=q, top=top)
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)