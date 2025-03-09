from typing import Optional, Literal

from genie_flow_invoker.doc_proc import SimilaritySearchRequest, ChunkedDocument
from pydantic import Field, BaseModel


WeaviateDistanceMethodType = Literal[
    "cosine", "dot", "l2-squared", "hamming", "manhattan"
]


class WeaviateSimilaritySearchRequest(SimilaritySearchRequest):
    method: WeaviateDistanceMethodType = Field(
        default="cosine",
        description="Weaviate similarity distance metric",
    )
    auto_limit: Optional[int] = Field(
        default=None,
        description="The number of auto-cut similarity search results groups",
    )
    alpha: Optional[float] = Field(
        default=None,
        description="The alpha parameter for Weaviate hybrid search",
    )
    collection_name: Optional[str] = Field(
        default=None, description="The collection name for the similarity search"
    )
    tenant_name: Optional[str] = Field(
        default=None, description="The tenant name for the similarity search"
    )
    vector_name: Optional[str] = Field(
        default=None, description="The named vector for the similarity search"
    )


class WeaviatePersistenceRequest(BaseModel):
    collection_name: str = Field(
        description="The collection name to store the chunked document in",
    )
    tenant_name: Optional[str] = Field(
        default=None,
        description="The tenant name to store the chunked document in",
    )
    docment: ChunkedDocument = Field(
        description="The document to persist",
    )