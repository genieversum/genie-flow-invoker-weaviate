from typing import Any, Literal, Optional

from genie_flow_invoker.doc_proc import ChunkedDocument, SimilaritySearchRequest
from pydantic import BaseModel, Field

WeaviateDistanceMethodType = Literal[
    "cosine", "dot", "l2-squared", "hamming", "manhattan"
]


class WeaviateSimilaritySearchRequest(SimilaritySearchRequest):
    method: WeaviateDistanceMethodType = Field(
        default="cosine",
        description="Weaviate similarity distance metric",
    )
    having_all: Optional[dict[str, Any]] = Field(
        default=None,
        description="Property filter that all need to match",
    )
    having_any: Optional[dict[str, Any]] = Field(
        default=None,
        description="Property filter that any need to match",
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
    collection_name: Optional[str] = Field(
        default=None,
        description="The collection name to store the chunked document in",
    )
    tenant_name: Optional[str] = Field(
        default=None,
        description="The tenant name to store the chunked document in",
    )
    document: ChunkedDocument | list[ChunkedDocument] = Field(
        description="The document to persist",
    )
    batch_size: int = Field(
        default=1000,
        description="The batch size for inserting chunks",
    )


class WeaviatePersistenceResponse(BaseModel):
    collection_name: str = Field(
        description="The collection name to store the chunked document in",
    )
    tenant_name: Optional[str] = Field(
        default=None,
        description="The tenant name to store the chunked document in",
    )
    nr_inserts: int = Field(
        description="The number of chunks inserted in the collection",
    )
    nr_replaces: int = Field(
        description="The number of chunks replaced in the collection",
    )


class WeaviateDeleteMessage(BaseModel):
    collection_name: Optional[str] = Field(
        default=None,
        description="The collection name to delete the chunked documents from",
    )
    tenant_name: Optional[str] = Field(
        default=None,
        description="The tenant name to delete the chunked documents from",
    )


class WeaviateDeleteChunksRequest(WeaviateDeleteMessage):
    chunk_id: Optional[str | list[str]] = Field(
        default=None,
        description="The ID or list of IDs of the chunk(s) to delete",
    )


class WeaviateDeleteByFilenameRequest(WeaviateDeleteMessage):
    filename: str = Field(
        description="The filename to filter all chunks to delete",
    )


class WeaviateDeleteByFilterRequest(WeaviateDeleteMessage):
    having_all: Optional[dict[str, Any]] = Field(
        default=None,
        description="Property filter that all need to match",
    )
    having_any: Optional[dict[str, Any]] = Field(
        default=None,
        description="Property filter that any need to match",
    )


class WeaviateDeleteErrorResponse(WeaviateDeleteMessage):
    error_code: str = Field(
        description="The code name of the error",
    )
    error: str = Field(
        description="The error descriptive message",
    )

    @classmethod
    def from_exception(cls, exc: Exception) -> "WeaviateDeleteErrorResponse":
        return cls(
            collection_name=getattr(exc, "collection_name", None),
            tenant_name=getattr(exc, "tenant_name", None),
            error_code=exc.__class__.__name__,
            error=str(exc),
        )
