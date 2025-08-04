# Weaviate Invokers
[![PyPI version](https://badge.fury.io/py/genie-flow-invoker-weaviate.svg?icon=si%3Apython)](https://badge.fury.io/py/genie-flow-invoker-weaviate)
![PyPI - Downloads](https://img.shields.io/pypi/dm/genie-flow-invoker-weaviate)

This package contains the Genie Flow invokers that interact with a Weaviate database.

## Invoker configuration `meta.yaml`
The configuration in `meta.yaml` consists of two sections: `connection` and `parameters`.
Here, the `connection` object contains the relevant parameters for making the connection
to the Weaviate database. The `parameters` object contain parameters that are relevant to
the invoker (such as search parameters). 

### connection settings
The following settings are required to connect to the weaviate database

`http_host`
: the url to connect to over HTTP

`http_port`
: the port to connect to over HTTP

`http_secure`
: boolean indicating if a secure connection needs to be made over gRPC

`grpc_host`
: the url to connect to over gRPC

`grpc_port`
: the port to connect to over gRPC

`grpc_secure`
: boolean indicating if a secure connection is to be made over gRPC

## Similarity Search
A similarity search conducts a nearest-neighbour search within the vector space of a given
collection. Every chunk (Document) that is ingested into the Weaviate database has at least
one vector (the default vector) but can contain multiple named vectors - for instance for
when different embedding models are used for the same chunk.

### query parameters
The following parameters can be given, in the `parameters` section, and/or the JSON object
that is sent to the invoker.

`collection`
: the collection the search needs to be conducted in. Fails if the given collection does
not exist

`tenant`
: an optional name for a particular tenant to use within the collection. Fails if the given
tenant does not exist

`named_vector`
: an optional name of a particular vector to be used. Defaults to `None` which will point to
the default vector.

`include_vector`
: whether to include the vector with the search results. Defaults to `False`.

`method`
: Optional similarity method, can be "cosine", "dot", "l2-squared", "hamming" or "manhattan".
Defaults to "cosine".
See [Available distance metrics](https://weaviate.io/developers/weaviate/config-refs/distances#available-distance-metrics)
for more information.

`parent_strategy`
: Optional strategy to determine how to deal with parent chunks; can be "include" or "replace". The
former will add the parents to the results, the latter will replace the parents. When left
out, no parents are looked up.

`top`
: Optional int indicating how many results to retrieve as a maximum. When left out, all
results are returned.

`horizon`
: Optional float indicating the maximum distance a found chunk can have. When left out,
no limit to the distance is set.

`operation_level`
: an int identifying at what level of the hierarchy the operation needs to be conducted.
When left out, the operation is conducted at every level. The top of the hierarchy has level
zero, the next one done `1`, etc. A negative level will count from the bottom, where `-1`
is the lowest level, `-2` the level above, etc.

### filter by properties
To filter for given values in properties can be done by adding a "having" attribute. This
can be either `having_all` or `having_any`, where the former will only retrieve chunks that
have _all_ matching properties and the latter where the chunk matches any property.

Every chunk stores two sets of properties: the document metadata and the custom properties.
The document metadata are stored in the `ChunkedDocument` that a chunk belongs to. The
custom properties are defined in the `DocumentChunk` in the property `custom_properties` as
a (potentially nested) dictionary.

Referring to a property is done using a dot-separated path, starting with either
"document_metadata" or "custom_property". So, to address a document metadata property
called "language", the property to filter by `document_metadata.language`. A custom property
of a chunk, called "age", would be address using `custom_property.age`.

If both `having_all` and `having_any` are specified, they are both applied in an AND-fashion:
the only chunks that match all the attributes specified by `having_all` as well as having
any matches specified by `having_any` will be returned.

`having_all`
: a dictionary of properties that is used to filter the return values by. When passing
multiple properties, only chunks matching all these properties will be returned.

`having_any`
: a dictionary of properties that is used to filter the return values by. When passing
multiple properties, chunks that match any of these properties will be returned.

By default, the property match is done using equality. The following indicators can be given
for different match types. These indicators should be the last character of the property name,
space-separated from the property name.

* `!=` indicating not-equal to
* `~` as the "like" matcher, where a `*` character in the string will form a wild card
* `>` as greater than
* `>=` as greater than or equal
* `<` as less than
* `<=` as less than or equal
* `contains` indicating a match when the given value is contained in the list
* `in` the value of the property is in a list that is given
* `not-in' the value of the propery is not in a list that is given

So, for example:

```json
{
  "having_all": {
    "some_property": "aap",
    "another_property >": 10,
    "a_list_property contains": "noot"
  }
}
```
will only return chunks that have `some_property == "aap"` AND `another_property > 10` AND 
`"noot"` in `a_list_property`. 

### Doing similarity search:
#### `WeaviateSimilaritySearchInvoker`
This invoker uses the on-the-fly embedding of a search query. All parameters for the search
are expected to be configured in the `meta.yaml`. The full text that is sent to the invoker
is used to do the similarity search, and the embedder that is configured at the Weaviate
server is used to conduct the embedding.

#### `WeaviateVectorSimilaritySearchInvoker`
This invoker expects a JSON dictionary that contains the parameters to use. Parameters that
are left out will be read from the `meta.yaml` configuration. If they do not exist there,
the default will be used, where possible.

It is up to the caller to pass at least the `query_embedding` attribute, as follows:

```json
{
  "query_embedding": [0.1, -0.1]
}
```
but all other parameters can be included in this JSON object.


## Persisting
Persisting is done through the `ChunkedDocument` object. There exist one invoker for this,
called `WeaviatePersistInvoker` than expects a JSON version of a `WeaviatePersistenceRequest`
object.

### Configuration
Configuration of the invoker is done through the `meta.yaml` file. Two keys required:

`connection`
: [see above](#connection-settings)

`persist`
: can contain the `collection_name` and potentially a `tenant_name` that serve as the based for 
the collection and tenant that the invoker refers to. Is overridden when the persist requests
also gives them. 
Also contains a flag `idempotent`, that indicates if inserting a chunk with the same id should
just overwrite that chunk or fail with an exception. Defaults to `False`.

### Weaviate Persistence Request
This object should contain:

`collection_name`
: an optional name of the collection to persist the document into. If not given, the persisting
is done into the collection named in the `meta.yaml` configuration. Lacking that, a `ValueError`
is raised.

`tenant_name`
: an optional name of a tenant within the collection. If not provided, the invoker will fall
back onto a tenant name configured in `meta.yaml`. If no tenant has been configured, this
invoker expects a collection that is not enabled for tenants.

`document`
: The `ChunkedDocument` that needs to be persisted. The ChunkedDocument should contain `chunks`
that can potentially contain an `embedding`. If no embedding is set, it is up to Weaviate to
do the content embedding in the way it is configured.

The invoker returns a JSON object containing the following attributes:

`collection_name`
: the collection that the document (chunks) were stored in

`tenant_name`
: the optional name of the tenant the document chunks were stored in

`nr_inserts`
: the number of chunks inserted

`nr_replaces`
: the number of chunks that have been replaced

## Deleting
There is a number of invokers to delete chunks, by id and by filters, and also whole tenants
or even collections. These deletion invokers expect a JSON version of a delete request. These
requests can optionally take:

`collection_name`
: an optional name of a collection for the delete operation. Any name given will override what
is defined in the `meta.yaml` for this invoker.

`tenant_name`
: an optional name of a tenant for the delete operation. Any name given will override what
is defined in the `meta.yaml`.

All delete chunk invokers will return a JSON object with the following properties:

`matches`
: the number of chunks that matched the filter

`failed`
: the number of chunks for which deletion failed

`successful`
: the number of chunks that were successfully deleted

### Deleting Chunks by ID
Deleting chunks is done through the `WeaviateDeleteChunkInvoker` that expects a
`WeaviateDeleteChunksRequest`. Besides the generic parameters, this request should also
contain:

`chunk_id`
: a chunk id or list of chunk ids of the chunk or chunks that need to be deleted.

### Deleting Chunks by Filter
Deleting chunks with this invoker expects a `WeaviateDeleteByFilterRequest` that, besides
the expected collection and target names, also expects the optional properties `having_all`
and `having_any`. This filter definition is interpreted in the exact same way as the [filter
for search](#filter-by-properties).

### Deleting Chunks by Filename
Because this is a common use case, filtering chunks only by filename has it's own invoker
called `WeaviateDeleteByFilenameInvoker`. The request that is made should contain the property
`filename` that points to the filename of all the chunks that need to be deleted.

### Deleting a Tenant
The `WeaviateDeleteTenantInvoker` removes an entire tenant. There are no further properties
to pass to the request. This may result in a number of errors or a confirmation message. If
the JSON returned contains the property `error_code`, then something went wrong. There will
also be a more descriptive `error` message.

The `error_code` can be:

`ValueError`
: the name of the tenant to delete could not be extracted from the request

`NoMultiTenancySupportException`
: the collection referred to does not support multi-tenancy, so the tenant cannot exist

`TenantNotFoundException`
: the tenant does not exist within the referenced collection

If all goes well, a JSON object is returned containing the collection and tenant name.

### Deleting a Collection
The `WeaviateDeleteCollectionInvoker` only expects the request to contain an optional
`collection_name` attribute.

This invoker may return an error response, by giving an `error_code` to be 
`CollectionNotFoundException` when the collection that is referred to does not exist. If
all goes well, a JSON object is returned with just the `collection_name` property set.

## Data Model
The collection in Weaviate will contain the following data model for each of the objects:

### properties
`content`: the text of the chunk
`hierarchy_level`: the level at which this chunk sits in the document hierarchy. Zero is the root
and higher levels mean that the chunk sits lower in the hierarchy.
`original_span_start` the starting character of the original document that this chunk comes from
`original_span_end`: the last character of the original document that this chunk comes from
`filename`: the name of the original file this chunk is from

Any metadata that is added to the document when it is stored is also added to each and every chunk.
This is done under the property `document_metadata` as a dictionary.

### vector or named vectors
Every object in the Weaviate database will have one or more vectors. If no named vectors are used, 
this would be the single vector, but with named vectors, this would be a dictionary of vectors.

### references
Every object can contain a reference to it's parent chunk. The property for this parent is called
`parent_id`.