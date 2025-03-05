# Weaviate Invokers
This package contains the invokers that interact with a Weaviate database.

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

`vector_name`
: an optional name of a particular vector to be used. Defaults to `None` which will point to
the default vector.

`include_vector`
: whether or not to include the vector with the search results. Defaults to `False`.

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

If both having_all and having_any are specified, they are both applied in and AND fashion:
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
* `@` indicating a match when the given value is contained in the list

So, for example:

```json
{
  "having_all": {
    "some_property": "aap",
    "another_property >": 10,
    "a_list_property @": "noot"
  }
}
```
will only return chunks that have `some_property == "aap"` AND `another_property > 10` AND 
`"noot" in a_list_property`. 

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

### vector or named vectors
Every object in the Weaviate database will have one or more vectors. If no named vectors are used, this
would the the single vector, but with named vectors, this would be a dictionary of vectors.

### references
Every object can contain a reference to it's parent chunk. The property for this parent is called
`parent_id`.