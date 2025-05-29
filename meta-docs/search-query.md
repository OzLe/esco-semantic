“Occupation Graph” Guide – Weaviate-specific Best Practices

1. Schema & Ingestion Guidelines

Area	Recommendation	Rationale
Class design	Keep separate classes: Occupation, Skill, SkillGroup, SkillCollection, ISCOGroup. Store URIs in a conceptUri scalar field and use Weaviate references for edges (hasEssentialSkill, etc.).	Clear separation keeps hybrid and vector search statistics clean and avoids bloated inverted indexes.
Vectorizer choice	If you only need semantic lookup on short labels, set vectorizer:"text2vec-transformer" (or "none" when you will upload your own vectors).	Built-in transformer gives solid embeddings for labels without external infrastructure.
Hybrid indexing	Leave the default inverted index on, so each class supports keyword (BM25) search out of the box.	Enables hybrid search where sparse keywords and dense vectors are fused.  ￼
Batch imports	Use the batch objects API with 100–500 items per batch, then add references in a second pass.	Minimises RPC overhead and respects best-practice import flow.  ￼
Vector sizes	Keep vectors ≤1 024 dimensions. Oversized vectors slow down HNSW insertions and queries.	Aligns with Weaviate performance advice.


⸻

2. Query-building Best Practices

2.1 Hybrid search to find the right occupation

Use hybrid to combine exact title keywords (“data engineer”) with semantic similarity from the text vector:

{
  Get {
    Occupation(
      hybrid:{
        query:"data engineer"
        alpha:0.6          # weight between BM25 (0) and vector (1)
      }
      limit:5
    ){
      conceptUri
      preferredLabel
      _additional { distance score }   # observe ranking metrics
    }
  }
}

Hybrid triggers BM25 + vector search, then fuses the lists with Reciprocal Rank Fusion.  ￼

2.2 Fetch the “Occupation Graph” in a second call

Avoid deeply nested look-ups in one huge query; split the workflow into:
	1.	Step A – get the occupation node(s) with the hybrid query above.
	2.	Step B – fetch the graph for each URI with a targeted query that goes only three hops deep. Deep nesting inside a single GraphQL call can hurt latency.  ￼ ￼

query OccupationGraph($occupationUri:String!){
  Get {
    Occupation(
      where:{
        path:["conceptUri"]
        operator:Equal
        valueText:$occupationUri
      }
    ){
      conceptUri
      preferredLabel
      iscoCode

      broaderOccupation {            # one hop: parent ISCO group or occupation
        conceptUri
        preferredLabel
        broaderOccupation {          # two hops max
          conceptUri
          preferredLabel
        }
      }

      hasEssentialSkill {
        ...SkillFields
      }
      hasOptionalSkill {
        ...SkillFields
      }
    }
  }
}

fragment SkillFields on Skill {
  conceptUri
  preferredLabel
  skillType
  broaderSkill {                    # keep to two hops
    conceptUri
    preferredLabel
  }
  memberOfSkillCollection {         # thematic tags
    preferredLabel
  }
}

Why two calls?
	•	The hybrid call benefits from the inverted index and vector similarity.
	•	The second call is highly selective (where filter on URI) and limits nesting depth, which is faster than a single, ten-level monster query.  ￼

2.3 Pagination & cursors

Use cursor-based pagination for long skill lists:

{
  Get {
    Skill(
      where:{path:["memberOfSkillCollection","conceptUri"],operator:Equal,valueText:".../languageSkillsCollection"},
      after:"eyJ..."          # cursor from previous page
      limit:100
    ){
      conceptUri
      preferredLabel
    }
  }
}

	•	Cursor (after) is cheaper than offset for large result sets.  ￼

2.4 Aggregate queries for facets

{
  Aggregate {
    Skill(
      where:{path:["skillType"],operator:Equal,valueText:"knowledge"}
    ){
      skillType {
        count
      }
      memberOfSkillCollection {
        count
        groupedBy{ value }    # list collection names for UI filters
      }
    }
  }
}

Use Aggregate on scalar filters to build instant facet counts without retrieving every node.  ￼

⸻

3. Performance Tuning Tips

Tip	Benefit	Source
Prefer gRPC over GraphQL in production ingestion or bulk fetch workloads.	40–70 % lower latency and up to 2.6× throughput.	￼
Keep GraphQL responses lean – retrieve only needed fields, drop large _additional payloads when not analysing scores.	Lower transfer size and parsing time.	Weaviate GraphQL design notes
Use filters with high selectivity before vector search when possible (where + nearText).	Reduces candidate set for HNSW vector phase.	￼
Avoid more than 3–4 nested reference hops in a single query; issue additional queries instead.	Prevents query explosion and heap pressure.	￼ ￼
Tune HNSW parameters efConstruction and ef per class if vectors differ greatly in distribution (skills vs occupations).	Balances recall vs latency.	Weaviate performance docs


⸻

4. Putting It All Together – Python Example

from weaviate import Client
import json

client = Client("http://localhost:8080")

# 1. Hybrid search to identify occupation nodes
hybrid_query = (
    client.query
          .get("Occupation", ["conceptUri", "preferredLabel", "_additional { distance score }"])
          .with_hybrid(query="data engineer", alpha=0.6)
          .with_limit(5)
)
hits = hybrid_query.do()["data"]["Get"]["Occupation"]

occupation_uri = hits[0]["conceptUri"]

# 2. Fetch the occupation graph
graph_query = (
    client.query
          .get("Occupation", [
                "conceptUri","preferredLabel","iscoCode",
                {"broaderOccupation":[
                    "conceptUri","preferredLabel",
                    {"broaderOccupation":["conceptUri","preferredLabel"]}
                ]},
                {"hasEssentialSkill":{
                    "conceptUri","preferredLabel","skillType",
                    {"broaderSkill":["conceptUri","preferredLabel"]},
                    {"memberOfSkillCollection":["preferredLabel"]}
                }},
                {"hasOptionalSkill":{
                    "conceptUri","preferredLabel","skillType"
                }}
          ])
          .with_where({"path":["conceptUri"],"operator":"Equal","valueText":occupation_uri})
)
graph = graph_query.do()
print(json.dumps(graph, indent=2))


⸻

5. Validation Checklist
	•	Hybrid search behaviour verified against Weaviate hybrid docs.  ￼
	•	Query depth advice sourced from Weaviate performance guide.  ￼
	•	gRPC throughput numbers taken from Weaviate performance benchmark blog.  ￼
	•	Operator syntax (nearText, hybrid, where) follows the official GraphQL API reference.  ￼ ￼

By integrating these practices you will maximise both relevance (through hybrid search, thematic skill tags and semantic neighbours) and query performance (through shallow queries, selective filters and gRPC-based batch ingestion) while building a robust Occupation Graph in Weaviate.