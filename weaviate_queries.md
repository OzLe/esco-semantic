# ESCO Taxonomy Analysis Queries for Weaviate

This document contains useful Weaviate queries for analyzing different aspects of the ESCO taxonomy graph.

## Basic Statistics

### Count of Different Node Types
*This query helps you understand the distribution of different types of nodes in your graph (Skills, Occupations, ISCO Groups, etc.). Useful for getting a quick overview of your data volume.*

```graphql
{
  Aggregate {
    Skill {
      meta {
        count
      }
    }
    Occupation {
      meta {
        count
      }
    }
    ISCOGroup {
      meta {
        count
      }
    }
    SkillGroup {
      meta {
        count
      }
    }
  }
}
```

### Count of Different Relationship Types
*Shows the distribution of relationship types in your graph. Helps identify the most common types of connections between nodes.*

```graphql
{
  Aggregate {
    Skill {
      essentialFor {
        meta {
          count
        }
      }
      optionalFor {
        meta {
          count
        }
      }
      broaderThan {
        meta {
          count
        }
      }
      relatedSkill {
        meta {
          count
        }
      }
      partOfSkillGroup {
        meta {
          count
        }
      }
    }
    Occupation {
      partOfISCOGroup {
        meta {
          count
        }
      }
      broaderThan {
        meta {
          count
        }
      }
    }
  }
}
```

## Skills Analysis

### Top Skills by Number of Relationships
*Identifies the most connected skills in the taxonomy. Skills with many relationships are likely to be fundamental or widely applicable across different domains.*

```graphql
{
  Get {
    Skill(
      limit: 20
      sort: {
        path: ["_additional", "certainty"]
        order: desc
      }
    ) {
      preferredLabel
      _additional {
        certainty
      }
      essentialFor {
        ... on Occupation {
          preferredLabel
        }
      }
      optionalFor {
        ... on Occupation {
          preferredLabel
        }
      }
      broaderThan {
        ... on Skill {
          preferredLabel
        }
      }
      relatedSkill {
        ... on Skill {
          preferredLabel
        }
      }
      partOfSkillGroup {
        ... on SkillGroup {
          preferredLabel
        }
      }
    }
  }
}
```

### Skills with Most Essential Relationships to Occupations
*Finds skills that are considered essential for the most number of occupations. These are likely to be core competencies required across many jobs.*

```graphql
{
  Get {
    Skill(
      limit: 20
      sort: {
        path: ["_additional", "certainty"]
        order: desc
      }
    ) {
      preferredLabel
      _additional {
        certainty
      }
      essentialFor {
        ... on Occupation {
          preferredLabel
        }
      }
    }
  }
}
```

### Skills with Most Optional Relationships to Occupations
*Identifies skills that are optional but commonly associated with many occupations. These might represent valuable but not mandatory competencies.*

```graphql
{
  Get {
    Skill(
      limit: 20
      sort: {
        path: ["_additional", "certainty"]
        order: desc
      }
    ) {
      preferredLabel
      _additional {
        certainty
      }
      optionalFor {
        ... on Occupation {
          preferredLabel
        }
      }
    }
  }
}
```

## Occupation Analysis

### Occupations with Most Required Skills
*Shows which occupations require the most essential skills. These might be complex roles requiring diverse competencies.*

```graphql
{
  Get {
    Occupation(
      limit: 20
      sort: {
        path: ["_additional", "certainty"]
        order: desc
      }
    ) {
      preferredLabel
      _additional {
        certainty
      }
      essentialSkills {
        ... on Skill {
          preferredLabel
        }
      }
    }
  }
}
```

### Occupations with Most Optional Skills
*Identifies occupations with the most optional skills. These might be roles with many possible skill paths or specializations.*

```graphql
{
  Get {
    Occupation(
      limit: 20
      sort: {
        path: ["_additional", "certainty"]
        order: desc
      }
    ) {
      preferredLabel
      _additional {
        certainty
      }
      optionalSkills {
        ... on Skill {
          preferredLabel
        }
      }
    }
  }
}
```

## ISCO Group Analysis

### ISCO Groups with Most Occupations
*Shows which ISCO groups contain the most occupations. Helps identify the most detailed or diverse occupational categories.*

```graphql
{
  Get {
    ISCOGroup(
      limit: 20
      sort: {
        path: ["_additional", "certainty"]
        order: desc
      }
    ) {
      preferredLabel
      code
      _additional {
        certainty
      }
      occupations {
        ... on Occupation {
          preferredLabel
        }
      }
    }
  }
}
```

## Skill Group Analysis

### Skill Groups with Most Skills
*Identifies skill groups that contain the most individual skills. These might represent broad skill domains or categories.*

```graphql
{
  Get {
    SkillGroup(
      limit: 20
      sort: {
        path: ["_additional", "certainty"]
        order: desc
      }
    ) {
      preferredLabel
      _additional {
        certainty
      }
      skills {
        ... on Skill {
          preferredLabel
        }
      }
    }
  }
}
```

## Cross-Domain Analysis

### Skills Required Across Multiple ISCO Groups
*Identifies skills that are essential across different ISCO groups. These are likely to be transferable skills valuable across various occupational domains.*

```graphql
{
  Get {
    Skill(
      limit: 20
      sort: {
        path: ["_additional", "certainty"]
        order: desc
      }
    ) {
      preferredLabel
      _additional {
        certainty
      }
      essentialFor {
        ... on Occupation {
          preferredLabel
          partOfISCOGroup {
            ... on ISCOGroup {
              preferredLabel
              code
            }
          }
        }
      }
    }
  }
}
```

### Most Common Skill Combinations
*Finds pairs of skills that are most frequently required together for occupations. Useful for identifying common skill sets and potential learning paths.*

```graphql
{
  Get {
    Occupation(
      limit: 20
    ) {
      preferredLabel
      essentialSkills {
        ... on Skill {
          preferredLabel
        }
      }
    }
  }
}
```

## Path Analysis

### Skills Required for a Specific Occupation Path
*Lists all essential skills required for a specific occupation. Useful for understanding the complete skill requirements for a particular role.*

```graphql
{
  Get {
    Occupation(
      where: {
        path: ["preferredLabel"]
        operator: Equal
        valueString: "Occupation Name"
      }
    ) {
      preferredLabel
      description
      essentialSkills {
        ... on Skill {
          preferredLabel
          description
        }
      }
      optionalSkills {
        ... on Skill {
          preferredLabel
          description
        }
      }
    }
  }
}
```

## Semantic Enrichment Queries

### Complete Occupation Profile
*Extracts all related information for a specific occupation, including required skills, optional skills, ISCO group, and related occupations. Useful for creating comprehensive occupation profiles.*

```graphql
{
  Get {
    Occupation(
      where: {
        path: ["preferredLabel"]
        operator: Equal
        valueString: "Occupation Name"
      }
    ) {
      preferredLabel
      altLabels
      description
      essentialSkills {
        ... on Skill {
          preferredLabel
          description
        }
      }
      optionalSkills {
        ... on Skill {
          preferredLabel
          description
        }
      }
      partOfISCOGroup {
        ... on ISCOGroup {
          preferredLabel
          code
        }
      }
      broaderThan {
        ... on Occupation {
          preferredLabel
        }
      }
      narrowerThan {
        ... on Occupation {
          preferredLabel
        }
      }
    }
  }
}
```

### Complete Skill Profile
*Extracts all related information for a specific skill, including occupations that require it, related skills, and skill groups. Useful for creating comprehensive skill profiles.*

```graphql
{
  Get {
    Skill(
      where: {
        path: ["preferredLabel"]
        operator: Equal
        valueString: "Skill Name"
      }
    ) {
      preferredLabel
      altLabels
      description
      essentialFor {
        ... on Occupation {
          preferredLabel
        }
      }
      optionalFor {
        ... on Occupation {
          preferredLabel
        }
      }
      broaderThan {
        ... on Skill {
          preferredLabel
        }
      }
      narrowerThan {
        ... on Skill {
          preferredLabel
        }
      }
      relatedSkill {
        ... on Skill {
          preferredLabel
        }
      }
      partOfSkillGroup {
        ... on SkillGroup {
          preferredLabel
        }
      }
    }
  }
}
```

## Semantic Search

### Semantic Search for Skills
*Finds skills that are semantically similar to a given query. Useful for discovering related skills based on meaning rather than exact matches.*

```graphql
{
  Get {
    Skill(
      nearText: {
        concepts: ["programming"]
        certainty: 0.6
      }
      limit: 10
    ) {
      preferredLabel
      description
      _additional {
        certainty
      }
    }
  }
}
```

### Find Occupations with Similar Skills
*Finds occupations that require skills semantically similar to a given skill. Useful for discovering related occupations based on skill requirements.*

```graphql
{
  Get {
    Skill(
      where: {
        path: ["preferredLabel"]
        operator: Like
        valueString: "data analysis"
      }
    ) {
      preferredLabel
      essentialFor {
        ... on Occupation {
          preferredLabel
          essentialSkills {
            ... on Skill {
              preferredLabel
              _additional {
                certainty
              }
            }
          }
        }
      }
    }
  }
}
```

## Notes

1. Weaviate queries are based on GraphQL syntax, which is different from Cypher.
2. Weaviate uses a schema-based approach, so all queries must conform to the defined schema.
3. The `_additional` field is used to access metadata like certainty scores.
4. Weaviate's vector search capabilities make semantic search more powerful than in Neo4j.
5. Weaviate doesn't have built-in graph algorithms like centrality or community detection, but these can be implemented using custom modules or external tools.
6. For complex graph traversals, you may need to use multiple queries and combine the results in your application code.
7. Weaviate's cross-references (relationships) are defined in the schema and accessed using nested queries.

## Usage Tips

1. Start with basic queries to understand your data structure
2. Use semantic search for finding related concepts
3. Combine multiple queries for complex analyses
4. Use filters to focus on specific aspects of the data
5. Leverage Weaviate's vector search capabilities for semantic similarity
6. Consider using custom modules for advanced graph algorithms 