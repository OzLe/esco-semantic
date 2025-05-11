# ESCO Taxonomy Analysis Queries

This document contains useful Cypher queries for analyzing different aspects of the ESCO taxonomy graph.

## Basic Statistics

### Count of Different Node Types
*This query helps you understand the distribution of different types of nodes in your graph (Skills, Occupations, ISCO Groups, etc.). Useful for getting a quick overview of your data volume.*
```cypher
MATCH (n)
RETURN labels(n) as NodeType, count(*) as Count
ORDER BY Count DESC;
```

### Count of Different Relationship Types
*Shows the distribution of relationship types in your graph. Helps identify the most common types of connections between nodes.*
```cypher
MATCH ()-[r]->()
RETURN type(r) as RelationshipType, count(*) as Count
ORDER BY Count DESC;
```

## Skills Analysis

### Top Skills by Number of Relationships
*Identifies the most connected skills in the taxonomy. Skills with many relationships are likely to be fundamental or widely applicable across different domains.*
```cypher
MATCH (s:Skill)
OPTIONAL MATCH (s)-[r]->()
RETURN s.preferredLabel as Skill, count(r) as RelationshipCount
ORDER BY RelationshipCount DESC
LIMIT 20;
```

### Skills with Most Essential Relationships to Occupations
*Finds skills that are considered essential for the most number of occupations. These are likely to be core competencies required across many jobs.*
```cypher
MATCH (s:Skill)-[r:ESSENTIAL_FOR]->(o:Occupation)
RETURN s.preferredLabel as Skill, count(r) as EssentialOccupationCount
ORDER BY EssentialOccupationCount DESC
LIMIT 20;
```

### Skills with Most Optional Relationships to Occupations
*Identifies skills that are optional but commonly associated with many occupations. These might represent valuable but not mandatory competencies.*
```cypher
MATCH (s:Skill)-[r:OPTIONAL_FOR]->(o:Occupation)
RETURN s.preferredLabel as Skill, count(r) as OptionalOccupationCount
ORDER BY OptionalOccupationCount DESC
LIMIT 20;
```

## Occupation Analysis

### Occupations with Most Required Skills
*Shows which occupations require the most essential skills. These might be complex roles requiring diverse competencies.*
```cypher
MATCH (o:Occupation)<-[r:ESSENTIAL_FOR]-(s:Skill)
RETURN o.preferredLabel as Occupation, count(r) as RequiredSkillsCount
ORDER BY RequiredSkillsCount DESC
LIMIT 20;
```

### Occupations with Most Optional Skills
*Identifies occupations with the most optional skills. These might be roles with many possible skill paths or specializations.*
```cypher
MATCH (o:Occupation)<-[r:OPTIONAL_FOR]-(s:Skill)
RETURN o.preferredLabel as Occupation, count(r) as OptionalSkillsCount
ORDER BY OptionalSkillsCount DESC
LIMIT 20;
```

## ISCO Group Analysis

### ISCO Groups with Most Occupations
*Shows which ISCO groups contain the most occupations. Helps identify the most detailed or diverse occupational categories.*
```cypher
MATCH (i:ISCOGroup)<-[r:PART_OF_ISCOGROUP]-(o:Occupation)
RETURN i.preferredLabel as ISCOGroup, i.code as ISCOCode, count(r) as OccupationCount
ORDER BY OccupationCount DESC
LIMIT 20;
```

### ISCO Group Hierarchy Depth
*Analyzes the depth of the ISCO classification hierarchy. Deeper hierarchies indicate more detailed sub-categorization.*
```cypher
MATCH path = (i:ISCOGroup)-[:BROADER_THAN*]->(j:ISCOGroup)
RETURN i.preferredLabel as ISCOGroup, i.code as ISCOCode, length(path) as HierarchyDepth
ORDER BY HierarchyDepth DESC
LIMIT 20;
```

## Skill Group Analysis

### Skill Groups with Most Skills
*Identifies skill groups that contain the most individual skills. These might represent broad skill domains or categories.*
```cypher
MATCH (sg:SkillGroup)-[:BROADER_THAN]->(s:Skill)
RETURN sg.preferredLabel as SkillGroup, count(s) as SkillCount
ORDER BY SkillCount DESC
LIMIT 20;
```

### Skill Group Hierarchy Depth
*Shows the depth of skill group hierarchies. Deeper hierarchies indicate more detailed skill categorization.*
```cypher
MATCH path = (sg:SkillGroup)-[:BROADER_THAN*]->(s:SkillGroup)
RETURN sg.preferredLabel as SkillGroup, length(path) as HierarchyDepth
ORDER BY HierarchyDepth DESC
LIMIT 20;
```

## Cross-Domain Analysis

### Skills Required Across Multiple ISCO Groups
*Identifies skills that are essential across different ISCO groups. These are likely to be transferable skills valuable across various occupational domains.*
```cypher
MATCH (s:Skill)-[:ESSENTIAL_FOR]->(o:Occupation)-[:PART_OF_ISCOGROUP]->(i:ISCOGroup)
RETURN s.preferredLabel as Skill, count(DISTINCT i) as ISCOGroupCount
ORDER BY ISCOGroupCount DESC
LIMIT 20;
```

### Most Common Skill Combinations
*Finds pairs of skills that are most frequently required together for occupations. Useful for identifying common skill sets and potential learning paths.*
```cypher
MATCH (s1:Skill)-[:ESSENTIAL_FOR]->(o:Occupation)<-[:ESSENTIAL_FOR]-(s2:Skill)
WHERE s1 <> s2
RETURN s1.preferredLabel as Skill1, s2.preferredLabel as Skill2, count(*) as CoOccurrenceCount
ORDER BY CoOccurrenceCount DESC
LIMIT 20;
```

## Path Analysis

### Shortest Path Between Two Skills
*Finds the shortest connection between two skills through any type of relationship. Useful for understanding how different skills are related in the taxonomy.*
```cypher
MATCH (s1:Skill {preferredLabel: 'Skill Name 1'}), (s2:Skill {preferredLabel: 'Skill Name 2'})
MATCH path = shortestPath((s1)-[*]-(s2))
RETURN path;
```

### Skills Required for a Specific Occupation Path
*Lists all essential skills required for a specific occupation. Useful for understanding the complete skill requirements for a particular role.*
```cypher
MATCH (o:Occupation {preferredLabel: 'Occupation Name'})<-[:ESSENTIAL_FOR]-(s:Skill)
RETURN o.preferredLabel as Occupation, collect(s.preferredLabel) as RequiredSkills;
```

## Advanced Analysis

### Skills with High Centrality (Betweenness)
*Identifies skills that act as bridges between different parts of the taxonomy. These skills might be particularly important for connecting different domains.*
```cypher
CALL gds.betweenness.stream('myGraph')
YIELD nodeId, score
MATCH (n) WHERE id(n) = nodeId
RETURN n.preferredLabel as Skill, score as BetweennessScore
ORDER BY BetweennessScore DESC
LIMIT 20;
```

### Community Detection in Skills
*Groups skills into communities based on their relationships. Helps identify clusters of related skills and potential skill domains.*
```cypher
CALL gds.louvain.stream('myGraph')
YIELD nodeId, communityId
MATCH (n) WHERE id(n) = nodeId
RETURN n.preferredLabel as Skill, communityId
ORDER BY communityId, n.preferredLabel;
```

## Notes

1. Some queries (like centrality and community detection) require the Neo4j Graph Data Science library to be installed and the graph to be projected.
2. Replace placeholder values (like 'Skill Name 1', 'Occupation Name') with actual values from your graph.
3. Adjust LIMIT clauses based on your needs and data volume.
4. Some queries might need optimization for large datasets.

## Usage Tips

1. Start with basic statistics to understand your graph's structure
2. Use path analysis to understand relationships between different elements
3. Apply filters to focus on specific aspects of the taxonomy
4. Combine queries to create more complex analyses
5. Use visualization tools in Neo4j Browser to better understand the results 