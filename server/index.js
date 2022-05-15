const { gql, ApolloServer } = require("apollo-server");
const { Neo4jGraphQL } = require("@neo4j/graphql");
const neo4j = require("neo4j-driver");
require("dotenv").config();

const typeDefs = gql`
  type Object {
    id: Int @unique
    file_type: Int
    file_uri: String
    thumbnail_uri: String
    tags: [TagType!]! @relationship(type: "TAGGED", direction: OUT)
  }

  interface Tag {
    id: Int 
    objects: [Object!]! @relationship(type: "TAGGED", direction: IN)
    tagset: [Tagset!]! @relationship(type: "IN_TAGSET", direction: OUT)
    node: HierarchyNode @relationship(type: "REPRESENTS", direction: IN)
  }

  union TagType = AlphanumericalTag | NumericalTag | DateTag | TimeTag | TimestampTag

  type AlphanumericalTag implements Tag @node(label: "Alphanumerical", additionalLabels: ["Tag"]) {
    id: Int @unique
    name: String @alias(property: "name")
    objects: [Object!]! @relationship(type: "TAGGED", direction: IN)
    tagset: [Tagset!]! @relationship(type: "IN_TAGSET", direction: OUT)
    node: HierarchyNode @relationship(type: "REPRESENTS", direction: IN)
  }

  type NumericalTag implements Tag @node(label: "Numerical", additionalLabels: ["Tag"]) {
    id: Int @unique
    value: Int @alias(property: "name")
    objects: [Object!]! @relationship(type: "TAGGED", direction: IN)
    tagset: [Tagset!]! @relationship(type: "IN_TAGSET", direction: OUT)
    node: HierarchyNode! @relationship(type: "REPRESENTS", direction: IN)
  }

  type DateTag implements Tag @node(label: "Date", additionalLabels: ["Tag"]) {
    id: Int @unique
    date: Date @alias(property: "name")
    objects: [Object!]! @relationship(type: "TAGGED", direction: IN)
    tagset: [Tagset!]! @relationship(type: "IN_TAGSET", direction: OUT)
    node: HierarchyNode! @relationship(type: "REPRESENTS", direction: IN)
  }

  type TimeTag implements Tag @node(label: "Time", additionalLabels: ["Tag"]) {
    id: Int @unique
    time: Time @alias(property: "name")
    objects: [Object!]! @relationship(type: "TAGGED", direction: IN)
    tagset: [Tagset!]! @relationship(type: "IN_TAGSET", direction: OUT)
    node: HierarchyNode! @relationship(type: "REPRESENTS", direction: IN)
  }

  type TimestampTag implements Tag @node(label: "Timestamp", additionalLabels: ["Tag"]) {
    id: Int @unique
    timestamp: Duration @alias(property: "name")
    objects: [Object!]! @relationship(type: "TAGGED", direction: IN)
    tagset: [Tagset!]! @relationship(type: "IN_TAGSET", direction: OUT)
    node: HierarchyNode! @relationship(type: "REPRESENTS", direction: IN)
  }

  type Tagset {
    id: Int @unique
    name: String
    tags: [TagType!]! @relationship(type: "HAS_TAG", direction: OUT)
  }

  type HierarchyNode @node(label: "Node") {
    id: Int @unique
    parent: HierarchyNode @relationship(type: "HAS_PARENT", direction: OUT)
    children: [HierarchyNode!]! @relationship(type: "HAS_PARENT", direction: IN)
    hierarchy: Hierarchy @relationship(type: "IN_HIERARCHY", direction: OUT)
    represents: TagType @relationship(type: "REPRESENTS", direction: OUT)
    subnodes: [HierarchyNode!]! @cypher(statement: "MATCH (this)<-[:HAS_PARENT*]-(n : Node) RETURN n")
    tagname: String @cypher(statement: "MATCH (this)-[:REPRESENTS]->(t : Tag) RETURN t.name")
    objects: [Object!]! @cypher(statement: "MATCH (this)-[:REPRESENTS]->(t : Tag)<-[:TAGGED]-(o : Object) RETURN o")
  }

  type Hierarchy {
    id: Int @unique
    name: String
    root: HierarchyNode @relationship(type: "HAS_ROOT", direction: OUT)
  }
`;

const driver = neo4j.driver(
  process.env.NEO4J_URI,
  neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD)
);

const neoSchema = new Neo4jGraphQL({ typeDefs, driver });

neoSchema.getSchema().then((schema) => {
    const server = new ApolloServer({
        schema: schema,
        context: ({ req }) => ({ req }),
    });

    server.listen().then(({ url }) => {
        console.log(`GraphQL server ready on ${url}`);
    });
});