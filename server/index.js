const { gql, ApolloServer } = require("apollo-server");
const { Neo4jGraphQL } = require("@neo4j/graphql");
const { OGM } = require("@neo4j/graphql-ogm");
const neo4j = require("neo4j-driver");
const { logging } = require("neo4j-driver");
require("dotenv").config();

const typeDefs = gql`
  type Object {
    id: Int @unique
    file_type: Int
    file_uri: String
    thumbnail_uri: String
    timestamp: Int  @cypher(statement: "MATCH (this)-[:TAGGED]->(ts : Tag:Timestamp) RETURN ts.name")
    tags: [TagType!]! @relationship(type: "TAGGED", direction: OUT)
  }

  interface Tagging {
    id: Int 
    objects: [Object!]! @relationship(type: "TAGGED", direction: IN)
    tagset: [Tagset!]! @relationship(type: "IN_TAGSET", direction: OUT)
    node: HierarchyNode @relationship(type: "REPRESENTS", direction: IN)
  }

  union TagType = AlphanumericalTag | NumericalTag | DateTag | TimeTag | TimestampTag 

  type AlphanumericalTag implements Tagging @node(label: "Alphanumerical", additionalLabels: ["Tag"]) {
    id: Int @unique
    name: String @alias(property: "name")
    objects: [Object!]! @relationship(type: "TAGGED", direction: IN)
    tagset: [Tagset!]! @relationship(type: "IN_TAGSET", direction: OUT)
    node: HierarchyNode @relationship(type: "REPRESENTS", direction: IN)
  }

  type NumericalTag implements Tagging @node(label: "Numerical", additionalLabels: ["Tag"]) {
    id: Int @unique
    value: Int @alias(property: "name")
    objects: [Object!]! @relationship(type: "TAGGED", direction: IN)
    tagset: [Tagset!]! @relationship(type: "IN_TAGSET", direction: OUT)
    node: HierarchyNode @relationship(type: "REPRESENTS", direction: IN)
  }

  type DateTag implements Tagging @node(label: "Date", additionalLabels: ["Tag"]) {
    id: Int @unique
    date: Date @alias(property: "name")
    objects: [Object!]! @relationship(type: "TAGGED", direction: IN)
    tagset: [Tagset!]! @relationship(type: "IN_TAGSET", direction: OUT)
    node: HierarchyNode @relationship(type: "REPRESENTS", direction: IN)
  }

  type TimeTag implements Tagging @node(label: "Time", additionalLabels: ["Tag"]) {
    id: Int @unique
    time: Time @alias(property: "name")
    objects: [Object!]! @relationship(type: "TAGGED", direction: IN)
    tagset: [Tagset!]! @relationship(type: "IN_TAGSET", direction: OUT)
    node: HierarchyNode @relationship(type: "REPRESENTS", direction: IN)
  }
  
  type TimestampTag implements Tagging @node(label: "Timestamp", additionalLabels: ["Tag"]) {
    id: Int @unique
    timestamp: Int @alias(property: "name")
    objects: [Object!]! @relationship(type: "TAGGED", direction: IN)
    tagset: [Tagset!]! @relationship(type: "IN_TAGSET", direction: OUT)
    node: HierarchyNode @relationship(type: "REPRESENTS", direction: IN)
  }

  type Tagset {
    id: Int @unique
    name: String
    tags: [TagType!]! @relationship(type: "HAS_TAG", direction: OUT)
  }

  type HierarchyNode @node(label: "Node") {
    id: Int @unique
    parent: HierarchyNode! @relationship(type: "HAS_PARENT", direction: OUT)
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

  type Cell {
    idx: Int
    idy: Int
    idz: Int
    file_uri: String
    cnt: Int
  }

  enum FilterType {
    H
    S
    T
  }


  type Query {
    cell(Dimensions: Int, FilterTypes: [FilterType], FilterIDs: [Int]) : [Object!]! 
    state(Dimensions: Int, FilterTypes: [FilterType], FilterIDs: [Int]) : [Cell!]!
  }
`;
const loggingConfig = { logging: neo4j.logging.console('debug') };

const driver = neo4j.driver(
  process.env.NEO4J_URI,
  neo4j.auth.basic(process.env.NEO4J_USER, process.env.NEO4J_PASSWORD),
  loggingConfig
);

const ogm = new OGM({ typeDefs, driver });
//const neoObjects = ogm.model("Object");

const resolvers = {
  Query: {
    cell: async (root, args) => {
      const { Dimensions, FilterTypes, FilterIDs } = args;
      

      if (Dimensions == 0 | FilterTypes == null) {
        query_all = "MATCH(o: Object) RETURN o.id as Id, o.file_uri as fileURI;";
        const result = await driver.session().readTransaction(tx => {
          return tx.run(query_all);
        }
        ).then(result => {
          return result.records.map(record => {
            return record.get("o").properties;
          }
          );
        }
        ).catch(error => {
          console.log(error);
        }
        );
        return result;
      } else {
        console.log("Dimensions: " + Dimensions);
        var frontstr = ""  // add profile / explain here
        var midstr = ""
        var endstr = "RETURN DISTINCT o;"

        for (let i = 0; i < Dimensions; i++) {
          var curr = i + 1;
          if (FilterTypes[i] == "T") {
            midstr += `MATCH (dim${curr}_t: Tag {id: ${FilterIDs[i]}})\n`;
            midstr += `MATCH (dim${curr}_t: Tag)<-[:TAGGED]-(o: Object)\n`;
          } else if (FilterTypes[i] == "H") {
            midstr += `MATCH (dim${curr}_n: Node {id: ${FilterIDs[i]}})\n`;
            midstr += `MATCH (dim${curr}_n)<-[:HAS_PARENT]-(R${curr} : Node)<-[:HAS_PARENT*0..]-(: Node)-[:REPRESENTS]->(: Tag)<-[:TAGGED]-(o: Object)\n`;
          }
        }

        const numtots = FilterIDs.length;
        for (let i = Dimensions; i < numtots; i++) {
          var cur = i + 1;
          if(FilterTypes[i] == "H") {
            midstr += `MATCH (fil${cur}_n: Node {id: ${FilterIDs[i]}})\n`; 
            midstr += `MATCH (fil${cur}_n)<-[:HAS_PARENT]-(R${curr} : Node)<-[:HAS_PARENT*]-(: Node)-[:REPRESENTS]->(: Tag)<-[:TAGGED]-(o: Object)\n`;
          } else if(FilterTypes[i] == "T") {
            midstr += `MATCH (fil${cur}_t: Tag {id: ${FilterIDs[i]}})\n`;
            midstr += `MATCH (fil${cur}_t: Tag)<-[:TAGGED]-(o: Object)\n`;
          } else if(FilterTypes[i] == "S") {
            midstr += `MATCH (fil${cur}_t: Tagset {id: ${FilterIDs[i]}})\n`;
            midstr += `MATCH (fil${cur}_t)<-[:IN_TAGSET]-(:Tag)<-[:TAGGED]-(o: Object)\n`;
          }
        }

        const neo4j_cell_query = frontstr + midstr + endstr;

        const result = await driver.session().readTransaction(tx => {
          return tx.run(neo4j_cell_query);
        }
        ).then(result => {
          return result.records.map(record => {
            return record.get("o").properties;
          }
          );
        }
        ).catch(error => {
          console.log(error);
        }
        );
        return result;
      }
    }
  }
};


const neoSchema = new Neo4jGraphQL({ typeDefs, driver, resolvers });

Promise.all([neoSchema.getSchema(), ogm.init()]).then(([schema]) => {
  const server = new ApolloServer({
    schema,
    context: ({ req }) => ({ req }),
  });

  server.listen().then(({ url }) => {
    console.log(`🚀 GraphQL server ready on ${url}`);
  });
});