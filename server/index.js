const { gql, ApolloServer } = require("apollo-server");
const { Neo4jGraphQL } = require("@neo4j/graphql");
const { OGM } = require("@neo4j/graphql-ogm");
const neo4j = require("neo4j-driver");
const { logging } = require("neo4j-driver");
const fs = require("fs");
const path = require("path");
require("dotenv").config();

const typeDefs = fs
  .readFileSync(path.join(__dirname, "schema.graphql"))
  .toString("utf-8");
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
    cell: async (_root, args) => {
      let session = driver.session();
      const { Dimensions, FilterTypes, FilterIDs, Offset, Limit } = args;

      var frontstr = ""  // add profile / explain here
      var midstr = ""
      var endstr = "RETURN DISTINCT o"
      if (Offset != 0) {
        endstr += ` SKIP ${Offset}`
      }
      if (Limit != 0) {
        endstr += ` LIMIT ${Limit}`
      }
      endstr += ";"

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
        if (FilterTypes[i] == "H") {
          midstr += `MATCH (fil${cur}_n: Node {id: ${FilterIDs[i]}})\n`;
          midstr += `MATCH (fil${cur}_n)<-[:HAS_PARENT]-(R${curr} : Node)<-[:HAS_PARENT*]-(: Node)-[:REPRESENTS]->(: Tag)<-[:TAGGED]-(o: Object)\n`;
        } else if (FilterTypes[i] == "T") {
          midstr += `MATCH (fil${cur}_t: Tag {id: ${FilterIDs[i]}})\n`;
          midstr += `MATCH (fil${cur}_t: Tag)<-[:TAGGED]-(o: Object)\n`;
        } else if (FilterTypes[i] == "S") {
          midstr += `MATCH (fil${cur}_t: Tagset {id: ${FilterIDs[i]}})\n`;
          midstr += `MATCH (fil${cur}_t)<-[:IN_TAGSET]-(:Tag)<-[:TAGGED]-(o: Object)\n`;
        }
      }

      const neo4j_cell_query = frontstr + midstr + endstr;

      const result = await session.readTransaction(tx => {
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
      ).finally(() => {
        session.close();
      });
      return result;

    },
    state: async (_root, args) => {
      let session = driver.session();
      const { Dimensions, FilterTypes, FilterIDs } = args;
      attrs = ["idx", "idy", "idz"]

      var frontstr = ""
      var midstr = ""
      var endstr = "RETURN "

      // handle empty dimensions
      for (let i = Dimensions; i < 3; i++) {
        endstr += `1 as ${attrs[i]}, `
      }

      for (let i = 0; i < Dimensions; i++) {
        var curr = i + 1;
        endstr += `R${curr}.id as ${attrs[i]}, `
        if (FilterTypes[i] == "S") {
          midstr += `MATCH (dim${curr}_t: Tagset {id: ${FilterIDs[i]}})\n`;
          midstr += `MATCH (dim${curr}_ts)<-[:IN_TAGSET]-(R${curr}: Tag)<-[:TAGGED]-(o: Object)\n`;
        } else if (FilterTypes[i] == "H") {
          midstr += `MATCH (dim${curr}_n: Node {id: ${FilterIDs[i]}})\n`;
          midstr += `MATCH (dim${curr}_n)<-[:HAS_PARENT]-(R${curr} : Node)<-[:HAS_PARENT*0..]-(: Node)-[:REPRESENTS]->(: Tag)<-[:TAGGED]-(o: Object)\n`;
        }
      }

      const numtots = FilterIDs.length;
      for (let i = Dimensions; i < numtots; i++) {
        var cur = i + 1;
        if (FilterTypes[i] == "H") {
          midstr += `MATCH (fil${cur}_n: Node {id: ${FilterIDs[i]}})\n`;
          midstr += `MATCH (fil${cur}_n)<-[:HAS_PARENT]-(R${curr} : Node)<-[:HAS_PARENT*]-(: Node)-[:REPRESENTS]->(: Tag)<-[:TAGGED]-(o: Object)\n`;
        } else if (FilterTypes[i] == "T") {
          midstr += `MATCH (fil${cur}_t: Tag {id: ${FilterIDs[i]}})\n`;
          midstr += `MATCH (fil${cur}_t: Tag)<-[:TAGGED]-(o: Object)\n`;
        } else if (FilterTypes[i] == "S") {
          midstr += `MATCH (fil${cur}_t: Tagset {id: ${FilterIDs[i]}})\n`;
          midstr += `MATCH (fil${cur}_t)<-[:IN_TAGSET]-(:Tag)<-[:TAGGED]-(o: Object)\n`;
        }
      }

      endstr += "max(o).file_uri as file_uri, count(distinct o) as cnt;"

      const neo4j_state_query = frontstr + midstr + endstr;


      const result = await session.readTransaction(tx => {
        return tx.run(neo4j_state_query);
      }
      ).then(result => {
        return result.records.map(record => {
          return record.toObject();
        });
      }
      ).catch(error => {
        console.log(error);
      }
      ).finally(() => {
        session.close();
      });
      return result;
    }
  }
};

const neoSchema = new Neo4jGraphQL({ typeDefs, driver, resolvers });

neoSchema.getSchema().then(async (schema) => {
  // Assert indexes and constraints defined using GraphQL schema directives
  await neoSchema.assertIndexesAndConstraints({ options: { create: true } });

  const server = new ApolloServer({
    schema,
  });

  server.listen().then(({ url }) => {
    console.log(`🚀 GraphQL server ready on ${url}`);
  });
});