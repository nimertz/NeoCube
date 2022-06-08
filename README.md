# PhotoCubeGraph

## Neo4j
### install
    sudo apt-get install neo4j


### Load PhotoCube data
**CSV files needed:**
* cubeobjects.csv
* tags.csv
* alphanumerical_tags.csv
* numerical_tags
* date_tags.csv
* time_tags.csv
* timestamp_tags.csv
* objecttagrelations.csv
* tagsets.csv
* nodes.csv
* hierarchies.csv

1. Place photocube csv data in the neo4j import folder. 

        <neo4j-home>/import
2. Run the [photocube_populate.cypher](scripts/photocube_populate.cypher) script to load the data. This script requires the Neo4j apoc library for timestamp tag name formatting.
   
        cypher-shell -u neo4j -d neo4j -f photocube_populate.cypher

## Neo4j & PostgreSQL Benchmarking suite
Located in the [benchmarking](benchmarking/) directory
### Dependencies
numpy, click, seaborn, neo4j, psycopg, python-dotenv

    pip install -r requirements.txt

### Environment variables
First place .env file in the server folder with the following properties:

    # Neo4j - uses default database (neo4j)
    NEO4J_URL=bolt://localhost:7687
    NEO4J_USER=<username>
    NEO4J_PASSWORD=<password>
    # PostgreSQL
    PSQL_HOST=127.0.0.1
    PSQL_PORT=5432
    PSQL_USER=<username>
    PSQL_PASSWORD=<password>
    PSQL_DB=<database name>
    # LSC dataset 
    MAX_TAG_ID=193189
    MAX_TAGSET_ID=21
    MAX_HIERARCHY_ID=3
    MAX_NODE_ID=8842
    MAX_OBJECT_ID=183386


### Run benchmarks

    python3 PhotoCubeBenchmarker.py --help

    python3 PhotoCubeBenchmarker.py complete --r 5

## GraphQL node.js server
https://neo4j.com/product/graphql-library/

Located in the [server](server/) directory.
### Dependencies
@neo4j/graphql @neo4j/graphql-ogm neo4j-driver graphql apollo-server dotenv 

**Install dependencies:**

    npm install

### Environment variables
First place .env file in the server folder with the following properties:

    NEO4J_USER=<username> 
    NEO4J_PASSWORD=<password>
    NEO4J_URI=bolt://localhost:7687
default user and password are neo4j and neo4j.

### Run server
    node index.js

The server can be visited at http://localhost:4000. GraphQL queries can be built here through Apollo Studio. 

## PhotoCube state generators
Navigate to the [generators](generators/) directory and run the following commands:

    python3 postgresql_state_generator_V7.py < 3d.txt
    python3 neo4j_state_generator_V1.py < 3d.txt