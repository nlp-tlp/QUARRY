# QUARRY

This is the source code for the AJCAI 2022 paper "QUARRY: A Graph Model for Storing and Querying Association Rules".

## Running the code

This repo uses Poetry, so you'll need Python 3.7 and the poetry package. Then simply install via

    poetry install

... and run the code via

    poetry shell
    python run_quarry.py

## Neo4j database

This source code is designed to operate over an existing knowledge graph produced by MWO2KG (code is available [here](https://github.com/nlp-tlp/mwo2kg-and-echidna)). It will work on any Neo4j graph with the same schema, i.e. `Document` nodes and `Instance` nodes connected to those documents.

We have run MWO2KG on the sample dataset already and have created a dump of the Neo4j database containing the knowledge graph for your convenience. It is located at `data/neo4j_dump.dump`. You can load it into Neo4j via the `neo4j-admin` command (more details [here](https://neo4j.com/docs/operations-manual/current/backup-restore/restore-dump/)).

To run the QUARRY code, you will need to have a Neo4j database running.

## Viewing the rules in Neo4j

Once you have run the code, open the Neo4j browser to query the rules. For example, you could try the query in the paper:

    MATCH (e1:Q_Entity)-[c:CONS]->
    (r:Q_Rule)<-[a:ANTE]-(e2:Q_Entity)
    WHERE r.confidence > 0.8
    AND e1.name = "Observation/Leaking"
    RETURN r.name, r.confidence, r.support, r.lift
    ORDER BY r.lift DESCENDING

Note: the query in the paper shows simplified node labels for brevity and consistency with the examples. The following are the actual node labels:

-   `Q_Entity`, rather than `Entity`
-   `Q_Document`, rather than `Transaction`
-   `Q_Rule`, rather than `Rule`

## Future work

We are currently working on an overhaul of the MWO2KG and Echidna code. This rebuild will also run QUARRY as part of the pipeline, with a significant speed improvement. Once this is done, this repository will be obsolete and a link to the updated MWO2KG/Echidna code will be included here.
