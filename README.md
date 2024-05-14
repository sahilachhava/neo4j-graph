# BLPLM Data Lineage Graph

## Installation

### Dependencies

* **Neo4j** running at `127.0.0.1:7687`
* **Python 3**

### This Python Package

To install this Python package, go to a terminal and navigate to the directory where this README.md is located.

```commandline
pip -e .
```

This will install the package in *development mode*, so that any changes you make to the code are reflected when you run
it.

## Running the Software

Typically, you run these steps in order, but you also may run each part independently:

1. `python -m blplm_data_lineage_graph.parse_sql`: Parse the file `assets/sql.xlsx` to extract the SQL queries and their
   individual assignment statements.
2. `python -m blplm_data_lineage_graph.build_neo`: Delete the current contents of the Neo4j graph and populate it using
   the table schema definitions in `assets/` as well as the SQL parsing result from step #1.
3. `python -m blplm_data_lineage_graph.query`: Run queries on the Neo4j graph, producing output data and visualizations.

## The `build` Directory

Each of the phases mentioned above reads and writes files to the `build` directory.

Each phase produces outputs that it stores there. Subsequent phases may read files from that location.

Test copies of these files are stored in the `build` directory for easier developer workflow, but during operations
these files will get overridden.