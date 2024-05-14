from neomodel import config, db


def run_cypher_file(file_path, neo4j_db):
    check_duplicates = []
    with open(file_path, 'r') as f:
        cypher_query = f.read()

    for line in cypher_query.split("\n"):
        print("Cypher Query Executed -> " + line)
        if line not in check_duplicates and line != "":
            check_duplicates.append(line)
            neo4j_db.cypher_query(line)


if __name__ == '__main__':
    config.DATABASE_URL = 'bolt://neo4j:password@localhost:7687'
    db.set_connection(url='bolt://neo4j:password@localhost:7687')

    cypher_file_path = "all_queries.cypher"
    result = run_cypher_file(cypher_file_path, db)
    print(result)
