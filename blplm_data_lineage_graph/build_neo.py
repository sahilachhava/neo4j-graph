from neomodel import (config, db, StructuredNode, StringProperty, IntegerProperty,
                      UniqueIdProperty, RelationshipTo)

import json
from pathlib import Path
from os import getcwd, makedirs

# Declaring Folder Paths
this_folder = getcwd()

assets_folder = this_folder / Path('assets')

output_files_folder = this_folder / Path('build')
build_folder = output_files_folder / Path('build_neo')
makedirs(build_folder, exist_ok=True)

transform_folder = build_folder / Path('transform')
makedirs(transform_folder, exist_ok=True)

# Using URL - auto-managed
config.DATABASE_URL = 'bolt://neo4j:password@127.0.0.1:7687'  # default
db.set_connection(url='bolt://neo4j:password@127.0.0.1:7687')

class Field(StructuredNode):
    uid = UniqueIdProperty()
    name = StringProperty(required=True)


class Table(StructuredNode):
    uid = UniqueIdProperty()
    name = StringProperty(required=True)
    field = RelationshipTo(Field, 'HAS_FIELD')


class System(StructuredNode):
    uid = UniqueIdProperty()
    name = StringProperty(required=True)
    table = RelationshipTo(Table, 'HAS_TABLE')


class Statement(StructuredNode):
    uid = UniqueIdProperty()
    name = StringProperty(required=True)
    expression = StringProperty(required=False)
    input_field = RelationshipTo(Field, 'HAS_INPUT_FIELD')
    output_field = RelationshipTo(Field, 'HAS_OUTPUT_FIELD')


class Transformation(StructuredNode):
    uid = UniqueIdProperty()
    name = StringProperty(required=True)
    statement = RelationshipTo(Statement, 'HAS_STATEMENT')


class TransformationBetweenTables(StructuredNode):
    uid = UniqueIdProperty()
    name = StringProperty(required=True)
    input_table = RelationshipTo(Table, 'HAS_INPUT_TABLE')
    output_table = RelationshipTo(Table, 'HAS_OUTPUT_TABLE')


class Test:

    def convert_field_to_graph(field_in_json, system):
        # print("converting field")

        # get table name and table node
        table_name = field_in_json['table_name']
        # print('table name: ' + table_name)

        table = Table.nodes.first_or_none(name=table_name)
        if table == None:
            table = Table(name=table_name).save()
            table.save()
            # adding new table to system
            system.table.connect(table)
            # print('new table node: ' + table_name)
        # else:
        # print('existing table node: ' + table_name)

        # get field name and field node
        column_name = field_in_json['column_name']
        field_name = column_name + '_' + table_name
        field = Field.nodes.first_or_none(name=field_name)
        if field == None:
            field = Field(name=field_name).save()
            field.save()
            # print('new field node: ' + column_name)
        else:
            print('existing field node!!!!!: ' + field_name)

        # adding new field to table
        table.field.connect(field)

        '''
        results, meta2 = db.cypher_query(
            "MATCH(a: Table)-[: HAS_FIELD]-(table_field:Field) WHERE  a.name = '" + table_name + "'  RETURN a, table_field")
        if results:
            print('table t36201 found with relationships: ' + results)
        else:
        '''

    if __name__ == '__main__':

        deleteGraph = True
        createGraph = True
        addSchemasToGraph = True
        createMiniGraph = True

        # Opening transformation JSON file
        f = open(output_files_folder / 'all_transformation.map.json')

        if (deleteGraph):
            results, meta = db.cypher_query("match() - [r]->() delete r")
            print("deleted all edges")
            results, meta = db.cypher_query("match(n) delete n")
            print("deleted all nodes")

        # prefix_system dict
        prefix_system_dict = {
            "CDC_ND72": "ignore",
            "cdc_nd72": "ignore",
            "CDC_Y103": "ignore",
            "CDC_X010": "ignore",
            "CDC_GG00_N": "Gilda",
            "CDC_A410_N": "ignore",
            "CDC_COMMON_N": "ignore",
            "CDC_SS10_N": "ignore",

            "gilda": "Gilda",
            "gilda_intermediary": "Gilda Intermediary",
            "coressi": "CoreSSI",
            "coressi_intermediary": "CoreSSI Intermediary",
            "wrk": "CoreSSI",
            "gilda_intermediary": "Gilda Intermediary",
            "CDC_I473_N": "ignore",
            "I532_N_COMMON": "ignore",
            "CDC_1U08_N": "ignore",
            "I532_N_APS": "ignore",
            "CDC_2S10_N": "ignore",
            "CDC_GG00": "Gilda",
            "CDC_2P18_N": "ignore",
            "CDC_2A64_N": "ignore",
            "CDC_1T21_N": "ignore",
            "I532_N_CMEELEC": "ignore",
            "I532_N_CMEPAD": "ignore",
            "CDC_Y103_N": "ignore",
            "I532_N_CORE": "ignore",
            "I532_N_CMESSI": "ignore",
            "CDC_CF9A_N": "ignore"

        }

        tables_to_be_ignored = ["X010_MAX_FINDSIMPACT", "GHR1085_FB_FINDS", "DPDS_30_TRANSFO", "SONACA_30_TRANSFO",
                                "COMMON_FINOFFLOEFF",
                                "T37172_FINCI", "T37150_ATA", "T3717I_RESP", "T36284_ECN", "T36201_RESP",
                                "T37182_FINLO_CIN"]

        # intermediary_tables = ["T37172_FINCI", "T37150_ATA", "T3717I_RESP", "T36284_ECN", "T36201_RESP", "T37182_FINLO_CIN"]

        # opening Gilda schema as json for lookup
        gilda_schema_file = open(assets_folder / 'gilda_schema.json')
        gilda_schema_json = json.load(gilda_schema_file)
        gilda_table_list = []

        for field_in_json in gilda_schema_json:
            column_name = field_in_json['table_name']
            # print(field_in_json['table_name'])
            if (createGraph and addSchemasToGraph) or (createMiniGraph and addSchemasToGraph):
                gilda_system = System.nodes.first_or_none(name="Gilda")
                if gilda_system == None:
                    gilda_system = System(name="Gilda").save()
                    gilda_system.save()

                convert_field_to_graph(field_in_json, gilda_system)

            if not (column_name in gilda_table_list):
                gilda_table_list.append(column_name)

        # opening Sprint schema as json for lookup
        # sprint_schema_file = open('relational_schemas_json/sprint_ng_table_schema.json')
        # sprint_schema_json = json.load(sprint_schema_file)

        # opening CoreSSI schema as json for lookup
        core_schema_file = open(assets_folder / 'core_ssi.json')
        core_schema_json = json.load(core_schema_file)
        core_table_list = []

        for core_field_in_json in core_schema_json:
            column_name = core_field_in_json['table_name']
            # print(core_field_in_json['table_name'])
            if (createGraph and addSchemasToGraph) or (createMiniGraph and addSchemasToGraph):
                core_system = System.nodes.first_or_none(name="CoreSSI")
                if core_system == None:
                    core_system = System(name="CoreSSI").save()
                    core_system.save()

                convert_field_to_graph(core_field_in_json, core_system)

            if not (column_name in core_table_list):
                core_table_list.append(column_name)

        # opening Sprint schema as json for lookup
        # sprint_schema_file = open('relational_schemas_json/sprint_ng_table_schema.json')
        # sprint_schema_json = json.load(sprint_schema_file)

        # returns JSON object as
        # a dictionary
        rows_dict = json.load(f)

        transform_input_tables_from_system_gilda_file = open(
            transform_folder / 'transform_input_tables_from_system_gilda.txt', 'w')
        transform_input_tables_from_system_gilda_list = []
        transform_input_tables_from_system_coressi_file = open(
            transform_folder / 'transform_input_tables_from_system_coressi.txt', 'w')
        transform_input_tables_from_system_coressi_list = []
        transform_input_tables_from_system_intermediary_file = open(
            transform_folder / 'transform_input_tables_from_system_intermediary.txt', 'w')
        transform_input_tables_from_system_intermediary_list = []
        transform_input_tables_ignored_file = open(
            transform_folder / 'transform_input_tables_ignored.txt', 'w')
        transform_input_tables_ignored_list = []

        transform_output_tables_from_system_gilda_file = open(
            transform_folder / 'transform_output_tables_from_system_gilda.txt', 'w')
        transform_output_tables_from_system_gilda_list = []
        transform_output_tables_from_system_coressi_file = open(
            transform_folder / 'transform_output_tables_from_system_coressi.txt', 'w')
        transform_output_tables_from_system_coressi_list = []
        transform_output_tables_from_system_intermediary_file = open(
            transform_folder / 'transform_output_tables_from_system_intermediary.txt', 'w')
        transform_output_tables_from_system_intermediary_list = []
        # transform_output_tables_ignored_file = open('transform_output_tables_ignored.txt', 'w')
        # transform_output_tables_ignored_list = []
        # create graph from transforms in JSON
        i = 0
        for row_dict in rows_dict:

            # get transformations json object
            transformations_object = rows_dict.get(row_dict)
            transformation_array = transformations_object.get(
                'transformations')
            # transformation_array = row_dict.get('transformations')

            for expression_json in transformation_array:

                # ignore None transformation nodes
                input_column_name = expression_json['input_column']
                output_column_name = expression_json['output_column']
                if input_column_name == "None" or output_column_name == "None":
                    continue

                # get system of input table
                # input_table_name = expression_json['input_table']
                input_table_name_with_prefix = expression_json['input_table_with_prefix']
                input_table_prefix = ""

                if (len(input_table_name_with_prefix.split(".")) > 1):
                    input_table_prefix = input_table_name_with_prefix.split('.')[
                        0]
                    input_table_name = input_table_name_with_prefix.split(
                        '.')[-1]
                else:
                    input_table_name = input_table_name_with_prefix

                # get system of input table
                if (input_table_name.lower() in gilda_table_list):
                    input_system = "Gilda"
                    print('table ' + input_table_name +
                          ' found in Gilda schema')
                    if not (input_table_name_with_prefix in transform_input_tables_from_system_gilda_list):
                        transform_input_tables_from_system_gilda_list.append(
                            input_table_name_with_prefix)
                        print(
                            'table ' + input_table_name_with_prefix + ' in Gilda schema',
                            file=transform_input_tables_from_system_gilda_file)
                elif (input_table_name.upper() in core_table_list):
                    input_system = "CoreSSI"
                    print('table ' + input_table_name +
                          ' found in CoreSSI schema')
                    if not (input_table_name_with_prefix in transform_input_tables_from_system_coressi_list):
                        transform_input_tables_from_system_coressi_list.append(
                            input_table_name_with_prefix)
                        print(
                            'table ' + input_table_name_with_prefix + ' in CoreSSI schema',
                            file=transform_input_tables_from_system_coressi_file)
                elif (input_table_name_with_prefix in tables_to_be_ignored):
                    print('Ignored table!: ' + input_table_name_with_prefix)
                    if not (input_table_name_with_prefix in transform_input_tables_ignored_list):
                        transform_input_tables_ignored_list.append(
                            input_table_name_with_prefix)
                        print(
                            'table ' + input_table_name_with_prefix + ' ignored',
                            file=transform_input_tables_ignored_file)
                    continue
                elif (prefix_system_dict.get(input_table_prefix) == 'ignore'):
                    print('Ignored table!: ' + input_table_name_with_prefix)
                    if not (input_table_name_with_prefix in transform_input_tables_ignored_list):
                        transform_input_tables_ignored_list.append(
                            input_table_name_with_prefix)
                        print(
                            'table ' + input_table_name_with_prefix + ' ignored',
                            file=transform_input_tables_ignored_file)
                    continue
                else:
                    input_system = "Intermediary"
                    if not (input_table_name in transform_input_tables_from_system_intermediary_list):
                        transform_input_tables_from_system_intermediary_list.append(
                            input_table_name)
                        print(
                            'table ' + input_table_name + ' in CoreSSI schema',
                            file=transform_input_tables_from_system_intermediary_file)

                input_table_printout = 'input table ' + \
                                       input_table_name_with_prefix + ' belongs to system ' + input_system
                print(input_table_printout)

                # get system of output table
                output_table_name = expression_json['output_table']
                if (output_table_name.lower() in gilda_table_list):
                    output_system = "Gilda"
                    print('table ' + output_table_name +
                          ' found in Gilda schema')
                    if not (output_table_name in transform_output_tables_from_system_gilda_list):
                        transform_output_tables_from_system_gilda_list.append(
                            output_table_name)
                        print(
                            'table ' + output_table_name + ' in Gilda schema',
                            file=transform_output_tables_from_system_gilda_file)
                elif (output_table_name.upper() in core_table_list):
                    output_system = "CoreSSI"
                    print('table ' + output_table_name +
                          ' found in CoreSSI schema')
                    if not (output_table_name in transform_output_tables_from_system_coressi_list):
                        transform_output_tables_from_system_coressi_list.append(
                            output_table_name)
                        print(
                            'table ' + output_table_name + ' in CoreSSI schema',
                            file=transform_output_tables_from_system_coressi_file)
                else:
                    output_system = "Intermediary"
                    print('table ' + output_table_name +
                          ' found in intermediary schema')
                    if not (output_table_name in transform_output_tables_from_system_intermediary_list):
                        transform_output_tables_from_system_intermediary_list.append(
                            output_table_name)
                        print(
                            'table ' + output_table_name + ' in Intermediary schema',
                            file=transform_output_tables_from_system_intermediary_file)

                output_table_printout = 'output table ' + \
                                        output_table_name + ' belongs to system ' + output_system
                print(output_table_printout)

                if (createGraph):

                    # create system node of input table
                    source_system = System.nodes.first_or_none(
                        name=input_system)
                    if source_system == None:
                        source_system = System(name=input_system).save()
                        source_system.save()

                    # create system node of target table
                    target_system = System.nodes.first_or_none(name='CoreSSI')
                    if target_system == None:
                        target_system = System(name='CoreSSI').save()
                        target_system.save()

                    # create graph nodes

                    # input_column_name = expression_json['input_column']
                    expression_name = expression_json['expression']
                    transformation_id = expression_json['transformation_id']
                    # output_column_name = expression_json['output_column']
                    output_table_name = expression_json['output_table']

                    input_table = Table.nodes.first_or_none(
                        name=input_table_name)
                    if input_table == None:
                        input_table = Table(name=input_table_name).save()
                        input_table.save()
                        source_system.table.connect(input_table)

                    output_table = Table.nodes.first_or_none(
                        name=output_table_name)
                    if output_table == None:
                        output_table = Table(name=output_table_name).save()
                        output_table.save()
                        target_system.table.connect(output_table)

                    qual_input_field_name = f"{input_table_name}.{input_column_name}"
                    input_field = Field.nodes.first_or_none(
                        name=qual_input_field_name)
                    if input_field == None:
                        input_field = Field(name=qual_input_field_name).save()
                        input_field.save()
                        input_table.field.connect(input_field)

                    if output_column_name == 'None':
                        qual_output_field_name = 'None'
                    else:
                        qual_output_field_name = f"{output_table_name}.{output_column_name}"
                    output_field = Field.nodes.first_or_none(
                        name=qual_output_field_name)
                    if output_field == None:
                        output_field = Field(
                            name=qual_output_field_name).save()
                        output_field.save()
                        output_table.field.connect(output_field)

                    transformation = Transformation.nodes.first_or_none(
                        name=transformation_id)
                    if transformation == None:
                        transformation = Transformation(
                            name=transformation_id).save()
                        transformation.save()

                    unique_statement_name = transformation_id + expression_name
                    statement = Statement.nodes.first_or_none(
                        name=unique_statement_name)
                    if statement == None:
                        statement = Statement(
                            name=unique_statement_name,
                            expression=expression_name).save()
                        statement.save()
                        statement.input_field.connect(input_field)
                        statement.output_field.connect(output_field)
                        transformation.statement.connect(statement)

                if (createMiniGraph):

                    # create system node of input table
                    source_system = System.nodes.first_or_none(
                        name=input_system)
                    if source_system == None:
                        source_system = System(name=input_system).save()
                        source_system.save()

                    # create system node of target table
                    target_system = System.nodes.first_or_none(name='CoreSSI')
                    if target_system == None:
                        target_system = System(name='CoreSSI').save()
                        target_system.save()

                    # create graph nodes

                    input_column_name = expression_json['input_column']
                    expression_name = expression_json['expression']
                    transformation_id = expression_json['transformation_id']
                    output_column_name = expression_json['output_column']
                    output_table_name = expression_json['output_table']

                    input_table = Table.nodes.first_or_none(
                        name=input_table_name)
                    if input_table == None:
                        input_table = Table(name=input_table_name).save()
                        input_table.save()
                        source_system.table.connect(input_table)

                    output_table = Table.nodes.first_or_none(
                        name=output_table_name)
                    if output_table == None:
                        output_table = Table(name=output_table_name).save()
                        output_table.save()
                        target_system.table.connect(output_table)

                    transformationBetweenTables = TransformationBetweenTables.nodes.first_or_none(
                        name=transformation_id)
                    if transformationBetweenTables == None:
                        transformationBetweenTables = TransformationBetweenTables(
                            name=transformation_id).save()
                        transformationBetweenTables.save()
                        transformationBetweenTables.input_table.connect(
                            input_table)
                        transformationBetweenTables.output_table.connect(
                            output_table)

            # Closing file
        f.close()

        # query to get all graph: MATCH (n) RETURN n LIMIT 25

    # results, meta = db.cypher_query("MATCH (n) RETURN n LIMIT 25")
    # print(results)

    # results, meta = db.cypher_query("MATCH (a:Table)-[:HAS_FIELD]-(b:Cluster)  WHERE a.name = "t36201" RETURN a,b)
    # print(results)

    results, meta = db.cypher_query("RETURN 'done' as message")
    print(results)

    # query returning all gilda schema fields
    # MATCH (system:System {name: "Gilda"})-[:HAS_TABLE]->(table:Table) -[:HAS_FIELD]-> (field:Field) WITH count (distinct field) as total RETURN total

    # query returning all gilda schema fields connected to a statement
    # MATCH (system:System {name: "Gilda"})-[:HAS_TABLE]->(table:Table) -[:HAS_FIELD]-> (field:Field) <-[:HAS_INPUT_FIELD]- (statement:Statement)  WITH count (distinct field) as total RETURN total

    # MATCH p=(:Kingdom)-[*..15]->(:Species) RETURN nodes(p)
    # MATCH p=(system:System {name: "Gilda"})-[:HAS_TABLE]->(:Table)<-[:HAS_INPUT_TABLE]-(:TransformationBetweenTables)-[:HAS_OUTPUT_TABLE]->(:Table)<-[:HAS_TABLE]-(system:System {name: "CoreSSI"})   RETURN nodes(p)

    # MATCH (system:System {name: "CoreSSI"})-[:HAS_TABLE]->(gilda_table:Table)<-[:HAS_INPUT_TABLE]-(:TransformationBetweenTables)-[:HAS_OUTPUT_TABLE]->(:Table)<-[:HAS_TABLE]-(target_system:System {name: "CoreSSI"})   RETURN gilda_table
