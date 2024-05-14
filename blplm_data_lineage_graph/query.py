"""
Target output format
{
  "tables": {
    "table1": [
      "field1",
      "field2",
      "field3"
    ],
    "table2": [
      "field1",
      "field2",
      "field3"
    ],
    "table3": [
      "field1",
      "field2",
      "field3"
    ]
  },
  "transforms": {
    "transform1": {
      "statement1": "expression1",
      "statement2": "expression2",
      "statement3": "expression3"
    },
    "transform2": {
      "statement1": "expression1",
      "statement2": "expression2",
      "statement3": "expression3"
    }
  },
  "flows": [
    {"table1:field1": "transform1:statement1"},
    "table2:field2": "transform2:statement2",
    "transform1:statement1": "table2:field1",
    "transform2:statement2": "table3:field3"
  ]
}
"""

template = {
    "tables": {
        # "table1": [
        #   # "field1",
        # ],
        # "table3": [
        #   # "field1",
        # ]
    },
    "transforms": {
        # "transform1": {
        #   # "statement1": "expression1"
        # },
        # "transform2": {
        #   # "statement1": "expression1"
        # }
    },
    "flows": [
        # { "table1:field1": "transform1:statement1"}
     ]

}
import json
import hashlib

from neomodel import (config, db)
from pathlib import Path
from os import makedirs, getcwd
from .result_viz import visualize_result

config.DATABASE_URL = 'bolt://neo4j:password@127.0.0.1:7687'  # default
# Using URL - auto-managed
db.set_connection(url='bolt://neo4j:password@127.0.0.1:7687')

flow_set = set()  # temporarily store flows until time to serialize the template to json file

def _expression_hash(expression):
    """
    use md5 hash on the expression to generate a statement_id
    """
    return hashlib.md5(expression.encode('utf-8')).hexdigest()


def process_node(node):
    if list(node.labels)[0] == 'Table':
        return process_table(node)
    elif list(node.labels)[0] == 'Field':
        return process_field(node)
    elif list(node.labels)[0] == 'Statement':
        return process_statement(node)
    elif list(node.labels)[0] == 'System':
        # do nothing
        pass
    else:
        # print(f'unknown type: {node.labels}')
        raise ValueError(f'unknown type: {node.labels}')


def add_table_to_template(table_name):
    if table_name not in template['tables'].keys():
        template['tables'][table_name] = []


def add_field_to_template(table_name, field_name):
    add_table_to_template(table_name)
    if field_name not in template['tables'][table_name]:
        template['tables'][table_name].append(field_name)


def add_transform_to_template(transform_name, expression):
    if transform_name not in template['transforms'].keys():
        template['transforms'][transform_name] = {}
    if _expression_hash(expression) not in template['transforms'][transform_name].keys():
        # count_transforms = len(list(template['transforms'][transform_name].keys()))
        expression_key = _expression_hash(expression)
        template['transforms'][transform_name][expression_key] = expression


def _print_debug_string(cur_start, cur_end, prev_start, prev_end):
    print(
        f"prev_start: {prev_start.get('name')}|{next(iter(prev_start.labels))}\tprev_end: {prev_end.get('name')}|{next(iter(prev_end.labels))}\t"
        f"cur_start: {cur_start.get('name')}|{next(iter(cur_start.labels))}\tcur_end: {cur_end.get('name')}|{next(iter(cur_end.labels))}")


def get_table_from_field_name(field_name):
    return field_name.split('.')[0]

def _add_to_flows(flow_key, flow_value):
    try:
        flow_set.add((flow_key, flow_value))
    except TypeError as e:
        print(f"{flow_key}: {flow_value}")
        raise e
def add_flow_to_template(relationship, prev_relationship):
    prev_start = prev_relationship.start_node
    prev_end = prev_relationship.end_node
    cur_start = relationship.start_node
    cur_end = relationship.end_node

    # print(
    #       f"prev_start: {prev_start.get('name')}\tprev_end: {prev_end.get('name')}\t"
    #       f"cur_start: {cur_start.get('name')}\tcur_end: {cur_start.get('name')}")
    # print(
    #     f"prev_start: {prev_start.get('name')}|{next(iter(prev_start.labels))}\tprev_end: {prev_end.get('name')}|{next(iter(prev_end.labels))}\t"
    #     f"cur_start: {cur_start.get('name')}|{next(iter(cur_start.labels))}\tcur_end: {cur_end.get('name')}|{next(iter(cur_end.labels))}")

    # _print_debug_string(cur_start, cur_end, prev_start, prev_end)
    if 'HAS_TABLE' in [prev_relationship.type, relationship.type]:
        # case omega, system, skip this
        pass
    elif 'HAS_FIELD' in [prev_relationship.type, relationship.type]:
        if 'Table' in prev_start.labels:
            # case alpha1: a table:field to transform:expression or vice versa
            flow_key = f"{prev_start.get('name')}:{prev_end.get('name')}"
            flow_value = f"""{extract_transform_name(cur_start.get('name'), cur_start.get('expression'))}:\
{_expression_hash(cur_start.get('expression'))}"""
            # template['flows'][flow_key] = flow_value
            _add_to_flows(flow_key, flow_value)
        elif 'Statement' in prev_start.labels:
            # case alpha2: transform:expression to table:field
            # pre: table->field_1 post: field_1<-statement
            flow_key = f"""{extract_transform_name(prev_start.get('name'), prev_start.get('expression'))}:\
{_expression_hash(prev_start.get('expression'))}"""
            flow_value = f"{cur_start.get('name')}:{cur_end.get('name')}"
            # template['flows'][flow_key] = flow_value
            _add_to_flows(flow_key, flow_value)
        else:
            print(
                f"prev_start: {prev_start.labels}\tprev_end: {prev_end.labels}\t"
                f"cur_start: {cur_start.labels}\tcur_end: {cur_end.labels}")
            raise ValueError(f'An unknown type of relation was detected.')
    else:
        # case beta: a transform:expression to transform:expression
        if prev_relationship.type == 'HAS_INPUT_FIELD':
            if cur_start.get('expression') != prev_start.get('expression'):
                raise ValueError('the statements must be equal!')
            #  beta1: input first
            # pre: field_1<-statement_1 post: field_2<-statement_1
            # statements must be equal


        if prev_relationship.type == 'HAS_OUTPUT_FIELD':
            # case beta2: output first
            # pre: field_1<-statement_1 post: field1<-statement_2
            # fields must be equal
            # flow pre is transform to field
            # flow post is field to transform
            # this is because the graph structure is field <- statement -> field
            # so statement is always the start node and the field is always the end node
            if cur_end.get('name') != prev_end.get('name'):


                raise ValueError('the fields must be equal!')

            table_pre = get_table_from_field_name(prev_end.get('name'))
            field_pre = prev_end.get('name')
            flow_value_pre = f"{table_pre}:{field_pre}"
            flow_key_pre = f"""{extract_transform_name(prev_start.get('name'), prev_start.get('expression'))}\
:{_expression_hash(prev_start.get('expression'))}"""
            # template['flows'][flow_key_pre] = flow_value_pre
            _add_to_flows(flow_key_pre, flow_value_pre)

            table_post = get_table_from_field_name(cur_end.get('name'))
            field_post = cur_end.get('name')
            flow_key_post = f"{table_post}:{field_post}"
            flow_value_post = f"""{extract_transform_name(cur_start.get('name'), cur_start.get('expression'))}\
:{_expression_hash(cur_start.get('expression'))}"""
            # template['flows'][flow_key_post] = flow_value_post
            _add_to_flows(flow_key_post, flow_value_post)





def process_table(node):
    table_name = node.get('name')
    add_table_to_template(table_name)
    return table_name


def process_field(node):
    qual_field_name = node.get('name')
    field_name = qual_field_name.split('.')[1]
    table_name = get_table_from_field_name(qual_field_name)
    add_field_to_template(table_name, field_name)
    return qual_field_name


def extract_transform_name(transform_name, expression):
    """
    remove the expression from the statement name to get the transform name
    """
    return transform_name.replace(expression, '')


def process_statement(node):
    """
    need to break apart statement name into transform name, statement id, and expression
    eg.
    TRANSFORMATION_413-3CAST(GET_SUB_TYPE('ADAPDS', T36211.RELR || T36211.RELS, 'GILDA') AS VARCHAR(20))
    TRANSFORMATION_413-3CAST(BUILD_INTERNAL_ISSUE(T36211.RELR || T36211.RELS, GETSORTABLEISSUE(CAST('___' AS VARCHAR(3)))) AS VARCHAR(200))
    "transforms": {
     "TRANSFORMATION_413-3": {
           "statement_0_id": "CAST(GET_SUB_TYPE(...)",
           "statement_0_id": "CAST(BUILD_INTERNAL_ISSUE(...)"
        }
    }
    """
    transform_name, expression = node.get('name'), node.get('expression')
    truncated_transform_name = extract_transform_name(transform_name, expression)
    add_transform_to_template(truncated_transform_name, expression)
    return truncated_transform_name


def run_neo4j_to_graphviz_json(query_results):
    """
    each index of result is a path from start to end table
    each path contains a list called relationships,
    each relationship is a structure containing start node, end node, and relation type
    the start node and end node can be used to populate the tables/fields and transforms/statements part
    of the graphviz json template
    use the current relationship and the previous relationship can be used to construct the flow part of
    the graphviz json template
    """
    for path in query_results:
        prev_relationship = None
        for relationship in path[0].relationships:
            # construct table/field and/or transforms/statements using the start and end nodes
            start = relationship.start_node
            end = relationship.end_node
            process_node(start)
            process_node(end)
            # construct flow using relationship and prev_relationship
            if prev_relationship is not None:
                add_flow_to_template(relationship, prev_relationship)
            prev_relationship = relationship


def write_template_to_json(filename):
    template['flows'] = [{tup[0]: tup[1]} for tup in flow_set]
    with open(filename, 'w+') as f:
        json.dump(template, f, indent=4)


def main():
    this_folder = getcwd()
    output_files_folder = this_folder / Path('build')
    build_folder = this_folder / Path('build/query')
    makedirs(build_folder, exist_ok=True)
    results_folder = build_folder / Path('results')
    makedirs(results_folder, exist_ok=True)

    FILENAME = results_folder / 'test_graphviz.json'

    query = """MATCH path = (t:Table {name: 'T36211'})-[:HAS_FIELD]-(f2:Field {name: 'T36211.REL'})
(()<-[:HAS_INPUT_FIELD]-()-[:HAS_OUTPUT_FIELD]->()){1,20}
()<-[:HAS_FIELD]-(t2:Table where t2.name = 'GG00_O_ADAPDS')
RETURN path"""

    results, meta = db.cypher_query(query)
    run_neo4j_to_graphviz_json(results)
    write_template_to_json(FILENAME)
    visualize_result(FILENAME)


if __name__ == '__main__':
    main()
