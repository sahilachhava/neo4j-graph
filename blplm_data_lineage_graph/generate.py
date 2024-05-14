import pydot
import re
import hashlib

failed_files = []

def generate_gv_file(json_data, output_file):
    gv_file_data = ''
    gv_file_data += 'digraph example {\n'
    gv_file_data += '\tfontname="Helvetica,Arial,sans-serif"\n'
    gv_file_data += '\tnode [fontname="Helvetica,Arial,sans-serif"]\n'
    gv_file_data += '\tedge [fontname="Helvetica,Arial,sans-serif"]\n'
    gv_file_data += '\tgraph [rankdir = "LR"];\n'
    gv_file_data += '\tranksep=2.0;\n'
    gv_file_data += '\tnode [fontsize = "16", shape = "ellipse"];\n\n'

    # Creating Input Tables
    input_tables = {}
    input_table_prefixes = {}
    for transformation in json_data['transformations']:
        input_table_name = transformation['input_table']
        if input_table_prefixes.get(str(input_table_name).lower()) == None:
            input_table_prefixes[str(input_table_name).lower()] = transformation['input_table_with_prefix']
        if input_tables.get(str(input_table_name).lower()) != None:
            if transformation['input_column'] != 'None' and transformation['input_column'] not in input_tables[str(input_table_name).lower()]:
                input_tables[str(input_table_name).lower()].append(transformation['input_column'])
        else:
            input_tables[str(input_table_name).lower()] = [transformation['input_column']]
    
    # Creating Input Table GV Data
    input_table_gv_data = ''
    for input_table in input_tables.keys():
        input_table_gv_data += f'\t"{input_table_prefixes[input_table]}" [\n'
        input_table_gv_data += f'\t\tlabel = "'
        input_table_gv_data += f'\n\t\t\t<table> {input_table_prefixes[input_table]}'
        
        for input_col in input_tables[input_table]:
            input_table_gv_data += f'|\n\t\t\t<{input_col}> {input_col}'

        input_table_gv_data += '"\n'
        input_table_gv_data += '\t\tshape = "record"\n'
        input_table_gv_data += '\t]\n\n'
    gv_file_data += input_table_gv_data
    
    # Creating Expression Tables
    expression_tables = {}
    expression_table_gv_data = ''
    expression_index = 0
    for transformation in json_data['transformations']:
        expression_index += 1
        expression_table_name = transformation['transformation_id']
        input_column = transformation['input_column']
        expression_val = transformation['expression']
        output_column = transformation['output_column']
        if expression_table_name not in expression_tables.keys():
            expression_tables[expression_table_name] = f'\t"{expression_table_name}" [\n'
            expression_tables[expression_table_name] += f'\t\tlabel = "'
            expression_tables[expression_table_name] += f'\n\t\t\t<table> {expression_table_name}'
        if expression_table_name in expression_tables.keys():
            expression_val = expression_val.replace('|', r'\|')
            expression_val = expression_val.replace('>', r'\>')
            expression_val = expression_val.replace('<', r'\<')
            findWhere = re.findall(r'\b' + re.escape('WHERE') + r'\b', expression_val, re.IGNORECASE)
            findGroupBy = re.findall(r'\b' + re.escape('GROUP') + r'\b', expression_val, re.IGNORECASE)
            if len(findWhere) != 0:
                if not expression_tables[expression_table_name].__contains__(f'|\n\t\t\t<WHERE> {expression_val}'):
                    expression_tables[expression_table_name] += f'|\n\t\t\t<WHERE> {expression_val}'
            elif len(findGroupBy) != 0:
                if not expression_tables[expression_table_name].__contains__(f'|\n\t\t\t<GROUPBY> {expression_val}'):
                    expression_tables[expression_table_name] += f'|\n\t\t\t<GROUPBY> {expression_val}'
            else:
                if expression_val == 'NULL' or not expression_tables[expression_table_name].__contains__(f'|\n\t\t\t<{generate_hash(expression_val)}> {expression_val}'):
                    if input_column == expression_val:
                        expression_tables[expression_table_name] += f'|\n\t\t\t<{generate_hash(expression_val)}> =\>'
                    else:
                        expression_tables[expression_table_name] += f'|\n\t\t\t<{generate_hash(expression_val)}> {expression_val}'
    
    for expression_table_name in expression_tables.keys():
        expression_table_gv_data += expression_tables[expression_table_name]
        expression_table_gv_data += '"\n'
        expression_table_gv_data += '\t\tshape = "record"\n'
        expression_table_gv_data += '\t]\n\n'
    gv_file_data += expression_table_gv_data

    # Creating Output Tables
    output_tables = {}
    for transformation in json_data['transformations']:
        output_table_name = transformation['output_table']
        if output_tables.get(output_table_name) != None:
            if transformation['output_column'] != 'None' and transformation['output_column'] not in output_tables[output_table_name]:
                output_tables[output_table_name].append(transformation['output_column'])
        else:
            output_tables[output_table_name] = [transformation['output_column']]
    
    # Creating Output Table GV Data
    output_table_gv_data = ''
    for output_table in output_tables.keys():
        output_table_gv_data += f'\t"{output_table}" [\n'
        output_table_gv_data += f'\t\tlabel = "'
        output_table_gv_data += f'\n\t\t\t<table> {output_table}'
        
        for output_col in output_tables[output_table]:
            output_table_gv_data += f'|\n\t\t\t<{output_col}> {output_col}'

        output_table_gv_data += '"\n'
        output_table_gv_data += '\t\tshape = "record"\n'
        output_table_gv_data += '\t]\n\n'
    gv_file_data += output_table_gv_data

    # Creating Source -> Transformation Connections
    connections = ''
    expression_index = 0
    for transformation in json_data['transformations']:
        expression_index += 1
        input_table_name = transformation['input_table_with_prefix']
        input_column = transformation['input_column']
        transformation_table_name = transformation['transformation_id']
        expression_val = transformation['expression']
        expression_val = expression_val.replace('|', r'\|')
        expression_val = expression_val.replace('>', r'\>')
        expression_val = expression_val.replace('<', r'\<')
        output_table_name = transformation['output_table']
        output_column = transformation['output_column']
        
        findWhere = re.findall(r'\b' + re.escape('WHERE') + r'\b', expression_val, re.IGNORECASE)
        findGroupBy = re.findall(r'\b' + re.escape('GROUP') + r'\b', expression_val, re.IGNORECASE)
        toNumberFormat = re.findall(r'\b' + re.escape('TO_NUMBER') + r'\b', expression_val, re.IGNORECASE)
        inputConnectionString = ''
        outputConnectionString = ''
        if input_column != 'None':
            if len(findWhere) != 0:
                inputConnectionString = f'\t"{input_table_name}":"{input_column}" -> "{transformation_table_name}":"WHERE"\n'
            elif len(findGroupBy) != 0:
                inputConnectionString = f'\t"{input_table_name}":"{input_column}" -> "{transformation_table_name}":"GROUPBY"\n'
            elif len(toNumberFormat) != 0:
                inputConnectionString = f'\t"{input_table_name}":"{input_column}" -> "{transformation_table_name}":"{generate_hash(expression_val)}"\n'
            else:
                inputConnectionString = f'\t"{input_table_name}":"{input_column}" -> "{transformation_table_name}":"{generate_hash(expression_val)}"\n'
        if output_column != 'None':
            if len(toNumberFormat) != 0:
                outputConnectionString = f'\t"{transformation_table_name}":"{generate_hash(expression_val)}" -> "{output_table_name}":"{output_column}"\n'
            else:
                outputConnectionString = f'\t"{transformation_table_name}":"{generate_hash(expression_val)}" -> "{output_table_name}":"{output_column}"\n'
        
        if not connections.__contains__(inputConnectionString):
            connections += inputConnectionString
        if not connections.__contains__(outputConnectionString):
            connections += outputConnectionString
    gv_file_data += connections

    gv_file_data += '}'
    with open(output_file, 'w') as f:
        f.write(gv_file_data)
    
    print(f"Graphviz file generated: {output_file}")  

def convert_gv_to_png(input_gv_file, output_png_file='graphs/sample.png'):
    graph = pydot.graph_from_dot_file(input_gv_file)
    try:
        if graph:
            graph[0].write_png(output_png_file)
            print(f"PNG image generated: {output_png_file}\n")
    except Exception as e:
        with open('failed_logs.n.txt', 'w') as f:
            f.write(f"Error: Unable to load the Graphviz file. {output_png_file}\n")
        print(f"Error: Unable to load the Graphviz file. {output_png_file}\n")
        print(e)

def generate_hash(value):
    sha256_hash = hashlib.sha256()
    sha256_hash.update(value.encode('utf-8'))
    return sha256_hash.hexdigest()