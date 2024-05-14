from sqlglot import parse_one, exp
import json
import re
from generate import convert_gv_to_png, generate_gv_file
from sql_formatter.core import format_sql
import pandas as pd
from pathlib import Path
from os import makedirs, getcwd
import concurrent.futures

# Switch 'parallel' to False to make debugging easier.
parallel = True
# parallel = False

# Declaring Folder Paths
this_folder = getcwd()
assets_folder = this_folder / Path('assets')
test_assets_folder = this_folder / Path('blplm_data_lineage_graph') / Path('test_assets')
build_folder = this_folder / Path('build/parse_sql')
output_files_folder = this_folder / Path('build')

graphs_folder = build_folder / Path('graphs')
gv_files_folder = build_folder / Path("gv_files")
json_files_folder = build_folder / Path("json_files")
sql_files_folder = build_folder / Path("sql_files")


def identifyDBSystem(table_name):
    rawGildaTablePattern = r'^T\d{5}$'
    rawGildaTableIntermediaryPattern = r'T\d{5}'
    gildaTablePrefix = r'GG00'
    coreSSIPattern = r'(O_|L_)'

    if (bool(re.match(rawGildaTablePattern, table_name)) or bool(re.search(gildaTablePrefix, table_name))):
        return 'Gilda'
    elif (bool(re.match(rawGildaTableIntermediaryPattern, table_name))):
        return 'Gilda Intermediary'
    elif (bool(re.search(coreSSIPattern, table_name))):
        return 'CoreSSI'
    else:
        return 'Unknown'


def filterSpecialCharExpression(expression):
    if type(expression) == str and "ON OVERFLOW TRUNCATE" in expression:
        expression = expression.replace(", ' '  ON OVERFLOW TRUNCATE '...'", "")
        expression = expression.replace(", ' ' ON OVERFLOW TRUNCATE '...'", "")
        expression = expression.replace(", ',' ON OVERFLOW TRUNCATE '...'", "")
        expression = expression.replace(",' ' ON OVERFLOW TRUNCATE '...'", "")
        expression = expression.replace(",', ' ON OVERFLOW TRUNCATE '...'", "")
        expression = expression.replace(",';' ON OVERFLOW TRUNCATE '...'", "")
        expression = expression.replace(",' ' ON OVERFLOW TRUNCATE", "")
        expression = expression.replace(", \n          ';' ON OVERFLOW TRUNCATE '...'", "")
        return format_sql(expression)
    return expression


def process_query(entry):
    row_id = entry['row_id']
    output_file_name = entry['output_file_name']
    output_table = entry['output_table']
    sql_query = entry['sql_query']
    filtered_query = filterSpecialCharExpression(sql_query)

    sql_file_name = output_file_name + ".sql"
    sql_file_path = sql_files_folder / sql_file_name
    with open(sql_file_path, "w") as f:
        f.write(str(sql_query))

    # Parse the SQL query
    try:
        parsed_query = parse_one(filtered_query)

        # Initialize a dictionary to map aliases to tables
        parsed_transaction = {}
        transformations = []
        tables = []
        table_index = 1
        transformation_index = 0
        table_name_with_prefix = {}

        for select_expr in parsed_query.find_all(exp.Select):
            table_name = select_expr.find(exp.Table).alias_or_name

            if select_expr.parent_select != None:
                continue

            if table_name in tables:
                table_index += 1
                transformation_index += 1
            else:
                tables.append(table_name)
                transformation_index += 1

            for table in select_expr.find_all(exp.Table):
                table_name_with_prefix[
                    str(table.alias_or_name).lower()] = table.db + '.' + table.name if table.db else table.name
                
            for table in select_expr.find_all(exp.Subquery):
                table_name_with_prefix[str(table.alias_or_name).lower()] = table.alias_or_name

            for select_column in select_expr.args['expressions']:
                flag = False
                if isinstance(select_column, exp.Alias):
                    output_column_name = select_column.alias_or_name
                    if type(select_column) == exp.Literal:
                        expression = str(select_column)
                    elif select_column.this == None:
                        expression = select_column.sql()
                    else:
                        expression = select_column.this.sql()
                else:
                    output_column_name = select_column.name
                    if type(select_column) == exp.Literal:
                        expression = str(select_column)
                    elif select_column.this == None:
                        expression = select_column.sql()
                    else:
                        expression = select_column.this.sql()

                for column in select_column.find_all(exp.Column):
                    tableName = table_name
                    if column.table != None and column.table != '' and column.table != 'None':
                        tableName = column.table
                    # if "." in expression:
                    #     expression = expression.split('.')[1]
                    transformations.append({
                        'input_table': tableName,
                        'input_table_db_system': identifyDBSystem(table_name_with_prefix[str(tableName).lower()]),
                        'input_table_with_prefix': table_name_with_prefix[str(tableName).lower()],
                        'input_column': column.name,
                        'transformation_id': 'TRANSFORMATION_' + str(row_id) + '-' + str(transformation_index),
                        'expression': expression,
                        'output_column': output_column_name,
                        'output_table_db_system': identifyDBSystem(output_table),
                        'output_table': output_table
                    })
                    flag = True

                if not flag:
                    tableName = table_name
                    # if "." in expression:
                    #     expression = expression.split('.')[1]
                    transformations.append({
                        'input_table': tableName,
                        'input_table_db_system': identifyDBSystem(table_name_with_prefix[str(tableName).lower()]),
                        'input_table_with_prefix': table_name_with_prefix[str(tableName).lower()],
                        'input_column': 'None',
                        'transformation_id': 'TRANSFORMATION_' + str(row_id) + '-' + str(transformation_index),
                        'expression': expression,
                        'output_column': output_column_name,
                        'output_table_db_system': identifyDBSystem(output_table),
                        'output_table': output_table
                    })

            # Extracting Data from WHERE clause
            for whereClause in select_expr.find_all(exp.Where):
                breakingConditions = str(whereClause)
                if breakingConditions != 'None':
                    whereClauseColumns = list(whereClause.find_all(exp.Column))
                    for indx in range(len(whereClauseColumns)):
                        whereColumn = whereClauseColumns[indx]
                        tableName = table_name
                        if whereColumn.table != None and whereColumn.table != '' and whereColumn.table != 'None':
                            tableName = whereColumn.table
                        transformations.append({
                            'input_table': tableName,
                            'input_table_db_system': identifyDBSystem(table_name_with_prefix[str(tableName).lower()]),
                            'input_table_with_prefix': table_name_with_prefix[str(tableName).lower()],
                            'input_column': whereColumn.name,
                            'transformation_id': 'TRANSFORMATION_' + str(row_id) + '-' + str(transformation_index),
                            'expression': breakingConditions,
                            'output_column': 'None',
                            'output_table_db_system': identifyDBSystem(output_table),
                            'output_table': output_table
                        })

            # Extracting Data from GROUP BY clause
            groupByClause = select_expr.find(exp.Group)
            breakingGroupByClauses = str(select_expr.find(exp.Group))
            if str(groupByClause) != 'None':
                groupByClauseColumns = list(groupByClause.find_all(exp.Column))

                for indx in range(len(groupByClauseColumns)):
                    groupByColumn = groupByClauseColumns[indx]
                    tableName = table_name
                    if groupByColumn.table != None and groupByColumn.table != '' and groupByColumn.table != 'None':
                        tableName = groupByColumn.table
                    transformations.append({
                        'input_table': tableName,
                        'input_table_db_system': identifyDBSystem(table_name_with_prefix[str(tableName).lower()]),
                        'input_table_with_prefix': table_name_with_prefix[str(tableName).lower()],
                        'input_column': groupByColumn.name,
                        'transformation_id': 'TRANSFORMATION_' + str(row_id) + '-' + str(transformation_index),
                        'expression': breakingGroupByClauses,
                        'output_column': 'None',
                        'output_table_db_system': identifyDBSystem(output_table),
                        'output_table': output_table
                    })

        parsed_transaction['transformations'] = transformations

        output_table_file_name = output_file_name + ".json"
        output_table_file_path = json_files_folder / output_table_file_name
        json.dump(parsed_transaction, fp=open(output_table_file_path, 'w'), indent=4)
        print(f"JSON file generated: {output_table_file_name}")  

        # makedirs(gv_files_folder, exist_ok=True)
        # output_table_gv_file_name = output_file_name + ".gv"
        # output_table_gv_file_path = gv_files_folder / output_table_gv_file_name
        # generate_gv_file(parsed_transaction, output_table_gv_file_path)

        # makedirs(graphs_folder, exist_ok=True)
        # output_table_png_file_name = output_file_name + ".png"
        # output_table_png_file_path = graphs_folder / output_table_png_file_name
        # convert_gv_to_png(output_table_gv_file_path, output_table_png_file_path)

        return ('ROW_' + str(row_id), parsed_transaction)

    except Exception as e:
        print(f"Error: Unable to parse the sql transformation no {row_id}.\n")
        print(e)


def main():
    makedirs(graphs_folder, exist_ok=True)
    makedirs(gv_files_folder, exist_ok=True)
    makedirs(json_files_folder, exist_ok=True)
    makedirs(sql_files_folder, exist_ok=True)

    excel_file_path = assets_folder / "sql.xlsx"
    df = pd.read_excel(excel_file_path, sheet_name='Scheduled')
    dfDynamic = pd.read_excel(excel_file_path, sheet_name='Dynamic')
    excel_output_table_name = df.get('OBJECT_NAME').tolist()
    excel_output_table_name += dfDynamic.get('OBJECT_NAME').tolist()
    excel_column_transformations = df.get('TRANSFORMATION').str.replace('_x0000_', '').tolist()
    excel_column_transformations += dfDynamic.get('TRANSFORMATION').str.replace('_x0000_', '').tolist()
    row_id = 1

    entries = []
    for index in range(len(excel_output_table_name)):
        row_id += 1
        output_file_name = "TRANSFORMATION_" + str(row_id)
        output_table = excel_output_table_name[index]
        sql_query = excel_column_transformations[index]
        
        if pd.isnull(sql_query):
            print("Skipped empty row: ", row_id)
            continue
        entries.append({
            'row_id': row_id,
            'output_file_name': output_file_name,
            'output_table': output_table,
            'sql_query': sql_query,
        })

    if not parallel:
        max_workers = 1
    else:
        max_workers = None

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_query, entries))

    all_transformations = {}
    all_transformation_array = []
    for result in results:
        if result is None:
            continue
        all_transformations[result[0]] = result[1]
        all_transformation_array.append(result[1])

    json.dump(all_transformations, fp=open(output_files_folder / 'all_transformation.map.json', 'w'), indent=2)
    json.dump(all_transformation_array, fp=open(output_files_folder / 'all_transformation.array.json', 'w'), indent=2)


if __name__ == '__main__':
    main()