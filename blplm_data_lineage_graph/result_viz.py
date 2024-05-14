import pydot
from json import dump, load
import datetime
from pathlib import Path
from os import makedirs, getcwd, listdir
from os.path import dirname, realpath, join, isfile

# Declaring Folder Paths
this_folder = getcwd()

assets_folder = this_folder / Path('assets')

query_results_folder = this_folder / Path('build/query')

build_folder = query_results_folder
output_files_folder = this_folder / Path('build')

gv_files_folder = build_folder / Path("gv_files")
makedirs(gv_files_folder, exist_ok=True)

png_files_folder = build_folder / Path("png_files")
makedirs(png_files_folder, exist_ok=True)

dir_of_this_file = Path(dirname(realpath(__file__)))
test_asset_folder = dir_of_this_file / Path('test_assets')


def build_gv(data):
    gv_file_lines = [
        'digraph example {',
        '  fontname="Helvetica,Arial,sans-serif"',
        '  node [',
        '    fontname="Helvetica,Arial,sans-serif"',
        '    shape="plain" style="filled" fillcolor="#ffffff"',
        '    pencolor="#00000044"',
        '  ]',
        '  edge [fontname="Helvetica,Arial,sans-serif"]',
        '  graph [rankdir = "LR"];',
        '  ranksep=2.0;',
        '',
    ]
    gv_file_lines.extend(content(data))
    gv_file_lines.append('}')

    return '\n'.join(gv_file_lines)


def content(data):
    c = []

    if 'special' in data:
        specials = data['special']
    else:
        specials = []

    for table, fields in data['tables'].items():
        if table in specials:
            border = 4
        else:
            border = 0

        t = [
            f'"{table}" [',
            '  pencolor="#000000" ',
            '  fillcolor="#00888822" ',
            f'  label = <<table border="{border}" cellborder="1" cellspacing="0" cellpadding="10">',
            f'    <tr><td><i><b>{table}</b></i></td></tr>',
        ]

        for field in fields:
            t.append(f'    <tr><td port="{field}">{field}</td></tr>')

        t.append(f'  </table>>')

        t.append(f']')

        for line in t:
            c.append('  ' + line)
        c.append('')

    for transform, statements in data['transforms'].items():
        t = [
            f'"{transform}" [',
            '  pencolor="#000000" ',
            '  fillcolor="#88ff0022" ',
            '  label = <<table border="2" cellborder="1" cellspacing="0" cellpadding="10">',
            f'    <tr><td><b><i>{transform}</i></b></td></tr>',
        ]

        for statement, expression in statements.items():
            t.append(f'    <tr><td port="{statement}">{expression}</td></tr>')

        t.append(f'  </table>>')
        t.append(f']')

        for line in t:
            c.append('  ' + line)
        c.append('')

    for flow_dict in data['flows']:
        for src, dst in flow_dict.items():
            src_part1, src_part2 = src.split(':')
            dst_part1, dst_part2 = dst.split(':')

            if '.' in src_part2:
                src_part2 = src_part2.split('.')[1]
            if '.' in dst_part2:
                dst_part2 = dst_part2.split('.')[1]

            c.append(
                f'  "{src_part1}":"{src_part2}" -> "{dst_part1}":"{dst_part2}"'
            )

    return c


def visualize_result(file_path):
    filename = file_path.name

    with open(file_path) as f:
        data = load(f)
    result = build_gv(data)

    path_gv = gv_files_folder / Path(filename).with_suffix('.gv')
    with open(path_gv, 'w') as f:
        f.write(result)

    path_png = png_files_folder / Path(filename).with_suffix('.png')
    graph = pydot.graph_from_dot_file(path_gv)
    graph[0].write_png(path_png)


def main():
    for filename in listdir(query_results_folder):
        file_path = Path(join(query_results_folder, filename))

        if isfile(file_path):
            visualize_result(file_path)


if __name__ == '__main__':
    current_time = datetime.datetime.now()
    print(f'{current_time}: running')

    main()

    print('done')
