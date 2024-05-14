clear

# clean repo
git clean -xdf

# delete pre-saved build artifacts
rm -rf build

# virtual env
python -m venv venv
source venv/bin/activate

pip install -e .

python -m blplm_data_lineage_graph.parse_sql
python -m blplm_data_lineage_graph.build_neo
python -m blplm_data_lineage_graph.query