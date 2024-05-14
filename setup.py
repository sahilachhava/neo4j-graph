"""Python setup.py for blplm_data_lineage_graph package"""
import io
import os
from setuptools import find_packages, setup


def read(*paths, **kwargs):
    content = ""
    with io.open(
            os.path.join(os.path.dirname(__file__), *paths),
            encoding=kwargs.get("encoding", "utf8"),
    ) as open_file:
        content = open_file.read().strip()
    return content


def read_requirements(path):
    return [
        line.strip()
        for line in read(path).split("\n")
        if not line.startswith(('"', "#", "-", "git+"))
    ]


setup(
    name="blplm_data_lineage_graph",
    version=read("blplm_data_lineage_graph", "VERSION"),
    # description="project_description",
    # url="https://github.com/author_name/project_urlname/",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    author="Acubed",
    packages=find_packages(include=['blplm_data_lineage_graph'], exclude=["tests", ".github"]),
    install_requires=read_requirements("requirements.txt"),
    entry_points={
        "console_scripts": [
            "blplm_data_lineage_graph = blplm_data_lineage_graph.__main__:main",
            "blplm_data_lineage_graph.parse_sql = blplm_data_lineage_graph.__main__:parse_sql",
            "blplm_data_lineage_graph.build_neo = blplm_data_lineage_graph.__main__:build_neo",
            "blplm_data_lineage_graph.query = blplm_data_lineage_graph.__main__:query",
        ]
    },
    # extras_require={"test": read_requirements("requirements-test.txt")},
)
