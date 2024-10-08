import json
import jsonschema
import logging

from .data_source import SourceLayer, clean

__version__ = "0.0.1a1"

LOG = logging.getLogger(__name__)

supported_spatial_types = [
    "POINT",
    "LINESTRING",
    "POLYGON",
    "MULTIPOINT",
    "MULTILINESTRING",
    "MULTIPOLYGON",
]


def parse_config(config_file):
    """validate and parse supplied config file"""

    # read config if a test string is provided
    if type(config_file) is str:
        with open(config_file, "r") as f:
            config = json.load(f)
    # for testing, accept json dict
    elif type(config_file) is list:
        config = config_file
    else:
        raise ValueError(
            "config_file must be a path to a file or a list of dicts (sources)"
        )
    # validate sources against schema doc
    with open("source_schema.json", "r") as f:
        schema = json.load(f)
    jsonschema.validate(instance=config, schema=schema)
    LOG.info("Source json is valid")

    sources = [SourceLayer(source) for source in config]

    # validate primary key(s) and hash key(s) are present in fields
    for source in sources:
        if source.primary_key:
            if not set(source.primary_key).issubset(set(source.fields)):
                raise ValueError(
                    "Specified primary key(s) must be included in fields tag"
                )
        if source.hash_fields:
            if not set(source.hash_fields).issubset(set(source.fields)):
                raise ValueError(
                    "Specified hash field(s) must be included in fields tag"
                )
    return sources
