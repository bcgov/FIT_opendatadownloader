import json
import jsonschema
import logging

from .source_layer import SourceLayer

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

    # for optional tags, set to None where not provided
    parsed = config
    for i, source in enumerate(config):
        for tag in ["source_layer", "query", "primary_key", "metadata_url"]:
            if tag not in source.keys():
                parsed[i][tag] = None

    # validate pk
    for source in parsed:
        if source["primary_key"]:
            if not set(source["primary_key"]).issubset(set(source["fields"])):
                raise ValueError(
                    "Specified primary key(s) must be included in fields tag"
                )

    return [SourceLayer(source) for source in parsed]
