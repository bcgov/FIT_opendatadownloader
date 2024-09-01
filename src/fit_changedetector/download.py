import json
import logging
import os
import re

from esridump.dumper import EsriDumper
import geopandas
from geopandas import GeoDataFrame
import jsonschema
from pyproj import CRS


LOG = logging.getLogger(__name__)


def parse_config(config):
    """validate and parse config list of dicts"""

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
                raise ValueError("Specified primary key(s) must be included in fields tag")

    return parsed


def download(source):
    """
    Download data, do some simple validation and standardization

    :source: Dict defining source
    :return: BC Albers GeoDataframe, with desired columns in lowercase
    """

    # download data from esri rest api endpoint
    if source["protocol"] == "esri":
        df = GeoDataFrame.from_features(
            features=(
                EsriDumper(source["source"], fields=source["fields"], parent_logger=LOG)
            ),
            crs=4326,
        )

    # download data from location readable by ogr
    elif source["protocol"] == "http":
        df = geopandas.read_file(
            os.path.expandvars(source["source"]),
            layer=source["source_layer"],
            where=source["query"],
        )

    # are expected columns present?
    columns = [x.lower() for x in df.columns]
    for column in source["fields"]:
        if column and column.lower() not in columns:
            raise ValueError(
                f"Download error: {source['out_layer']} - column {column} is not present, modify 'fields'"
            )

    # is there data?
    count = len(df.index)
    if count == 0:
        raise ValueError(
            f"Download error: {source['out_layer']} - no data returned, check source and query"
        )

    # is a crs defined?
    if not df.crs:
        raise ValueError(
            f"Download error: {source['out_layer']} does not have a defined projection/coordinate reference system"
        )

    # presume layer is defined correctly if no errors are raised
    LOG.info(f"Download successful: {source['out_layer']} - record count: {str(count)}")

    # reproject to BC Albers if necessary
    if df.crs != CRS.from_user_input(3005):
        df = df.to_crs("EPSG:3005")

    # standardize column naming
    df = df.rename_geometry("geom")
    cleaned_column_map = {}
    for column in source["fields"]:
        cleaned_column_map[column] = re.sub(r"\W+", "", column.lower().strip().replace(" ", "_"))
    df = df.rename(columns=cleaned_column_map)
    # retain only columns noted in config and geom
    df = df[list(cleaned_column_map.values()) + ["geom"]]

    # if primary key(s) provided, sort data by key(s)
    if source["primary_key"]:
        pks = [cleaned_column_map[k] for k in source["primary_key"]]
        df = df.sort_values(pks)

    return df
