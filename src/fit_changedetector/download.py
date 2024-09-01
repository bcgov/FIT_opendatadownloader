import json
import logging
import os

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

    # add null layer key if not present
    parsed = config
    for i, source in enumerate(config):
        if "source_layer" not in source.keys():
            parsed[i]["source_layer"] = None

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

    # standardize column names
    df.columns = [x.lower() for x in df.columns]
    df = df.rename_geometry("geom")

    # retain only fields of interest
    df = df[[c.lower() for c in source["fields"]] + ["geom"]]

    return df
