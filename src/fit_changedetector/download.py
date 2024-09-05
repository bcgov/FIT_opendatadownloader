import hashlib
import json
import logging
import os
import re

import bcdata
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
                raise ValueError(
                    "Specified primary key(s) must be included in fields tag"
                )

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

    # download from BC WFS
    elif source["protocol"] == "bcgw":
        df = bcdata.get_data(source["source"], query=source["query"], as_gdf=True)

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
        cleaned_column_map[column] = re.sub(
            r"\W+", "", column.lower().strip().replace(" ", "_")
        )
    df = df.rename(columns=cleaned_column_map)
    # retain only columns noted in config and geom
    df = df[list(cleaned_column_map.values()) + ["geom"]]

    # if primary key(s) provided, sort data by key(s)
    if source["primary_key"]:
        pks = [cleaned_column_map[k] for k in source["primary_key"]]
        df = df.sort_values(pks)

    # default to creating hash on all input fields, but if supplied use the pk(s)
    if pks:
        hashcols = pks
    else:
        hashcols = df.columns

    # check that output hashed id column is not already present
    if "fcd_load_id" not in df.columns:
        load_id_column = "fcd_load_id"
    else:
        raise Warning(
            "column fcd_load_id is present in input dataset, using __fcd_load_id__ instead and overwriting any existing values"
        )
        load_id_column = "__" + load_id_column + "__"

    # add truncated sha1 hash as synthetic primary key
    # default to truncating at 8 characters but check for conflicts and bump up length if required
    # Note that this adds some complexity when joining datasets - check that lengths of hashed load id
    # match, and if they do not, compare on the shorter length - anything that doesn't match is therefore a new record anyway
    # (probably simpler to just use a 10-15 char and presume no collisions occur?)
    hash_len = 8
    hash_len = 8
    uniq = True
    while uniq is False:
        hashed = df[hashcols].apply(
            lambda x: hashlib.sha1(
                "|".join(x.astype(str).fillna("NULL").values).encode("utf-8")
            ).hexdigest()[hash_len],
            axis=1,
        )
        if len(hashed.unique()) > len(hashed):
            uniq = False
    df[load_id_column] = hashed
    return df
