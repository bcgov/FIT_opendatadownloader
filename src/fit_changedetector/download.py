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
from shapely.geometry.linestring import LineString
from shapely.geometry.multilinestring import MultiLineString
from shapely.geometry.multipoint import MultiPoint
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.point import Point
from shapely.geometry.polygon import Polygon


LOG = logging.getLogger(__name__)


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

    return parsed


def standardize_spatial_types(df):
    """
    introspect spatial types
    - fail if multiple dimensions are found (ie point and poly)
    - promote to multipart if any multipart feature is found
    (drivers like .gdb do not support mixed-types)
    """
    types = set([t.upper() for t in df.geometry.geom_type.unique()])
    # geopandas does not seem to have a st_dimension function,
    # inspect the types with string comparison
    valid_types = set(
        [
            "POINT",
            "LINESTRING",
            "POLYGON",
            "MULTIPOINT",
            "MULTILINESTRING",
            "MULTIPOLYGON",
        ]
    )
    if types.difference(valid_types):
        raise ValueError(
            f"Geometries of type {types.difference(valid_types)} are not supported"
        )
        # fail for now but maybe better would be to warn and remove all rows having this type?
        # df = df[[df["geom"].geom_type != t]]
    # promote geometries to multipart if any multipart features are found
    if set(types).intersection(set(("MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON"))):
        LOG.info("Promoting all features to multipart")
        df["geom"] = [
            MultiPoint([feature]) if isinstance(feature, Point) else feature
            for feature in df["geom"]
        ]
        df["geom"] = [
            MultiLineString([feature]) if isinstance(feature, LineString) else feature
            for feature in df["geom"]
        ]
        df["geom"] = [
            MultiPolygon([feature]) if isinstance(feature, Polygon) else feature
            for feature in df["geom"]
        ]
    return df


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
    for column in source["fields"]:
        if column and column.lower() not in [x.lower() for x in df.columns]:
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

    # check and fix spatial types
    df = standardize_spatial_types(df)

    # if primary key(s) provided, ensure unique and sort data by key(s)
    pks = None
    if source["primary_key"]:
        # swap provided pk names to cleaned column names
        pks = [cleaned_column_map[k] for k in source["primary_key"]]
        # are values unique?
        if len(df) != len(df[pks].drop_duplicates()):
            pk_string = ",".join(pks)
            raise ValueError(
                f"Duplicate values exist for primary_key {pk_string}, consider removing primary_key from config"
            )
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

    # add sha1 hash as synthetic primary key
    hashed = df[hashcols].apply(
        lambda x: hashlib.sha1(
            "|".join(x.astype(str).fillna("NULL").values).encode("utf-8")
        ).hexdigest()[:13],
        axis=1,
    )
    df[load_id_column] = hashed
    return df
