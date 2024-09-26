import hashlib
import logging
import os
import re

import bcdata
from esridump.dumper import EsriDumper
import geopandas
from geopandas import GeoDataFrame

from pyproj import CRS
from shapely.geometry.linestring import LineString
from shapely.geometry.multilinestring import MultiLineString
from shapely.geometry.multipoint import MultiPoint
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.point import Point
from shapely.geometry.polygon import Polygon

import fit_opendatadownloader as fdl
import fit_changedetector as fcd

LOG = logging.getLogger(__name__)


class SourceLayer:
    def __init__(self, layer_keys):
        # attributes for a layer are taken directly from the config keys
        if layer_keys is not None:
            for key, value in layer_keys.items():
                setattr(self, key, value)

    def download(self):
        """Download source to GeoDataFrame self.df and do some basic validation"""

        # download data from esri rest api endpoint
        if self.protocol == "esri":
            df = GeoDataFrame.from_features(
                features=(
                    EsriDumper(self.source, fields=self.fields, parent_logger=LOG)
                ),
                crs=4326,
            )

        # download from BC WFS
        elif self.protocol == "bcgw":
            df = bcdata.get_data(self.source, query=self.query, as_gdf=True)

        # download data from location readable by ogr
        elif self.protocol == "http":
            df = geopandas.read_file(
                os.path.expandvars(self.source),
                layer=self.source_layer,
                where=self.query,
            )

        # are expected columns present?
        for column in self.fields:
            if column and column.lower() not in [x.lower() for x in df.columns]:
                raise ValueError(
                    f"Download error: {self.out_layer} - column {column} is not present, modify 'fields'"
                )

        # is there data?
        count = len(df.index)
        if count == 0:
            raise ValueError(
                f"Download error: {self.out_layer} - no data returned, check source and query"
            )

        # is a crs defined?
        if not df.crs:
            raise ValueError(
                f"Download error: {self.out_layer} does not have a defined projection/coordinate reference system"
            )

        # presume layer is defined correctly if no errors are raised
        LOG.info(f"Download successful: {self.out_layer} - record count: {str(count)}")

        return df


def clean(
    df,
    fields,
    primary_key=None,
    precision=0.01,
    fdl_primary_key="fdl_load_id",
    drop_geom_duplicates=False,
):
    """
    Standardize a geodataframe, confirming:

    - geometries are BC Albers
    - geometries are of supported spatial type
    - if multipart geometries are present, promote *all* geometries to multipart
    - clean field names (lowercase, no special characters or spaces)
    - remove any fields not included in config fields key
    - if primary key is provided, validate to ensure it is unique
    - if primary key is not provided:
    -    - create hash of the geometry to use as primary key (defaulting to 1cm coordinate precision)
    -    - optionally, drop duplicate records (based on geometry hash key)
    """
    # reproject to BC Albers if necessary
    if df.crs != CRS.from_user_input(3005):
        df = df.to_crs("EPSG:3005")

    # standardize column naming
    if df.geometry.name != "geometry":
        df = df.rename_geometry("geometry")
    cleaned_column_map = {}
    for column in fields:
        cleaned_column_map[column] = re.sub(
            r"\W+", "", column.lower().strip().replace(" ", "_")
        )
    df = df.rename(columns=cleaned_column_map)

    # assign cleaned column names to fields list
    fields = list(cleaned_column_map.values())

    # drop any columns not listed in config (minus geometry)
    df = df[fields + ["geometry"]]

    # check and fix spatial types (working with original geometries)
    df = standardize_spatial_types(df)

    # Validate primary keys, they must be unique
    pks = []
    if primary_key:
        # swap provided pk names to cleaned column names
        pks = [cleaned_column_map[k] for k in primary_key]
        # fail if pk values are not unique
        if len(df) != len(df[pks].drop_duplicates()):
            pk_string = ",".join(pks)
            raise ValueError(f"Duplicate values exist for primary_key {pk_string}")

        # Just to keep things as simple as possible, always create a hashed key
        # based on supplied primary key. This way we can always use the same column
        # as pk when running the change detection.
        LOG.info(
            f"Adding hashed key {fdl_primary_key}, based on hash of provided primary_key {','.join(pks)}"
        )
        df = fcd.add_hash_key(df, fdl_primary_key, fields=pks, hash_geometry=False)
        pks = [fdl_primary_key]

    # if no primary key provided, just use the geometry
    else:
        LOG.info(
            f"Adding hashed key {fdl_primary_key}, based on hash of geometry {','.join(pks)}"
        )
        df = fcd.add_hash_key(
            df, fdl_primary_key, hash_geometry=True, precision=precision
        )
        pks = [fdl_primary_key]

    # duplicates could be present if using geometry hash as pk
    # report on duplicates and drop (if specified)
    n_duplicates = len(df.drop_duplicates(subset=pks))
    if n_duplicates > 0:
        LOG.warning(f"{n_duplicates} duplicates are present in data")
        if drop_geom_duplicates:
            df = df.drop_duplicates(subset=pks)
            LOG.warning(
                f"Dropped {n_duplicates} duplicate rows (equivalent geometries)"
            )

    return df


def standardize_spatial_types(df):
    """
    Ensure geodataframe geometry is:
    - of supported type
    - set to mulitpart if any multipart features are found
    """
    # inspect spatial types
    types = set([t.upper() for t in df.geometry.geom_type.unique()])
    unsupported = types.difference(fdl.supported_spatial_types)
    if unsupported:
        raise ValueError(f"Geometries of type {unsupported} are not supported")
        # fail for now but maybe better would be to warn and remove all rows having this type?
        # df = df[[df["geometry"].geom_type != t]]

    # promote geometries to multipart if any multipart features are found
    if set(types).intersection(set(("MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON"))):
        LOG.info("Promoting all features to multipart")
        df.geometry = [
            MultiPoint([feature]) if isinstance(feature, Point) else feature
            for feature in df.geometry
        ]
        df.geometry = [
            MultiLineString([feature]) if isinstance(feature, LineString) else feature
            for feature in df.geometry
        ]
        df.geometry = [
            MultiPolygon([feature]) if isinstance(feature, Polygon) else feature
            for feature in df.geometry
        ]
    return df
