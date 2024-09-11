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


def clean(df, fields, primary_key, precision=0.01, fcd_primary_key="fcd_load_id"):
    """
    Standardize a geodataframe, confirming:

    - geometries are BC Albers
    - geometries are of supported spatial type
    - if multipart geometries are present, ensure *all* geometries are multipart
    - clean field names (lowercase, no special characters or spaces)
    - remove any fields not included in config fields key
    - drop duplicates (considering retained fields and geometries, with reduced precision if specified)
    - if primary key is provided, validate it is unique and add geometry to pk if it is not
    - if primary key is not provided, default to using the geometry as pk (detecting changes to attributes)
    - hash the primary key into a new column called fc_load_id, to use as simple key for generating diffs
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

    # normalize the geometries
    # todo - should this also call make_valid()?
    df["geometry"] = df["geometry"].normalize()

    # then add a reduced precision geometry column based on the normalized geoms
    df["geometry_p"] = df["geometry"].set_precision(precision, mode="pointwise")

    # drop duplicates based on all retained fields and this geometry
    if len(df) != len(df.drop_duplicates(subset=fields + ["geometry_p"])):
        n_dropped = len(df) - len(df.drop_duplicates(subset=fields + ["geometry_p"]))
        df = df.drop_duplicates(subset=fields + ["geometry_p"])
        LOG.warning(
            f"Dropped {n_dropped} duplicate rows (equivalent attributes and geometries)"
        )

    # check and fix spatial types (working with original geometries)
    df = standardize_spatial_types(df)

    # process primary keys, adding geometry if they are not unique
    pks = None
    if primary_key:
        # swap provided pk names to cleaned column names
        pks = [cleaned_column_map[k] for k in primary_key]
        # if pk values are not unique, add the reduced precision geometry to the pk
        if len(df) != len(df[pks].drop_duplicates()):
            pk_string = ",".join(pks)
            LOG.warning(
                f"Duplicate values exist for primary_key {pk_string}, adding geometries to primary key"
            )
            pks = pks + ["geometry_p"]

    # if no pk provided, default to using the reduced precision geometry
    else:
        pks = ["geometry_p"]

    # Even after adding geometries to the primary key, duplicates could still
    # exist (duplicates for *all* columns plus geometry are removed above)
    # Check for this and fail if duplicate primary keys are still present
    if len(df) != len(df[pks].drop_duplicates()):
        raise ValueError(
            f"Duplicate values for primary keys {','.join(pks)} exist - set config primary_key to a column with unique values or remove"
        )

    # Fail if output hashed id column is already present in data
    if fcd_primary_key in df.columns:
        raise ValueError(
            f"column {fcd_primary_key} is present in input dataset, use some other column name"
        )

    # add sha1 hash of primary keys
    df[fcd_primary_key] = df[pks].apply(
        lambda x: hashlib.sha1(
            "|".join(x.astype(str).fillna("NULL").values).encode("utf-8")
        ).hexdigest(),
        axis=1,
    )

    # drop the reduced precision geometry column
    df = df[[fcd_primary_key] + fields + ["geometry"]]
    return df


def standardize_spatial_types(df):
    """
    Ensure geodataframe geometry is:
    - of supported type
    - set to mulitpart if any multipart features are found
    """
    # inspect spatial types
    types = set([t.upper() for t in df.geometry.geom_type.unique()])
    unsupported = types.difference(fcd.supported_spatial_types)
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