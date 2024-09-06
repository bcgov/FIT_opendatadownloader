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


LOG = logging.getLogger(__name__)


class SourceLayer:
    def __init__(self, layer_keys):
        # attributes for a layer are taken directly from the config keys
        if layer_keys is not None:
            for key, value in layer_keys.items():
                setattr(self, key, value)
        self.supported_types = set(
            [
                "POINT",
                "LINESTRING",
                "POLYGON",
                "MULTIPOINT",
                "MULTILINESTRING",
                "MULTIPOLYGON",
            ]
        )

    def download(self):
        """Download source to GeoDataFrame self.df and do some basic validation"""

        # download data from esri rest api endpoint
        if self.protocol == "esri":
            self.df = GeoDataFrame.from_features(
                features=(
                    EsriDumper(self.source, fields=self.fields, parent_logger=LOG)
                ),
                crs=4326,
            )

        # download from BC WFS
        elif self.protocol == "bcgw":
            self.df = bcdata.get_data(self.source, query=self.query, as_gdf=True)

        # download data from location readable by ogr
        elif self.protocol == "http":
            self.df = geopandas.read_file(
                os.path.expandvars(self.source),
                layer=self.source_layer,
                where=self.query,
            )

        # are expected columns present?
        for column in self.fields:
            if column and column.lower() not in [x.lower() for x in self.df.columns]:
                raise ValueError(
                    f"Download error: {self.out_layer} - column {column} is not present, modify 'fields'"
                )

        # is there data?
        count = len(self.df.index)
        if count == 0:
            raise ValueError(
                f"Download error: {self.out_layer} - no data returned, check source and query"
            )

        # is a crs defined?
        if not self.df.crs:
            raise ValueError(
                f"Download error: {self.out_layer} does not have a defined projection/coordinate reference system"
            )

        # presume layer is defined correctly if no errors are raised
        LOG.info(f"Download successful: {self.out_layer} - record count: {str(count)}")

    def clean(self):
        """
        After downloading, standardize the data extracted from the source:

        - geometries are BC Albers
        - geometries are of supported spatial type
        - if multipart geometries are present, ensure *all* geometries are multipart
        - clean field names (lowercase, no special characters or spaces)
        - remove any fields not included in config fields key
        - if primary key is provided, validate it is unique
        - if primary is not provided, generate a synthetic pk from fields and geometry
        """
        # reproject to BC Albers if necessary
        if self.df.crs != CRS.from_user_input(3005):
            self.df = self.df.to_crs("EPSG:3005")

        # standardize column naming
        self.df = self.df.rename_geometry("geom")
        cleaned_column_map = {}
        for column in self.fields:
            cleaned_column_map[column] = re.sub(
                r"\W+", "", column.lower().strip().replace(" ", "_")
            )
        self.df = self.df.rename(columns=cleaned_column_map)
        # retain only columns noted in config and geom
        self.df = self.df[list(cleaned_column_map.values()) + ["geom"]]

        # check and fix spatial types
        self.standardize_spatial_types()

        # if primary key(s) provided, ensure unique and sort data by key(s)
        pks = None
        if self.primary_key:
            # swap provided pk names to cleaned column names
            pks = [cleaned_column_map[k] for k in self.primary_key]
            # are values unique?
            if len(self.df) != len(self.df[pks].drop_duplicates()):
                pk_string = ",".join(pks)
                raise ValueError(
                    f"Duplicate values exist for primary_key {pk_string}, consider removing primary_key from config"
                )
            self.df = self.df.sort_values(pks)

        # default to creating hash on all input fields, but if supplied use the pk(s)
        if pks:
            hashcols = pks
        else:
            hashcols = self.df.columns

        # check that output hashed id column is not already present
        if "fcd_load_id" not in self.df.columns:
            load_id_column = "fcd_load_id"
        else:
            raise Warning(
                "column fcd_load_id is present in input dataset, using __fcd_load_id__ instead and overwriting any existing values"
            )
            load_id_column = "__" + load_id_column + "__"

        # add truncated (14char) sha1 hash as synthetic primary key
        hashed = self.df[hashcols].apply(
            lambda x: hashlib.sha1(
                "|".join(x.astype(str).fillna("NULL").values).encode("utf-8")
            ).hexdigest()[:13],
            axis=1,
        )
        self.df[load_id_column] = hashed

    def standardize_spatial_types(self):
        """
        Ensure geodataframe geometry is:
        - of supported type
        - set to mulitpart if any multipart features are found
        """
        # ensure
        types = set([t.upper() for t in self.df.geometry.geom_type.unique()])
        unsupported = types.difference(self.supported_types)
        if unsupported:
            raise ValueError(f"Geometries of type {unsupported} are not supported")
            # fail for now but maybe better would be to warn and remove all rows having this type?
            # df = df[[df["geom"].geom_type != t]]

        # promote geometries to multipart if any multipart features are found
        if set(types).intersection(
            set(("MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON"))
        ):
            LOG.info("Promoting all features to multipart")
            self.df["geom"] = [
                MultiPoint([feature]) if isinstance(feature, Point) else feature
                for feature in self.df["geom"]
            ]
            self.df["geom"] = [
                MultiLineString([feature])
                if isinstance(feature, LineString)
                else feature
                for feature in self.df["geom"]
            ]
            self.df["geom"] = [
                MultiPolygon([feature]) if isinstance(feature, Polygon) else feature
                for feature in self.df["geom"]
            ]
