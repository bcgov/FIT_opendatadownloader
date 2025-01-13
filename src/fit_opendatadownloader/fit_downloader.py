import glob
import json
import logging
import os
import re
import shutil
import tempfile
import zipfile
from pathlib import Path
from urllib.parse import urlparse

import bcdata
import boto3
import click
import fit_changedetector as fcd
import geopandas
import jsonschema
from botocore.exceptions import ClientError
from cligj import quiet_opt, verbose_opt
from esridump.dumper import EsriDumper
from geopandas import GeoDataFrame
from pyproj import CRS
from shapely.geometry.linestring import LineString
from shapely.geometry.multilinestring import MultiLineString
from shapely.geometry.multipoint import MultiPoint
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.point import Point
from shapely.geometry.polygon import Polygon

LOG = logging.getLogger(__name__)

SUPPORTED_SPATIAL_TYPES = [
    "POINT",
    "LINESTRING",
    "POLYGON",
    "MULTIPOINT",
    "MULTILINESTRING",
    "MULTIPOLYGON",
]


def configure_logging(verbosity):
    log_level = max(10, 30 - 10 * verbosity)
    logging.basicConfig(
        handlers=[logging.FileHandler("fit_downloader.log"), logging.StreamHandler()],
        level=log_level,
        format="%(asctime)s:%(levelname)s:%(name)s: %(message)s",
    )
    # squelch pyogrio INFO logs
    logging.getLogger("pyogrio._io").setLevel(logging.WARNING)


def zip_gdb(gdb_path, zip_path):
    """Compress the contents of a .gdb folder into a zip file."""
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(gdb_path):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(file_path, gdb_path)
                zipf.write(file_path, relative_path)


def gdf_standardize_spatial_types(df):
    """
    Ensure geodataframe geometry is:
    - of supported type
    - set to mulitpart if any multipart features are found
    """
    # inspect spatial types
    types = set([t.upper() for t in df.geometry.geom_type.unique()])
    unsupported = types.difference(SUPPORTED_SPATIAL_TYPES)
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


class Layer:
    def __init__(self, layer_keys, load_id="fdl_load_id", out_path="."):
        # initialize object with empty values for all properties present in schema
        with open("source_schema.json", "r") as f:
            schema = json.load(f)
        for key in schema["items"]["properties"]:
            setattr(self, key, None)
        # overwrite empty attributes with values from config keys
        if layer_keys is not None:
            for key, value in layer_keys.items():
                setattr(self, key, value)

        # note name of field to use for hashed id
        self.load_id = load_id

        # output file name
        self.out_file = os.path.join(out_path, self.out_layer + ".gdb.zip")

        # are we working with files on s3?
        if self.out_file.startswith("s3://"):
            self.s3 = boto3.client("s3")
            self.s3_key = urlparse(self.out_file, allow_fragments=False).path.lstrip("/")
            self.s3_changes_key = self.s3_key.replace(
                self.out_layer + ".gdb.zip", self.out_layer + "_changes.gdb.zip"
            )
            # self.s3_log_key =
        else:
            self.s3 = None

        self.gdf = None
        self.duplicates = []
        self.duplicate_report = {}
        self.change_report = {}
        self.tempdir = tempfile.mkdtemp()
        if not self.hash_fields:
            self.hash_fields = []

    @property
    def out_file_exists(self):
        """Return true if output file exists, supporting both local files and s3 keys"""
        # if working with s3 path, use s3 methods
        if self.s3:
            try:
                # Extract bucket name and object key from the path, check if key exists
                s3_url = self.out_file[5:]  # Strip 's3://'
                bucket_name, key = s3_url.split("/", 1)
                self.s3.head_object(Bucket=bucket_name, Key=key)
                return True
            except ClientError as e:
                # when head_object returns 404 (Not Found), file doesn't exist
                if e.response["Error"]["Code"] == "404":
                    LOG.debug(f"File does not exist: {e}")
                    return False
                else:
                    LOG.error(f"Error checking object: {e}")
                    return False
        # otherwise, use os methods
        else:
            return os.path.isfile(self.out_file)

    def download(self):
        """Download source to GeoDataFrame and do some basic validation"""

        # download data from esri rest api endpoint
        if self.protocol == "esri":
            df = GeoDataFrame.from_features(
                features=(EsriDumper(self.source, fields=self.fields, parent_logger=LOG)),
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

        self.gdf = df

    def clean(
        self,
        precision=0.01,
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
        df = self.gdf

        # reproject to BC Albers if necessary
        if df.crs != CRS.from_user_input(3005):
            df = df.to_crs("EPSG:3005")

        # standardize column naming
        if df.geometry.name != "geometry":
            df = df.rename_geometry("geometry")

        cleaned_column_map = {}
        for column in self.fields + self.hash_fields:
            cleaned_column_map[column] = re.sub(
                r"\W+", "", column.lower().strip().replace(" ", "_")
            )
        df = df.rename(columns=cleaned_column_map)

        # assign cleaned column names to fields list
        fields = list(cleaned_column_map.values())
        hash_fields = [cleaned_column_map[k] for k in self.hash_fields]

        # drop any columns not listed in config (minus geometry)
        df = df[fields + ["geometry"]]

        # check and fix spatial types (working with original geometries)
        df = gdf_standardize_spatial_types(df)

        # Validate primary keys, they must be unique
        pks = []
        duplicates = []
        if self.primary_key:
            # swap provided pk names to cleaned column names
            pks = [cleaned_column_map[k] for k in self.primary_key]
            # fail if pk values are not unique
            if len(df) != len(df[pks].drop_duplicates()):
                pk_string = ",".join(pks)
                raise ValueError(f"Duplicate values exist for primary_key {pk_string}")

            # Just to keep things as simple as possible, always create a hashed key
            # based on supplied primary key. This way we can always use the same column
            # as pk when running the change detection.
            LOG.info(
                f"Adding hashed key {self.load_id}, based on hash of provided primary_key {','.join(pks)}"
            )
            df = fcd.add_hash_key(df, new_field=self.load_id, fields=pks, hash_geometry=False)
            pks = [self.load_id]

        # if no primary key provided, use the geometry (and additional hash fields if provided)
        else:
            LOG.info(f"Adding hashed key {self.load_id}, based on hash of geometry")
            df = fcd.add_hash_key(
                df,
                new_field=self.load_id,
                fields=hash_fields,
                hash_geometry=True,
                precision=precision,
                allow_duplicates=True,
            )
            pks = [self.load_id]

            # if duplicates are present in the hash key:
            #  - drop the duplicates
            #  - note the duplicate data in a separate data structure
            if len(df) != len(df[self.load_id].drop_duplicates()) and drop_geom_duplicates:
                dup_fields = "/".join(["geometry"] + hash_fields)
                LOG.info(f"Duplicate {dup_fields} found when hashing, dropping duplicate rows")
                df["_duplicated_"] = df.duplicated(keep=False, subset=[self.load_id])
                duplicates = (
                    df[df["_duplicated_"]][[self.load_id] + fields]
                    .sort_values(by=[self.load_id])
                    .to_dict("records")
                )
                df = df.drop_duplicates(subset=[self.load_id])

        # to ensure type consistency, round trip to gdb and back to geopandas
        df.to_file(
            os.path.join(self.tempdir, "_roundtrip_.gdb"),
            driver="OpenFileGDB",
            layer=self.out_layer,
            mode="w",
        )
        df = geopandas.read_file(
            os.path.join(self.tempdir, "_roundtrip_.gdb"), layer=self.out_layer
        )

        self.gdf = df
        if duplicates:
            LOG.info("DUPLICATES: \n" + json.dumps(duplicates, indent=2))
        # populate report duplicate keys
        if duplicates:
            self.duplicate_report["n_duplicates"] = len(duplicates)
            self.duplicate_report["duplicate_ids"] = ",".join([k[self.load_id] for k in duplicates])
            self.duplicates = duplicates

    def dump(self):
        # write uncompressed .gdb in /tmp
        self.gdf.to_file(
            os.path.join(self.tempdir, self.out_layer + ".gdb"),
            driver="OpenFileGDB",
            layer=self.out_layer,
        )
        # compress the .gdb, still in /tmp
        zip_gdb(
            os.path.join(self.tempdir, self.out_layer + ".gdb"),
            os.path.join(self.tempdir, self.out_layer + ".gdb.zip"),
        )

        diff = {}

        # if output file *does not* exist, write it without running change detection
        if not self.out_file_exists:
            LOG.info("No existing file found, writing to file")
            if self.s3:
                LOG.info(f"{self.s3_key} - writing to object storage")
                self.s3.upload_file(
                    os.path.join(self.tempdir, self.out_layer + ".gdb.zip"),
                    os.environ.get("BUCKET"),
                    self.s3_key,
                )
            else:
                shutil.copyfile(
                    os.path.join(self.tempdir, self.out_layer + ".gdb.zip"), self.out_file
                )

        # run change detection if output file already exists
        else:
            LOG.info("Running change detection")
            gdf_previous = geopandas.read_file(self.out_file)
            diff = fcd.gdf_diff(
                gdf_previous,
                self.gdf,
                primary_key=self.load_id,
                suffix_a="original",
                suffix_b="new",
                return_type="gdf",
            )

            # do not write new data if nothing has changed
            if diff and len(diff["UNCHANGED"]) == len(gdf_previous) == len(self.gdf):
                LOG.info("Data unchanged")

            # if changes are present
            elif diff and (
                len(diff["UNCHANGED"]) != len(self.gdf)
                or len(diff["UNCHANGED"]) != len(gdf_previous)
            ):
                # populate report change keys
                self.change_report["record_count_original"] = len(gdf_previous)
                self.change_report["record_count_new"] = len(self.gdf)
                self.change_report["record_count_difference"] = len(self.gdf) - len(gdf_previous)
                self.change_report["record_count_difference_pct"] = round(
                    ((len(gdf_previous) - len(self.gdf)) / len(self.gdf)) * 100, 2
                )
                self.change_report["n_unchanged"] = len(diff["UNCHANGED"])
                self.change_report["n_deletions"] = len(diff["DELETED"])
                self.change_report["n_additions"] = len(diff["NEW"])
                self.change_report["n_modified"] = (
                    len(diff["MODIFIED_BOTH"])
                    + len(diff["MODIFIED_ATTR"])
                    + len(diff["MODIFIED_GEOM"])
                )
                self.change_report["n_modified_spatial_only"] = len(diff["MODIFIED_GEOM"])
                self.change_report["n_modified_spatial_attributes"] = len(diff["MODIFIED_BOTH"])
                self.change_report["n_modified_attributes_only"] = len(diff["MODIFIED_ATTR"])

                LOG.info("CHANGES: \n" + json.dumps(self.change_report, indent=2))

                # write changes gdb
                changes_gdb = self.out_layer + "_changes.gdb"
                # ensure tempfile does not already exist
                mode = "w"
                for key in [
                    "NEW",
                    "DELETED",
                    "MODIFIED_BOTH",
                    "MODIFIED_ATTR",
                    "MODIFIED_GEOM",
                ]:
                    if len(diff[key]) > 0:
                        # create empty geodataframe if geometry is not present
                        if "geometry" not in diff[key].columns:
                            diff[key] = geopandas.GeoDataFrame(
                                diff[key], geometry=geopandas.GeoSeries([None] * len(diff[key]))
                            )
                        diff[key].to_file(
                            os.path.join(self.tempdir, changes_gdb),
                            driver="OpenFileGDB",
                            layer=key,
                            mode=mode,
                        )
                        mode = "a"
                mode = "w"
                zip_gdb(
                    os.path.join(self.tempdir, changes_gdb),
                    os.path.join(self.tempdir, changes_gdb + ".zip"),
                )
                if self.s3:
                    LOG.info(f"{self.s3_changes_key}: writing to object storage")
                    self.s3.upload_file(
                        os.path.join(self.tempdir, changes_gdb + ".zip"),
                        os.environ.get("BUCKET"),
                        self.s3_changes_key,
                    )
                else:
                    shutil.copyfile(
                        os.path.join(self.tempdir, changes_gdb + ".zip"),
                        self.out_file.replace(self.out_layer + ".gdb.zip", changes_gdb + ".zip"),
                    )


def parse_config(config, out_path=".", load_id="fdl_load_id"):
    """Parse and validate layer configuration json, adding out_file and load_id to layer definition"""
    # validate sources against schema doc
    with open("source_schema.json", "r") as f:
        schema = json.load(f)
    jsonschema.validate(instance=config, schema=schema)

    # if no errors are raised by jsonschema, config is valid
    LOG.info("Config json is valid")

    # turn each source from config into a "Layer"
    # a Layer has methods download/clean/dump and properites load_id/out_path plus config keys
    layers = [Layer(source, out_path=out_path, load_id=load_id) for source in config]

    # validate primary key(s) and hash key(s) are present in fields
    for layer in layers:
        if layer.primary_key:
            if not set(layer.primary_key).issubset(set(layer.fields)):
                raise ValueError("Specified primary key(s) must be included in fields tag")
        if layer.hash_fields:
            if not set(layer.hash_fields).issubset(set(layer.fields)):
                raise ValueError("Specified hash field(s) must be included in fields tag")
    return layers


@click.group()
def cli():
    pass


@cli.command()
@click.option("--path", "-p", default="sources", type=click.Path(exists=True))
@click.option(
    "--schedule",
    "-s",
    type=click.Choice(["D", "W", "M", "Q", "A"], case_sensitive=False),
    help="Process only sources with given schedule tag.",
)
@verbose_opt
@quiet_opt
def list_configs(path, schedule, verbose, quiet):
    """List all configs available in specified folder as RD/MUNI"""
    configure_logging((verbose - quiet))
    # note that folders prefixed with _ are ignored
    files = glob.glob(os.path.join(path, "[!_]**/*.json"), recursive=True)
    for config_file in files:
        # parse schedule if specified
        if schedule:
            with open(config_file, "r") as f:
                config = json.load(f)
            sources = [s for s in config if s["schedule"] == schedule]
            if len(sources) > 0:
                click.echo(os.path.splitext(Path(config_file).relative_to("sources"))[0])
        # otherwise just dump all file names
        else:
            click.echo(os.path.splitext(Path(config_file).relative_to("sources"))[0])


@cli.command()
@click.argument("config_file", type=click.Path(exists=True))
@click.argument("admin_prefix")
@click.option("load_id", "-k", default="fdl_load_id", help="Name of column to hold hashed id/key")
@click.option(
    "--layer",
    "-l",
    help="Layer to process in provided config.",
)
@click.option(
    "--schedule",
    "-s",
    type=click.Choice(["D", "W", "M", "Q", "A"], case_sensitive=False),
    help="Process only sources with given schedule tag.",
)
@click.option(
    "--validate",
    "-V",
    is_flag=True,
    help="Validate configuration",
)
@verbose_opt
@quiet_opt
def process(
    config_file,
    admin_prefix,
    load_id,
    layer,
    schedule,
    validate,
    verbose,
    quiet,
):
    """Download data defined in config, write to file if changed"""
    configure_logging((verbose - quiet))

    # define output prefix
    s3_prefix = "Change_Detection/" + admin_prefix

    with open(config_file, "r") as f:
        config = json.load(f)
    issues = []
    layers = parse_config(
        config, load_id=load_id, out_path=os.path.join("s3://", os.environ.get("BUCKET"), s3_prefix)
    )

    # if specified, process only specified layer
    if layer:
        layers = [s for s in layers if s.out_layer == layer]
        if len(layers) == 0:
            LOG.warning(f"No layer named {layer} found in {config_file}")

    # if specified, use only layers with given schedule tag
    if schedule:
        layers = [s for s in layers if s.schedule == schedule]
        # alert if no layers in config match this schedule
        if len(layers) == 0:
            LOG.warning(f"No source with schedule={schedule} found in {config_file}")

    for layer in layers:
        report = {}
        layer.download()
        layer.clean()
        if not validate:
            layer.dump()
            report.update(layer.duplicate_report)
            report.update(layer.change_report)

            # dump duplicates/changes report as text for creating a gh issue
            # note that issues are only created if changes are present, not for fresh uploads
            if layer.change_report:
                issues.append(
                    {
                        "title": "Data changes: " + os.path.join(admin_prefix, layer.out_layer),
                        "body": "<br />".join([k + ": " + str(report[k]) for k in report]),
                    }
                )

    with open("issues.json", "w") as f:
        json.dump(issues, f, indent=2)


if __name__ == "__main__":
    cli()
