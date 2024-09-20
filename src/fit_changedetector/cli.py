import logging
import glob
import json
import os
from pathlib import Path
import re
import sys
import shutil
from urllib.parse import urlparse
import zipfile

import boto3
import click
from cligj import verbose_opt, quiet_opt
import geopandas

import fit_changedetector as fcd


LOG = logging.getLogger(__name__)


def configure_logging(verbosity):
    log_level = max(10, 30 - 10 * verbosity)
    logging.basicConfig(
        stream=sys.stderr,
        level=log_level,
        format="%(asctime)s:%(levelname)s:%(name)s: %(message)s",
    )


def zip_gdb(folder_path, zip_path):
    """
    Compress the contents of an entire folder into a zip file.

    :param folder_path: Path to the folder to be zipped.
    :param zip_path: Path to the resulting zip file.
    """
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        # Walk through the directory
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                # Create the full file path
                file_path = os.path.join(root, file)
                # Create a relative path for the file in the zip
                relative_path = os.path.relpath(file_path, folder_path)
                # Add file to the zip file
                zipf.write(file_path, relative_path)


@click.group()
@click.version_option(version=fcd.__version__, message="%(version)s")
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
    """List RD/muni component of all config files present in specified folder"""
    configure_logging((verbose - quiet))
    files = glob.glob(os.path.join(path, "**/*.json"), recursive=True)
    for config_file in files:
        # parse schedule if specified
        if schedule:
            with open(config_file, "r") as f:
                config = json.load(f)
            sources = [s for s in config if s["schedule"] == schedule]
            if len(sources) > 0:
                click.echo(
                    os.path.splitext(Path(config_file).relative_to("sources"))[0]
                )
        # otherwise just dump all file names
        else:
            click.echo(os.path.splitext(Path(config_file).relative_to("sources"))[0])


@cli.command()
@click.argument("config_file", type=click.Path(exists=True))
@click.option(
    "--layer",
    "-l",
    help="Layer to process in provided config.",
)
@click.option(
    "--out-path",
    "-o",
    type=click.Path(),
    default=".",
    help="Output path or s3 prefix.",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Force download to out-path without running change detection.",
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
    layer,
    out_path,
    force,
    schedule,
    validate,
    verbose,
    quiet,
):
    """For each configured layer - download latest, detect changes, write to file"""
    configure_logging((verbose - quiet))

    # parse config, returning a list of dicts defining layers to process
    layers = fcd.parse_config(config_file)

    # if specified, download only specified layer
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

    # process all layers defined in source config
    for layer in layers:
        # download data from source to a geodataframe (df)
        df = layer.download()

        # clean the data and warn if duplicates are found
        # Duplicates processing for geometries is based on provided precision value
        # This fails if primary key + geometry is non-unique
        df = fcd.clean(df, layer.fields, layer.primary_key, precision=0.1)

        # process and dump to file if "validate" option is not set
        if not validate:
            # write to gdb in cwd
            out_file = layer.out_layer + ".gdb"
            df.to_file(out_file, driver="OpenFileGDB", layer=layer.out_layer)

            # run change detection unless otherwise specified
            # if not force:
            # - get previous version (if present)
            # - compare to previous version
            # - if changes detected, modify output path to include <fcd_YYYYMMDD> prefix,
            # - write diffs / reports

            # then write data

            # zip and write to target location
            zip_gdb(out_file, out_file + ".zip")

            # copy to s3 if out_path prefix is s3://
            if bool(re.compile(r"^s3://").match(out_path)):
                s3_key = urlparse(out_path, allow_fragments=False).path.lstrip("/")
                s3_client = boto3.client("s3")
                s3_client.upload_file(
                    out_file + ".zip", os.environ.get("BUCKET"), s3_key
                )
                LOG.info(f"layer {layer.out_layer} saved to {s3_key}")
                os.unlink(out_file + ".zip")

            # alternatively, move to local path
            elif out_path != ".":
                Path(out_path).mkdir(parents=True, exist_ok=True)
                destination = os.path.join(
                    out_path,
                    out_file + ".zip",
                )
                os.rename(out_file + ".zip", destination)
                LOG.info(f"layer {layer.out_layer} saved to {destination}")

            # do nothing if out_path is empty
            elif out_path == ".":
                LOG.info(f"layer {layer.out_layer} saved to {out_file}.zip")

            # cleanup
            shutil.rmtree(out_file)


@cli.command()
@click.argument("in_file_a", type=click.Path(exists=True))
@click.argument("in_file_b", type=click.Path(exists=True))
@click.option("--layer_a")
@click.option("--layer_b")
@click.option(
    "--fields",
    "-f",
    help="Comma separated list of fields to compare (do not include primary key)",
)
@click.option(
    "--out-path",
    "-o",
    type=click.Path(),
    default=".",
    help="Output path",
)
@click.option(
    "--primary-key",
    "-k",
    multiple=True,
    help="Primary key column(s), common to both datasets",
)
@click.option(
    "--precision",
    "-p",
    default=0.001,
    help="Precision to use when comparing geometries",
)
@click.option(
    "--suffix_a",
    "-a",
    default="a",
    help="Suffix to append to column names from data source A when comparing attributes",
)
@click.option(
    "--suffix_b",
    "-b",
    default="b",
    help="Suffix to append to column names from data source B when comparing attributes",
)
@verbose_opt
@quiet_opt
def compare(
    in_file_a,
    in_file_b,
    layer_a,
    layer_b,
    primary_key,
    fields,
    out_path,
    precision,
    suffix_a,
    suffix_b,
    verbose,
    quiet,
):
    """Compare two datasets"""
    configure_logging((verbose - quiet))

    # load source data
    df_a = geopandas.read_file(in_file_a, layer=layer_a)
    df_b = geopandas.read_file(in_file_b, layer=layer_b)

    # is pk present in both sources?
    if primary_key:
        primary_key = list(primary_key)
        if not bool(set(primary_key) & set(df_a.columns)):
            raise ValueError(
                f"Primary key {','.join(primary_key)} not present in {in_file_a}"
            )
        if not bool(set(primary_key) & set(df_b.columns)):
            raise ValueError(
                f"Primary key {','.join(primary_key)} not present in {in_file_b}"
            )

        # is pk unique in both sources? If not, append geom to pk
        if (len(df_a) != len(df_a[primary_key].drop_duplicates())) or (
            len(df_b) != len(df_b[primary_key].drop_duplicates())
        ):
            LOG.warning(
                f"Duplicate values exist for primary_key {primary_key}, appending geometry"
            )
            primary_key = primary_key + ["geometry_p"]
    else:
        primary_key = ["geometry_p"]

    # add slightly generalized geometry to primary key columns
    if "geometry_p" in primary_key:
        df_a["geometry_p"] = (
            df_a[df_a.geometry.name]
            .normalize()
            .set_precision(precision, mode="pointwise")
        )
        df_b["geometry_p"] = (
            df_b[df_b.geometry.name]
            .normalize()
            .set_precision(precision, mode="pointwise")
        )

    # generate new synthentic pk
    if "geometry_p" in primary_key or len(primary_key) > 1:
        LOG.info("Adding synthetic primary key fcd_id to both sources")
        df_a = fcd.add_synthetic_primary_key(df_a, primary_key, new_column="fcd_id")
        df_b = fcd.add_synthetic_primary_key(df_b, primary_key, new_column="fcd_id")
        primary_key = "fcd_id"
        # remove the temp geom
        df_a = df_a.drop(columns=["geometry_p"])
        df_b = df_b.drop(columns=["geometry_p"])
        dump_inputs_with_new_pk = True

    # otherwise, pick the pk from first (and only) item in the pk list
    else:
        primary_key = primary_key[0]
        dump_inputs_with_new_pk = False

    # if string of fields is provided, parse into list
    if fields:
        fields = fields.split(",")

    # run the diff
    diff = fcd.gdf_diff(
        df_a,
        df_b,
        primary_key,
        fields=fields,
        precision=precision,
        suffix_a=suffix_a,
        suffix_b=suffix_b,
    )

    # write output data
    mode = "w"  # for writing the first non-empty layer, subsequent writes are appends
    out_gdb = os.path.join(out_path, "changedetector.gdb")
    if os.path.exists(out_gdb):
        LOG.warning(f"changedetector.gdb exists in {out_path}, overwriting")
        shutil.rmtree(out_gdb)

    for key in [
        "NEW",
        "DELETED",
        "MODIFIED_BOTH",
        "MODIFIED_ATTR",
        "MODIFIED_GEOM",
        "MODIFIED_ALL",
    ]:
        if len(diff[key]) > 0:
            LOG.info(f"writing {key} to {out_gdb}")
            diff[key].to_file(out_gdb, driver="OpenFileGDB", layer=key, mode=mode)
            mode = "a"

    # re-write source datasets if new pk generated (and some kind of output generated)
    if dump_inputs_with_new_pk and mode == "a":
        df_a.to_file(
            out_gdb, driver="OpenFileGDB", layer="source_" + suffix_a, mode="a"
        )
        df_b.to_file(
            out_gdb, driver="OpenFileGDB", layer="source_" + suffix_b, mode="a"
        )


if __name__ == "__main__":
    process()
