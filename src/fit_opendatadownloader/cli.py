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

import fit_opendatadownloader as fdl


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
@click.version_option(version=fdl.__version__, message="%(version)s")
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
    layers = fdl.parse_config(config_file)

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
        df = fdl.clean(df, layer.fields, layer.primary_key, precision=0.1)

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


if __name__ == "__main__":
    cli()
