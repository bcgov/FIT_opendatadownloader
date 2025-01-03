import glob
import json
import logging
import os
import shutil
import sys
import zipfile
from pathlib import Path
from urllib.parse import urlparse

import boto3
import click
import fit_changedetector as fcd
import geopandas
from botocore.exceptions import ClientError
from cligj import quiet_opt, verbose_opt

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


def s3_key_exists(s3_client, s3_key):
    """Return True if s3 key exists, False if it does not"""
    try:
        s3_client.head_object(Bucket=os.environ.get("BUCKET"), Key=s3_key)
        return True  # If no exception, the object exists
    except ClientError as e:
        # If the error code is '404', the object doesn't exist
        if e.response["Error"]["Code"] == "NoSuchKey":
            return False
        else:
            LOG.error(f"Error checking object: {e}")
            return False


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
    """List all configs available in specified folder as RD/MUNI"""
    configure_logging((verbose - quiet))
    files = glob.glob(os.path.join(path, "**/*.json"), recursive=True)
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
@click.option(
    "--layer",
    "-l",
    help="Layer to process in provided config.",
)
@click.option(
    "--prefix",
    "-p",
    help="S3 prefix.",
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
    prefix,
    schedule,
    validate,
    verbose,
    quiet,
):
    """For given config, download data and write to file if changed"""
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

        # clean the data slightly
        df = fdl.clean(
            df,
            fields=layer.fields,
            primary_key=layer.primary_key,
            fdl_primary_key="fdl_load_id",
            hash_fields=layer.hash_fields,
            precision=0.1,
        )
        # if no primary key provided, use "fdl_primary_key"
        if layer.primary_key:
            primary_key = layer.primary_key
        else:
            primary_key = "fdl_load_id"

        # process and dump to file if "validate" option is not set
        if not validate:
            # write download to zipped gdb in cwd
            out_file = layer.out_layer + ".gdb"
            df.to_file(out_file, driver="OpenFileGDB", layer=layer.out_layer)
            zip_gdb(out_file, out_file + ".zip")
            # derive output path
            s3_key = (
                urlparse(prefix, allow_fragments=False).path.lstrip("/") + "/" + out_file + ".zip"
            )
            # create s3 client
            s3_client = boto3.client("s3")

            # default to writing
            write = True

            # run change detection if out file/ s3 key already exists
            if s3_key_exists(s3_client, s3_key):
                # read from existing file on s3
                df2 = geopandas.read_file(os.path.join("s3://", os.environ.get("BUCKET"), s3_key))
                # run change detection
                diff = fcd.gdf_diff(df2, df, primary_key=primary_key, return_type="gdf")
                # do not write new data if nothing has changed
                if len(diff["UNCHANGED"]) == len(df2) == len(df):
                    LOG.info(f"Data unchanged {s3_key}")
                    write = False
                else:
                    LOG.info("Changes found")

                    # todo // write changes to log

                    # todo // alert users that new data is available

            if write:
                LOG.info(f"Writing {layer.out_layer} to {s3_key}")
                s3_client.upload_file(out_file + ".zip", os.environ.get("BUCKET"), s3_key)

            # cleanup
            shutil.rmtree(out_file)
            os.unlink(out_file + ".zip")


if __name__ == "__main__":
    cli()
