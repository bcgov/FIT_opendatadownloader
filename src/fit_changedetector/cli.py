import logging
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


@click.command()
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

    with open(config_file, "r") as f:
        config = json.load(f)

    # parse config, returning a list of dicts defining layers to process
    sources = fcd.parse_config(config)

    # if specified, download only specified layer
    if layer:
        config = [s for s in config if s["layer"] == layer]
        # alert if no sources match this schedule
        if len(sources) == 0:
            LOG.warning(f"No layer named {layer} found in {config}")

    # if specified, use only sources with given schedule tag
    if schedule:
        sources = [s for s in sources if s["schedule"] == schedule]
        # alert if no sources match this schedule
        if len(sources) == 0:
            LOG.warning(f"No source with schedule={schedule} found in {config}")

    # process all layers defined in source config
    for layer in sources:

        # download data, do some tidying/standardization
        df = fcd.download(layer)

        if not validate:

            # write to gdb in cwd
            out_file = layer["out_layer"] + ".gdb"
            df.to_file(out_file, driver="OpenFileGDB", layer=layer["out_layer"])

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
                LOG.info(f"layer {layer['out_layer']} saved to {s3_key}")
                os.unlink(out_file + ".zip")

            # alternatively, move to local path
            elif out_path != ".":
                Path(out_path).mkdir(parents=True, exist_ok=True)
                destination = os.path.join(
                    out_path,
                    out_file + ".zip",
                )
                os.rename(out_file + ".zip", destination)
                LOG.info(f"layer {layer['out_layer']} saved to {destination}")
                os.unlink(out_file + ".zip")

            # do nothing if out_path is empty
            elif out_path == ".":
                LOG.info(f"layer {layer['out_layer']} saved to {out_file}.zip")

            # cleanup
            shutil.rmtree(out_file)


if __name__ == '__main__':
    process()
