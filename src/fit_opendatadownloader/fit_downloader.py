import csv
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
        if e.response["Error"]["Code"] == "404":
            LOG.debug(f"File does not exist: {e}")
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
    # note that folders prefixed with _ are ignored
    # files = glob.glob(os.path.join(path, "[!_]**/*.json"), recursive=True)
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
    change_reports = []
    for layer in layers:
        # download data from source to a geodataframe (df)
        df = layer.download()

        # Clean the data slightly
        # - add synthetic primary key
        # - fail if provided primary key is not unique
        # - if no pk provided, delete duplicate geometries (or geometry + hash field)
        df, duplicates = fdl.clean(
            df,
            fields=layer.fields,
            primary_key=layer.primary_key,
            fdl_primary_key="fdl_load_id",
            hash_fields=layer.hash_fields,
            precision=0.1,
            drop_geom_duplicates=True,
        )

        # to ensure type consistency, round trip to gdb and back to geopandas
        df.to_file("temp.gdb", driver="OpenFileGDB", layer=layer.out_layer, mode="w")
        df = geopandas.read_file("temp.gdb", layer=layer.out_layer)

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

            # run change detection if out file / s3 key already exists
            if s3_key_exists(s3_client, s3_key):
                # read from existing file on s3
                df2 = geopandas.read_file(os.path.join("s3://", os.environ.get("BUCKET"), s3_key))
                # run change detection
                diff = fcd.gdf_diff(
                    df2,
                    df,
                    primary_key="fdl_load_id",
                    suffix_a="original",
                    suffix_b="new",
                    return_type="gdf",
                )
                # do not write new data if nothing has changed
                if len(diff["UNCHANGED"]) == len(df2) == len(df):
                    LOG.info(f"{s3_key}: data unchanged")
                    write = False
                else:
                    # write changeset
                    changes_file = layer.out_layer + "_changes.gdb"
                    # derive output path
                    changes_s3_key = (
                        urlparse(prefix, allow_fragments=False).path.lstrip("/")
                        + "/"
                        + changes_file
                        + ".zip"
                    )
                    LOG.info(f"{s3_key}: changes found, creating changes .gdb")
                    logging.getLogger("pyogrio._io").setLevel(
                        logging.WARNING
                    )  # squelch pyogrio INFO logs
                    mode = "w"
                    for key in [
                        "NEW",
                        "DELETED",
                        "MODIFIED_BOTH",
                        "MODIFIED_ATTR",
                        "MODIFIED_GEOM",
                    ]:
                        if len(diff[key]) > 0:
                            if "geometry" not in diff[key].columns:
                                diff[key] = geopandas.GeoDataFrame(
                                    diff[key], geometry=geopandas.GeoSeries([None] * len(diff[key]))
                                )
                            diff[key].to_file(
                                changes_file, driver="OpenFileGDB", layer=key, mode=mode
                            )
                            mode = "a"
                    zip_gdb(changes_file, changes_file + ".zip")
                    LOG.info(f"{changes_s3_key}: writing to object storage")
                    s3_client.upload_file(
                        changes_file + ".zip", os.environ.get("BUCKET"), changes_s3_key
                    )
                    shutil.rmtree(changes_file)
                    os.unlink(changes_file + ".zip")

                    # build the report
                    change_report = {}
                    change_report["record_count_original"] = len(df2)
                    change_report["record_count_new"] = len(df)
                    change_report["record_count_difference"] = len(df) - len(df2)
                    change_report["record_count_difference_pct"] = round(
                        ((len(df2) - len(df)) / len(df)) * 100, 2
                    )
                    change_report["n_unchanged"] = len(diff["UNCHANGED"])
                    change_report["n_deletions"] = len(diff["DELETED"])
                    change_report["n_additions"] = len(diff["NEW"])
                    change_report["n_modified"] = (
                        len(diff["MODIFIED_BOTH"])
                        + len(diff["MODIFIED_ATTR"])
                        + len(diff["MODIFIED_GEOM"])
                    )
                    change_report["n_modified_spatial_only"] = len(diff["MODIFIED_GEOM"])
                    change_report["n_modified_spatial_attributes"] = len(diff["MODIFIED_BOTH"])
                    change_report["n_modified_attributes_only"] = len(diff["MODIFIED_ATTR"])

                    change_report["n_duplicates"] = len(duplicates)
                    change_report["duplicate_ids"] = ",".join(duplicates)

                    # append change report to list of all changes
                    rd_muni = prefix.split("Change_Detection/")[1]
                    change_reports.append(
                        {
                            "title": "Data changes: " + os.path.join(rd_muni, layer.out_layer),
                            "body": "<br />".join(
                                [k + ": " + str(change_report[k]) for k in change_report]
                            ),
                        }
                    )
                    # write to csv for upload to s3
                    changes_csv_file = layer.out_layer + "_changes.csv"
                    with open(changes_csv_file, "w") as f:
                        writer = csv.writer(f)
                        writer.writerow(["key", "value"])
                        for key, value in change_report.items():
                            writer.writerow([key, value])

                    # upload to s3
                    changes_csv_s3_key = (
                        urlparse(prefix, allow_fragments=False).path.lstrip("/")
                        + "/"
                        + changes_csv_file
                    )
                    LOG.info(f"{changes_csv_s3_key}: writing to object storage")
                    s3_client.upload_file(
                        changes_csv_file, os.environ.get("BUCKET"), changes_csv_s3_key
                    )
                    os.unlink(changes_csv_file)

            if write:
                LOG.info(f"{s3_key}: writing to object storage")
                s3_client.upload_file(out_file + ".zip", os.environ.get("BUCKET"), s3_key)

                # also write duplicate report
                if duplicates:
                    LOG.info(
                        "Duplicates present, writing duplicate record log to object storage as csv"
                    )
                    dups_csv_file = layer.out_layer + "_duplicates.csv"
                    dups_csv_s3_key = (
                        urlparse(prefix, allow_fragments=False).path.lstrip("/")
                        + "/"
                        + dups_csv_file
                    )
                    with open(dups_csv_file, "w") as f:
                        writer = csv.DictWriter(f, fieldnames=duplicates[0].keys())
                        writer.writeheader()
                        writer.writerows(duplicates)
                    s3_client.upload_file(dups_csv_file, os.environ.get("BUCKET"), dups_csv_s3_key)

            # dump change summary to local json for creating GH issues
            with open("issues.json", "w") as f:
                json.dump(change_reports, f, indent=2)

            # cleanup
            shutil.rmtree(out_file)
            os.unlink(out_file + ".zip")
            shutil.rmtree("temp.gdb")


if __name__ == "__main__":
    cli()
