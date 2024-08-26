import json
import logging
import os
import re
import sys
import zipfile

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
import click
from cligj import verbose_opt, quiet_opt
import geopandas
import jsonschema
from pyproj import CRS
from slugify import slugify


LOG_FORMAT = "%(asctime)s:%(levelname)s:%(name)s: %(message)s"
LOG = logging.getLogger(__name__)


def upload_file_to_s3(file_path, bucket_name, s3_key):
    """
    Upload a file to an S3 bucket.

    :param file_path: Local path to the file to upload.
    :param bucket_name: Name of the S3 bucket.
    :param s3_key: Key (path) for the file in the S3 bucket.
    :return: None
    """
    s3_client = boto3.client('s3')

    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        LOG.info(f"File {file_path} uploaded to {bucket_name}/{s3_key}")
    except FileNotFoundError:
        LOG.error(f"The file {file_path} was not found.")
    except NoCredentialsError:
        LOG.error("Credentials not available.")
    except PartialCredentialsError:
        LOG.error("Incomplete credentials provided.")
    except ClientError as e:
        LOG.error(f"Client error: {e}")
    except Exception as e:
        LOG.error(f"An error occurred: {e}")


def configure_logging(verbosity):
    log_level = max(10, 30 - 10 * verbosity)
    logging.basicConfig(stream=sys.stderr, level=log_level, format=LOG_FORMAT)


def parse_sources(sources):
    """validate and parse sources data structure"""

    # validate sources against schema doc
    with open("source.schema.json", "r") as f:
        schema = json.load(f)
    jsonschema.validate(instance=sources, schema=schema)

    # add index key, enumerating the sources
    sources = [dict(d, index=index + 1) for (index, d) in enumerate(sources)]

    # add alias key, a sluggified/lowercasified version of admin_area_abbreviation
    parsed = sources
    for i, source in enumerate(sources):
        # create a slugified version of abbreviated name (and remove apostrophe from hudsons hope)
        parsed[i]["alias"] = slugify(
            source["admin_area_abbreviation"].replace("'", ""), separator="_", lowercase=True
        )

    LOG.info("Source json is valid")
    return parsed


def download_source(source):
    """download data and do some simple validation
    """
    # load file
    df = geopandas.read_file(
        os.path.expandvars(source["source"]),
        layer=source["layer"],
        where=source["query"],
    )

    # are expected columns present?
    columns = [x.lower() for x in df.columns]
    for column in source["fields"]:
        if column and column.lower() not in columns:
            raise ValueError(
                f"Validation error: {source['alias']} - column {column} is not present, modify config 'fields'"
            )

    # is there data?
    count = len(df.index)
    if count == 0:
        raise ValueError(
            f"Validation error: {source['alias']} - no data returned, check source and query"
        )

    # is a crs defined?
    if not df.crs:
        raise ValueError(
            "Source does not have a defined projection/coordinate reference system"
        )

    # presume layer is defined correctly if no errors are raised
    LOG.info(f"Download and validation successful: {source['alias']} - record count: {str(count)}")
    return df


def zip_gdb(folder_path, zip_path):
    """
    Compress the contents of an entire folder into a zip file.

    :param folder_path: Path to the folder to be zipped.
    :param zip_path: Path to the resulting zip file.
    """
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Walk through the directory
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                # Create the full file path
                file_path = os.path.join(root, file)
                # Create a relative path for the file in the zip
                relative_path = os.path.relpath(file_path, folder_path)
                # Add file to the zip file
                zipf.write(file_path, relative_path)


def is_s3_path(path):
    """
    Check if the given path is an S3 path.

    :param path: The path to check.
    :return: True if the path is an S3 path, False otherwise.
    """
    # Define a regular expression to match S3 paths
    s3_pattern = re.compile(r'^s3://')
    return bool(s3_pattern.match(path))


@click.command()
@click.argument("sources_file", type=click.Path(exists=True), default="sources.json")
@click.option(
    "--out_file",
    "-o",
    default="parks.gdb",
    help="Output file name",
)
@click.option(
    "--out_prefix",
    "-p",
    type=click.Path(),
    default=".",
    help="Output s3 prefix",
)
@click.option(
    "--out_layer",
    "-nln",
    default="parks",
    help="Name of output file",
)
@click.option(
    "--source_alias",
    "-s",
    default=None,
    help="Validate and download just the specified source",
)
@click.option(
    "--dry_run",
    "-t",
    is_flag=True,
    help="Validate sources only, do not write data to file",
)
@verbose_opt
@quiet_opt
def download(
    sources_file, out_file, out_prefix, out_layer, source_alias, dry_run, verbose, quiet
):
    """Download sources defined in provided file"""
    configure_logging((verbose - quiet))

    # open sources file
    with open(sources_file, "r") as f:
        sources = json.load(f)

    # parse it
    sources = parse_sources(sources_file)

    # if specified, use only one source
    if source_alias:
        sources = [s for s in sources if s["alias"] == source_alias]

    # process all sources
    for source in sources:

        # download data, check to see if it meets expectations
        df = download_source(source)

        if not dry_run:

            # reproject to BC Albers if necessary
            if df.crs != CRS.from_user_input(3005):
                df = df.to_crs("EPSG:3005")

            # standardize column names
            df.columns = [x.lower() for x in df.columns]
            df = df.rename_geometry("geom")

            # retain only fields of interest
            df = df[[c.lower() for c in source["fields"]] + ["geom"]]

            # dump to temp local file
            df.to_file(out_file, driver="OpenFileGDB", layer=out_layer)

            # compress
            zip_gdb(out_file, out_file+".zip")

            # upload
            s3_key = "/".join([out_prefix, source[""], source[""], out_file + ".zip"])
            upload_file_to_s3(out_file+".zip", os.environ.get("BUCKET"), s3_key)

            LOG.info(f"{source['alias']} written to {s3_key}")


if __name__ == "__main__":
    download()
