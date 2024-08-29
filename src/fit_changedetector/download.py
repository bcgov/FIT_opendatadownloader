import json
import logging
import os
from pathlib import Path
import re
from urllib.parse import urlparse
import zipfile

import boto3
import geopandas
import jsonschema
from pyproj import CRS
from slugify import slugify


LOG = logging.getLogger(__name__)


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
            source["admin_area_abbreviation"].replace("'", ""),
            separator="_",
            lowercase=True,
        )

    # add null layer key if not present
    if "layer" not in source.keys():
        source["layer"] = None

    LOG.info("Source json is valid")
    return parsed


def download_source(source):
    """
    Download data, do some simple validation and standardization

    :source: Dict defining source
    :return: BC Albers GeoDataframe, with desired columns in lowercase
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
    LOG.info(
        f"Download and validation successful: {source['alias']} - record count: {str(count)}"
    )

    # reproject to BC Albers if necessary
    if df.crs != CRS.from_user_input(3005):
        df = df.to_crs("EPSG:3005")

    # standardize column names
    df.columns = [x.lower() for x in df.columns]
    df = df.rename_geometry("geom")

    # retain only fields of interest
    df = df[[c.lower() for c in source["fields"]] + ["geom"]]

    return df


def save_source(df, source, out_path, out_file, out_layer):
    """
    Save downloaded dataframe to <out_path>/<out_file>.zip/<out_layer>
    """
    # write df to current working directory
    df.to_file(out_file, driver="OpenFileGDB", layer=out_layer)

    # compress
    zip_gdb(out_file, out_file + ".zip")

    # copy to s3 if out_path prefix is s3://
    if bool(re.compile(r"^s3://").match(out_path)):
        prefix = urlparse(out_path, allow_fragments=False).path.lstrip("/")
        s3_key = "/".join(
            [
                prefix,
                source["admin_area_group_name_abbreviation"],
                source["alias"],
                out_file + ".zip",
            ]
        )
        s3_client = boto3.client("s3")
        s3_client.upload_file(out_file + ".zip", os.environ.get("BUCKET"), s3_key)
        LOG.info(f"{s3_key} saved to S3")

    # alternatively, move to local path
    else:
        out_path = os.path.join(
            out_path,
            source["admin_area_group_name_abbreviation"],
            source["alias"],
        )
        Path(out_path).mkdir(parents=True, exist_ok=True)
        destination = os.path.join(
            out_path,
            out_file + ".zip",
        )
        os.rename(out_file + ".zip", destination)
        LOG.info(f"{destination} saved to disk.")


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
