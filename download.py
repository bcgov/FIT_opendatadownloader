import json
import logging
import os
from pathlib import Path
import sys

import click
from cligj import verbose_opt, quiet_opt
import geopandas
import jsonschema
from pyproj import CRS
from slugify import slugify


LOG_FORMAT = "%(asctime)s:%(levelname)s:%(name)s: %(message)s"
LOG = logging.getLogger(__name__)


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
                f"Validation error: {source['alias']} - column {column} is not present, modify config 'field_mapper'"
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


@click.command()
@click.argument("sources_file", type=click.Path(exists=True), default="sources.json")
@click.option(
    "--out_file",
    "-of",
    default="parks.gdb",
    help="Output file name",
)
@click.option(
    "--out_path",
    "-o",
    type=click.Path(),
    default=".",
    help="Output path to write data (local or s3://)",
)
@click.option(
    "--out_layer",
    "-of",
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
    sources_file, out_file, out_path, out_layer, source_alias, dry_run, verbose, quiet
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
            # clean
            # reproject to BC Albers if necessary
            if df.crs != CRS.from_user_input(3005):
                df = df.to_crs("EPSG:3005")

            # standardize column names
            df.columns = [x.lower() for x in df.columns]
            df = df.rename_geometry("geom")

            # retain only fields of interest
            df = df[[c.lower() for c in source["fields"]] + ["geom"]]

            # dump to file
            full_out_path = os.path.join(
                out_path,
                source["admin_area_group_name_abbreviation"],
                source["alias"]
            )
            Path(full_out_path).mkdir(parents=True, exist_ok=True)
            full_out_file = os.path.join(full_out_path, out_file)
            df.to_file(full_out_file, driver="OpenFileGDB", layer=out_layer)
            LOG.info(f"{source['alias']} written to {full_out_file}")


if __name__ == "__main__":
    download()
