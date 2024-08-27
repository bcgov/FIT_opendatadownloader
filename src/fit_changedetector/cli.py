import logging
import json
from pathlib import Path
import sys

import fit_changedetector as fcd
import click
from cligj import verbose_opt, quiet_opt


LOG = logging.getLogger(__name__)


def configure_logging(verbosity):
    log_level = max(10, 30 - 10 * verbosity)
    logging.basicConfig(
        stream=sys.stderr,
        level=log_level,
        format="%(asctime)s:%(levelname)s:%(name)s: %(message)s",
    )


@click.group()
@click.version_option(version=fcd.__version__, message="%(version)s")
def cli():
    pass


@cli.command()
@click.argument("sources_file", type=click.Path(exists=True), default="sources.json")
@click.option(
    "--out_file",
    "-o",
    help="Output file name",
)
@click.option(
    "--out_path",
    "-p",
    type=click.Path(),
    default=".",
    help="Output path or s3 prefix",
)
@click.option(
    "--out_layer",
    "-nln",
    help="Output layer name",
)
@click.option(
    "--source_alias",
    "-s",
    default=None,
    help="Validate and download only the specified source",
)
@click.option(
    "--dry_run",
    "-t",
    is_flag=True,
    help="Validate sources (do not write data to file)",
)
@click.option(
    "--schedule",
    "-s",
    type=click.Choice(["D", "W", "M", "Q", "A"], case_sensitive=False),
    help="Validate and download sources with given schedule tag",
)
@verbose_opt
@quiet_opt
def download(
    sources_file,
    out_file,
    out_path,
    out_layer,
    source_alias,
    dry_run,
    schedule,
    verbose,
    quiet,
):
    """Download sources as defined in provided config"""
    configure_logging((verbose - quiet))

    # open sources file
    with open(sources_file, "r") as f:
        sources = json.load(f)

    # parse sources
    sources = fcd.parse_sources(sources)

    # if specified, use only one source
    if source_alias:
        sources = [s for s in sources if s["alias"] == source_alias]

    # is specified, use only sources with given schedule tag
    if schedule:
        sources = [s for s in sources if s["schedule"] == schedule]
        # alert if no sources match this schedule
        if len(sources) == 0:
            LOG.warning(f"No source with schedule={schedule} found in {sources_file}")

    # default to writing to file/layer with same name as sources config
    if not out_file:
        out_file = Path(sources_file).with_suffix(".gdb").name
    if not out_layer:
        out_layer = Path(sources_file).stem

    # process all sources
    for source in sources:

        # download data, do some tidying/standardization
        df = fcd.download_source(source)

        if not dry_run:
            fcd.save_source(df, source, out_path, out_file, out_layer)
