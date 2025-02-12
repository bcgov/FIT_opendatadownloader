import csv
import glob
import os
from pathlib import Path

import click


@click.command()
@click.argument("sources_path")
@click.option("--valid_sources_csv", default="valid_sources.csv")
@click.option("--csv_column", default="filename")
def validate_source_filenames(sources_path, valid_sources_csv, csv_column):
    """
    ensure all files in path have name matching those listed in given column of
    provided csv
    """
    filenames = []
    files = glob.glob(os.path.join(sources_path, "[!_]**/*.json"), recursive=True)
    # extract just final folder/file string from file path
    for f in files:
        path = Path(f)
        file_name = path.name
        folder = path.parent.name
        filenames.append(os.path.join(folder, os.path.splitext(file_name)[0]))
    filenames = set(filenames)
    valid_filenames = []
    with open(valid_sources_csv) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            valid_filenames.append(row[csv_column])

    invalid = ",".join(list(filenames - set(valid_filenames)))
    if invalid:
        print(f"Invalid source names for files: {invalid}")
    else:
        n = len(files)
        print(f"Names of all {n} json files in {sources_path} are valid")


if __name__ == "__main__":
    validate_source_filenames()
