import os

import boto3
import geopandas
import pytest
from click.testing import CliRunner

from fit_opendatadownloader.fit_downloader import cli


@pytest.fixture(autouse=True)
def cleanup():
    s3_client = boto3.client("s3")
    yield
    # after every test, delete everything in bucket with prefix /Change_Detection/_TESTING/test/
    response = s3_client.list_objects_v2(
        Bucket=os.environ.get("BUCKET"), Prefix="Change_Detection/_TESTING/test/"
    )
    if "Contents" in response:
        objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
        s3_client.delete_objects(
            Bucket=os.environ.get("BUCKET"), Delete={"Objects": objects_to_delete}
        )


def test_fresh_download():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "process",
            "tests/test_config_a.json",
            "_TESTING/test",
            "--layer",
            "parks",
            "-v",
        ],
    )
    assert result.exit_code == 0
    df = geopandas.read_file(
        os.path.join(
            "s3://", os.environ.get("BUCKET"), "Change_Detection/_TESTING/test/parks.gdb.zip"
        )
    )
    assert len(df) == 8


def test_download_changed():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "process",
            "tests/test_config_a.json",
            "_TESTING/test",
            "--layer",
            "parks",
            "-v",
        ],
    )
    result = runner.invoke(
        cli,
        [
            "process",
            "tests/test_config_b.json",
            "_TESTING/test",
            "--layer",
            "parks",
            "-v",
        ],
    )
    assert result.exit_code == 0
    df = geopandas.read_file(
        os.path.join(
            "s3://", os.environ.get("BUCKET"), "Change_Detection/_TESTING/test/parks.gdb.zip"
        )
    )
    assert len(df) == 8
    df = geopandas.read_file(
        os.path.join(
            "s3://",
            os.environ.get("BUCKET"),
            "Change_Detection/_TESTING/test/parks_changes.gdb.zip",
        ),
        layer="NEW",
    )
    assert len(df) == 1
    df = geopandas.read_file(
        os.path.join(
            "s3://",
            os.environ.get("BUCKET"),
            "Change_Detection/_TESTING/test/parks_changes.gdb.zip",
        ),
        layer="DELETED",
    )
    assert len(df) == 1
