import os

import boto3
import geopandas
import pytest
from click.testing import CliRunner

from fit_opendatadownloader.fit_downloader import cli, s3_key_exists

_s3 = boto3.client("s3")


@pytest.fixture(autouse=True)
def cleanup():
    yield
    # after every test, delete everything in bucket with prefix /Change_Detection/TEST/test/
    response = _s3.list_objects_v2(
        Bucket=os.environ.get("BUCKET"), Prefix="Change_Detection/TEST/test/"
    )
    if "Contents" in response:
        objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
        _s3.delete_objects(Bucket=os.environ.get("BUCKET"), Delete={"Objects": objects_to_delete})


def test_fresh_download():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "process",
            "tests/test_config_a.json",
            "--layer",
            "parks",
            "--prefix",
            "s3://$BUCKET/Change_Detection/TEST/test",
            "-v",
        ],
    )
    assert result.exit_code == 0
    df = geopandas.read_file(
        os.path.join("s3://", os.environ.get("BUCKET"), "Change_Detection/TEST/test/parks.gdb.zip")
    )
    assert len(df) == 8


def test_download_unchanged():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "process",
            "tests/test_config_a.json",
            "--layer",
            "parks",
            "--prefix",
            "s3://$BUCKET/Change_Detection/TEST/test",
            "-v",
        ],
    )
    result = runner.invoke(
        cli,
        [
            "process",
            "tests/test_config_a.json",
            "--layer",
            "parks",
            "--prefix",
            "s3://$BUCKET/Change_Detection/TEST/test",
            "-v",
        ],
    )
    assert result.exit_code == 0
    df = geopandas.read_file(
        os.path.join("s3://", os.environ.get("BUCKET"), "Change_Detection/TEST/test/parks.gdb.zip")
    )
    assert len(df) == 8
    assert s3_key_exists(_s3, "Change_Detection/TEST/test/parks_changes.gdb.zip") is False
    assert s3_key_exists(_s3, "Change_Detection/TEST/test/parks_changes.csv") is False


def test_download_changed():
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "process",
            "tests/test_config_a.json",
            "--layer",
            "parks",
            "--prefix",
            "s3://$BUCKET/Change_Detection/TEST/test",
            "-v",
        ],
    )
    result = runner.invoke(
        cli,
        [
            "process",
            "tests/test_config_b.json",
            "--layer",
            "parks",
            "--prefix",
            "s3://$BUCKET/Change_Detection/TEST/test",
            "-v",
        ],
    )
    assert result.exit_code == 0
    df = geopandas.read_file(
        os.path.join("s3://", os.environ.get("BUCKET"), "Change_Detection/TEST/test/parks.gdb.zip")
    )
    assert len(df) == 8
    assert s3_key_exists(_s3, "Change_Detection/TEST/test/parks_changes.gdb.zip")
    df = geopandas.read_file(
        os.path.join(
            "s3://", os.environ.get("BUCKET"), "Change_Detection/TEST/test/parks_changes.gdb.zip"
        ),
        layer="NEW",
    )
    assert len(df) == 1
    df = geopandas.read_file(
        os.path.join(
            "s3://", os.environ.get("BUCKET"), "Change_Detection/TEST/test/parks_changes.gdb.zip"
        ),
        layer="DELETED",
    )
    assert len(df) == 1
