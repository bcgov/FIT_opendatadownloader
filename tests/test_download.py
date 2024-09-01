import pytest
from jsonschema.exceptions import ValidationError
import fit_changedetector as fcd


@pytest.fixture
def test_config_file():
    return [
        {
            "out_layer": "parks",
            "metadata_url": None,
            "source": "https://coquitlam-spatial.s3.us-west-2.amazonaws.com/PRC/GDB/Coquitlam_Parks_GDB.zip",
            "source_layer": "parks",
            "protocol": "http",
            "query": None,
            "fields": [
                "PARKNAME",
                "ADDRESS",
                "PARKTYPE",
                "OWNERSHIP",
                "AREA_ACRES",
                "PARK_LEVEL",
            ],
            "primary_key": ["PARKNAME", "AREA_ACRES"],
            "schedule": "M",
        }
    ]


@pytest.fixture
def test_config_esri():
    return [
        {
            "out_layer": "roads",
            "metadata_url": None,
            "source": "https://services2.arcgis.com/CnkB6jCzAsyli34z/arcgis/rest/services/OpenData_RoadTraffic/FeatureServer/4",
            "protocol": "esri",
            "query": None,
            "fields": [
                "AssetID",
                "Location",
                "StrName",
                "StrType",
                "SurfaceMaterial",
                "NumberofLanes",
                "StrPrefix",
                "StrSuffix",
                "FullName",
                "LF_Addr",
                "RF_Addr",
                "LT_Addr",
                "RT_Addr",
                "SubType_TEXT",
                "DGRoute",
                "SpeedLimit"
            ],
            "primary_key": ["AssetID"],
            "schedule": "Q"
        }
    ]


# parsing does not fail so config is valid
def test_parse_config(test_config_file):
    source = fcd.parse_config(test_config_file)[0]
    assert source["out_layer"] == "parks"


def test_download_file(test_config_file, tmpdir):
    source = fcd.parse_config(test_config_file)[0]
    df = fcd.download(source)
    assert len(df) > 0


def test_download_esri(test_config_esri, tmpdir):
    source = fcd.parse_config(test_config_esri)[0]
    df = fcd.download(source)
    assert len(df) > 0


def test_invalid_file(test_config_file):
    source = fcd.parse_config(test_config_file)[0]
    source["fields"] = ["INVALID_COLUMN"]
    with pytest.raises(ValueError):
        fcd.download(source)


def test_invalid_schedule(test_config_file):
    sources = test_config_file
    sources[0]["schedule"] = "MONTH"
    with pytest.raises(ValidationError):
        fcd.parse_config(sources)
