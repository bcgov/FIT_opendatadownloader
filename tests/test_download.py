import pytest
from jsonschema.exceptions import ValidationError
from fit_changedetector.download import download_source, parse_sources


@pytest.fixture
def test_data():
    return [
        {
            "admin_area_abbreviation": "Coquitlam",
            "admin_area_group_name_abbreviation": "MVRD",
            "metadata_url": None,
            "source": "https://coquitlam-spatial.s3.us-west-2.amazonaws.com/PRC/GDB/Coquitlam_Parks_GDB.zip",
            "layer": "parks",
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
        },
        {
            "admin_area_abbreviation": "Surrey",
            "admin_area_group_name_abbreviation": "MVRD",
            "metadata_url": None,
            "source": "https://cosmos.surrey.ca/geo_ref/Images/OpenDataArchives/parks_GDB.zip",
            "layer": "parks",
            "query": None,
            "fields": ["FACILITYID", "DESCRIPTION", "LOCATION", "PARK_NAME", "STATUS"],
            "primary_key": ["FACILITYID"],
            "schedule": "W",
        },
    ]


def test_parse_sources(test_data):
    source = parse_sources(test_data)[0]
    assert source["index"] == 1


def test_parse_alias(test_data):
    source = test_data[0]
    source["admin_area_abbreviation"] = "Port Coquitlam"
    sources = parse_sources([source])
    assert sources[0]["alias"] == "port_coquitlam"


def test_download(test_data, tmpdir):
    sources = parse_sources(test_data)
    source = sources[1]
    df = download_source(source)
    assert len(df) > 0


def test_invalid_admin(test_data):
    sources = test_data
    sources[0]["admin_area_abbreviation"] = "Victannich"
    with pytest.raises(ValidationError):
        parse_sources(sources)


def test_invalid_admin_group(test_data):
    sources = test_data
    sources[0]["admin_area_group_name_abbreviation"] = "METROVIC"
    with pytest.raises(ValidationError):
        parse_sources(sources)


def test_invalid_file(test_data):
    source = parse_sources(test_data)[0]
    source["fields"] = ["INVALID_COLUMN"]
    with pytest.raises(ValueError):
        download_source(source)


def test_invalid_schedule(test_data):
    sources = test_data
    sources[0]["schedule"] = "MONTH"
    with pytest.raises(ValidationError):
        parse_sources(sources)
