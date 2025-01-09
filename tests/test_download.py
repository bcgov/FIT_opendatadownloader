import json

import pytest
from jsonschema.exceptions import ValidationError

import fit_opendatadownloader as fdl


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
                "SpeedLimit",
            ],
            "primary_key": ["AssetID"],
            "schedule": "Q",
        }
    ]


# parsing does not fail so config is valid
def test_parse_config(test_config_file):
    layer = fdl.parse_config(test_config_file)[0]
    assert layer.out_layer == "parks"


def test_all_keys_present():
    # read schema document
    with open("source_schema.json", "r") as f:
        schema = json.load(f)
    # create source layer from required keys
    source_dict = {k: "foo" for k in schema["items"]["required"]}
    layer = fdl.SourceLayer(source_dict)
    # assert that all expected attributes are present in sourcelayer object
    for k in schema["items"]["properties"]:
        assert hasattr(layer, k)


def test_download_file(test_config_file, tmpdir):
    layer = fdl.parse_config(test_config_file)[0]
    df = layer.download()
    assert len(df) > 0


def test_download_bcgw(tmpdir):
    sources = [
        {
            "out_layer": "parks",
            "source": "WHSE_IMAGERY_AND_BASE_MAPS.GSR_AIRPORTS_SVW",
            "protocol": "bcgw",
            "fields": [
                "SOURCE_DATA_ID",
                "AIRPORT_NAME",
                "DESCRIPTION",
                "LOCALITY",
            ],
            "query": "SOURCE_DATA_ID in (456, 457, 458)",
            "primary_key": ["SOURCE_DATA_ID"],
            "schedule": "Q",
        }
    ]
    layer = fdl.parse_config(sources)[0]
    df = layer.download()
    assert len(df) == 3


def test_invalid_file(test_config_file):
    layer = fdl.parse_config(test_config_file)[0]
    layer.fields = ["INVALID_COLUMN"]
    with pytest.raises(ValueError):
        layer.download()


def test_invalid_pk(test_config_file):
    config = test_config_file
    config[0]["primary_key"] = ["PARK_NAME_INVALID"]
    with pytest.raises(ValueError):
        fdl.parse_config(config)


def test_invalid_schedule(test_config_file):
    sources = test_config_file
    sources[0]["schedule"] = "MONTH"
    with pytest.raises(ValidationError):
        fdl.parse_config(sources)


def test_clean_columns():
    sources = [
        {
            "out_layer": "parks",
            "source": "tests/data/fieldnames.geojson",
            "protocol": "http",
            "fields": [
                "SOURCE_DATA_ID",
                "SUPPLIED_SOURCE_ID_IND",
                "AIRPO#RT NAME $",
                "DESCRIPTION",
                "PHYSICAL_ADDRESS",
                "ALIAS_ADDRESS",
                "STREET_ADDRESS",
                "POSTAL_CODE",
                "LOCALITY",
            ],
            "primary_key": ["SOURCE_DATA_ID"],
            "schedule": "Q",
        }
    ]
    layer = fdl.parse_config(sources)[0]
    df = layer.download()
    df = fdl.clean(df, fields=layer.fields, primary_key=layer.primary_key, precision=0.1)[0]
    assert "airport_name_" in df.columns


def test_hash_pk():
    sources = [
        {
            "out_layer": "parks",
            "source": "tests/data/fieldnames.geojson",
            "protocol": "http",
            "fields": [
                "SOURCE_DATA_ID",
                "SUPPLIED_SOURCE_ID_IND",
                "AIRPO#RT NAME $",
                "DESCRIPTION",
                "PHYSICAL_ADDRESS",
                "ALIAS_ADDRESS",
                "STREET_ADDRESS",
                "POSTAL_CODE",
                "LOCALITY",
            ],
            "schedule": "Q",
        }
    ]
    layer = fdl.parse_config(sources)[0]
    df = layer.download()
    df = fdl.clean(df, fields=layer.fields, precision=0.1)[0]
    assert df["fdl_load_id"].iloc[0] == "597b8d8bef757cb12fec15ce027fb2c6f84775d7"


def test_mixed_types():
    sources = [
        {
            "out_layer": "parks",
            "source": "tests/data/mixed_types.geojson",
            "protocol": "http",
            "fields": [
                "SOURCE_DATA_ID",
            ],
            "schedule": "Q",
        }
    ]
    layer = fdl.parse_config(sources)[0]
    df = layer.download()
    df = fdl.clean(df, fields=layer.fields, primary_key=layer.primary_key, precision=0.1)[0]
    assert [t.upper() for t in df.geometry.geom_type.unique()] == ["MULTIPOINT"]


def test_duplicate_pk():
    sources = [
        {
            "out_layer": "parks",
            "source": "tests/data/dups.geojson",
            "protocol": "http",
            "fields": [
                "SOURCE_DATA_ID",
            ],
            "primary_key": [
                "SOURCE_DATA_ID",
            ],
            "schedule": "Q",
        }
    ]
    layer = fdl.parse_config(sources)[0]
    df = layer.download()
    with pytest.raises(ValueError):
        df = fdl.clean(df, fields=layer.fields, primary_key=layer.primary_key, precision=0.1)[0]


def test_duplicate_geom():
    sources = [
        {
            "out_layer": "parks",
            "source": "tests/data/dups_geom.json",
            "protocol": "http",
            "fields": [
                "description",
            ],
            "schedule": "Q",
        }
    ]
    layer = fdl.parse_config(sources)[0]
    df = layer.download()
    df, duplicates = fdl.clean(df, fields=layer.fields, precision=0.1, drop_geom_duplicates=True)
    assert len(duplicates) == 2
    assert duplicates[1]["description"] == "heliport_dup2"


def test_hash_fields():
    sources = [
        {
            "out_layer": "parks",
            "source": "tests/data/dups.geojson",
            "protocol": "http",
            "fields": ["SOURCE_DATA_ID", "DESCRIPTION"],
            "hash_fields": ["DESCRIPTION"],
            "schedule": "Q",
        }
    ]
    layer = fdl.parse_config(sources)[0]
    df = layer.download()
    df.at[1, "geometry"] = df.at[0, "geometry"]
    assert (
        len(fdl.clean(df, fields=layer.fields, hash_fields=layer.hash_fields, precision=0.1)[0])
        == 2
    )
