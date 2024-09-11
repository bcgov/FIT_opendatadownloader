import fit_changedetector as fcd


def test_diff():
    # one instance of each type of change is found
    # when comparing these two test files
    d = fcd.differ(
        "tests/data/test_parks_a.geojson",
        "tests/data/test_parks_b.geojson",
        primary_key="fcd_load_id",
    )
    assert len(d[0] == 1)
    assert len(d[1] == 1)
    assert len(d[2] == 1)
    assert len(d[3] == 1)
    assert len(d[4] == 1)
