import geopandas
import fit_changedetector as fcd


def load(source_file_a, source_file_b, primary_key, fields=None):
    """Load input datasets and inspect - are they comparable?"""
    df_a = geopandas.read_file(source_file_a)
    df_b = geopandas.read_file(source_file_b)

    # field names equivalent? (for fields of interest)
    fields_a = set([c for c in df_a.columns if c != "geometry"])
    fields_b = set([c for c in df_b.columns if c != "geometry"])
    fields_common = fields_a.intersection(fields_b)
    if fields:
        if len(set(fields).intersection(fields_common)) != len(fields):
            raise ValueError("Provided fields are not common to both datasets")
    else:
        fields = list(fields_common)

    if len(fields) == 0:
        raise ValueError("Datasets have no field names in common, cannot compare")

    # check primary key is present in both datasets (it must have the same name)
    if primary_key not in fields:
        raise ValueError(f"Primary key {primary_key} must be present in both datasets")

    # only retain data for common fields of interest (and geometry)
    df_a = df_a[fields + ["geometry"]]
    df_b = df_b[fields + ["geometry"]]

    # are general data types of the common fields equivalent?
    if list(df_a.dtypes) != list(df_b.dtypes):
        raise ValueError("Field types do not match")

    # are geometry data types equivalent (and valid)?
    geomtypes_a = set([t.upper() for t in df_a.geometry.geom_type.unique()])
    geomtypes_b = set([t.upper() for t in df_b.geometry.geom_type.unique()])

    if geomtypes_a != geomtypes_b:
        raise ValueError(
            f"Geometry types {','.join(list(geomtypes_a))} and {','.join(list(geomtypes_b))} are not equivalent"
        )

    # are geometry types supported?
    for geomtypes in [geomtypes_a, geomtypes_b]:
        unsupported = geomtypes.difference(fcd.supported_spatial_types)
        if unsupported:
            raise ValueError(f"Geometries of type {unsupported} are not supported")

    # is CRS equivalent?
    if df_a.crs != df_b.crs:
        raise ValueError("Coordinate reference systems are not equivalent")

    # is primary key unique in both datasets?
    if len(df_a) != len(df_a[[primary_key]].drop_duplicates()):
        raise ValueError(
            f"Duplicate values exist for primary_key {primary_key}, in {source_file_a} consider using another primary key or pre-processing to remove duplicates"
        )
    if len(df_b) != len(df_b[[primary_key]].drop_duplicates()):
        raise ValueError(
            f"Duplicate values exist for primary_key {primary_key}, in {source_file_b} consider using another primary key or pre-processing to remove duplicates"
        )

    # set pandas dataframe index to primary key
    df_a = df_a.set_index(primary_key)
    df_b = df_b.set_index(primary_key)

    # if no error raised, return both dataframes
    return (df_a, df_b)


def diff(df_a, df_b, tolerance):
    """compare data frames by joining on indexes (primary keys)"""
    # find additions / deletions by joining on indexes
    joined = df_a.merge(
        df_b,
        how="outer",
        left_index=True,
        right_index=True,
        suffixes=["_a", "_b"],
        indicator=True,
    )
    additions = joined[joined["_merge"] == "right_only"]
    deletions = joined[joined["_merge"] == "left_only"]
    common = joined[joined["_merge"] == "both"]

    # find rows with modified attributes
    columns = list(df_a.columns)
    # remove the a/b suffixes from the join result,
    # generating two new df with rows common to inputs
    column_name_remap_a = {k + "_a": k for k in columns}
    column_name_remap_b = {k + "_b": k for k in columns}
    common_a = common.rename(columns=column_name_remap_a)[columns]
    common_b = common.rename(columns=column_name_remap_b)[columns]

    # compare the attributes, returning only those that have changed
    common_a_attrib = common_a.drop("geometry", axis=1)
    common_b_attrib = common_b.drop("geometry", axis=1)
    modifications_attributes = common_a_attrib.compare(
        common_b_attrib, result_names=("a", "b")
    )

    # find rows with modified geometries
    modifications_geometries = common[~common_a.geom_almost_equals(common_b, tolerance)]

    # find rows with modified geometries AND attributes
    modifications_both = set(list(modifications_attributes.index.array)) & set(
        list(modifications_geometries.index.array)
    )
    return (
        additions,
        deletions,
        modifications_attributes,
        modifications_geometries,
        modifications_both,
    )
