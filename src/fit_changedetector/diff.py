import geopandas
import fit_changedetector as fcd


def differ(source_file_a, source_file_b, primary_key, fields=None, precision=2):
    df_a = geopandas.read_file(source_file_a)
    df_b = geopandas.read_file(source_file_b)

    # field names equivalent? (for fields of interest)
    fields_a = set([c for c in df_a.columns if c != "geometry"])
    fields_b = set([c for c in df_b.columns if c != "geometry"])
    fields_common = fields_a.intersection(fields_b)

    # is primary key present in both datasets?
    if primary_key not in fields_common:
        raise ValueError(f"Primary key {primary_key} must be present in both datasets")

    # if provided a list of fields to work with, validate that list
    if fields:
        fields = list(set(fields + [primary_key]))
        if len(set(fields).intersection(fields_common)) != len(fields):
            raise ValueError("Provided fields are not common to both datasets")
    else:
        fields = list(fields_common)

    if len(fields) == 0:
        raise ValueError("Datasets have no field names in common, cannot compare")

    # remove all columns other than primary key, common fields of interest, and geometry
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

    # clean column names in resulting dataframes
    columns = list(df_a.columns)
    column_name_remap_a = {k + "_a": k for k in columns}
    column_name_remap_b = {k + "_b": k for k in columns}
    # additions is data from source b
    additions = additions.rename(columns=column_name_remap_b)[columns]
    # deletions is data from source a
    deletions = deletions.rename(columns=column_name_remap_a)[columns]

    # create two dataframes holding records from respective source
    # that are common to both sources
    common_a = common.rename(columns=column_name_remap_a)[columns]
    common_b = common.rename(columns=column_name_remap_b)[columns]

    # compare the attributes
    common_a_attrib = common_a.drop("geometry", axis=1)
    common_b_attrib = common_b.drop("geometry", axis=1)
    modified_attributes = common_a_attrib.compare(
        common_b_attrib,
        result_names=(
            "a",
            "b",
        ),
        keep_shape=True,
    ).dropna(axis=0, how="all")

    # flatten the resulting data structure
    modified_attributes.columns = [
        "_".join(a) for a in modified_attributes.columns.to_flat_index()
    ]

    # join back to geometries in b, creating attribute diff
    modified_attributes.merge(
        common_b["geometry"], how="inner", left_index=True, right_index=True
    )
    # note the columns generated
    attribute_diff_columns = list(modified_attributes.columns.values) + ["geometry"]

    # find all rows with modified geometries, retaining new geometries only
    common_mod_geoms = common.rename(columns=column_name_remap_b)[columns]
    modified_geometries = common_mod_geoms[
        ~common_a.geom_equals_exact(common_b, precision)
    ]

    # join modified attributes to modified geometries
    modified_attributes_geometries = modified_attributes.merge(
        modified_geometries,
        how="outer",
        left_index=True,
        right_index=True,
        indicator=True,
    )

    # two sets of modified attributes, using diff columns
    m_attributes = modified_attributes_geometries[
        modified_attributes_geometries["_merge"] == "left_only"
    ][attribute_diff_columns]
    m_attributes_geometries = modified_attributes_geometries[
        modified_attributes_geometries["_merge"] == "both"
    ][attribute_diff_columns]

    # modified geoms only, using source column names
    m_geometries = modified_attributes_geometries[
        modified_attributes_geometries["_merge"] == "right_only"
    ][columns]

    # todo - returning 5 dataframes is fine,
    # but as a dict prob better? or as properties of a diff/change detector object?
    return (additions, deletions, m_attributes_geometries, m_attributes, m_geometries)
