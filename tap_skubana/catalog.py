from singer import metadata


def generate_catalog(streams):

    catalog = {}
    catalog['streams'] = []
    for stream in streams:
        schema = stream.load_schema()
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream.key_properties,
            valid_replication_keys=stream.valid_replication_keys,
            replication_method=stream.replication_method)
        mdata = metadata.to_map(mdata)
        catalog_entry = {
            'stream': stream.name,
            'tap_stream_id': stream.name,
            'schema': schema,
            'metadata': metadata.to_list(mdata)
        }
        catalog['streams'].append(catalog_entry)

    return catalog
