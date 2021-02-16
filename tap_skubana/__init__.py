import json
import sys

import singer
from singer import metadata
from singer.utils import strptime_to_utc
from tap_skubana.catalog import generate_catalog
from tap_skubana.client import SkubanaClient
from tap_skubana.streams import AVAILABLE_STREAMS

LOGGER = singer.get_logger()


def discover(client):
    LOGGER.info('Starting Discovery..')
    streams = [
        stream_class(client) for _, stream_class in AVAILABLE_STREAMS.items()
    ]
    catalog = generate_catalog(streams)
    json.dump(catalog, sys.stdout, indent=2)


def stream_is_selected(mdata):
    return mdata.get((), {}).get('selected', False)


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def get_selected_streams(catalog):
    selected_stream_ids = []
    for stream in catalog.streams:
        mdata = metadata.to_map(stream.metadata)
        if stream_is_selected(mdata):
            selected_stream_ids.append(stream.tap_stream_id)
    return selected_stream_ids


def sync(client, config, catalog, state):
    LOGGER.info('Starting Sync..')
    selected_streams = catalog.get_selected_streams(state)

    streams = []
    stream_keys = []

    for catalog_entry in selected_streams:
        streams.append(catalog_entry)
        stream_keys.append(catalog_entry.stream)

    total_streams_count = 0
    for catalog_entry in streams:
        stream = AVAILABLE_STREAMS[catalog_entry.stream](client=client,
                                                         config=config,
                                                         catalog=catalog,
                                                         state=state)
        LOGGER.info('Syncing stream: %s', catalog_entry.stream)
        bookmark_date = stream.get_bookmark(stream.name, config['start_date'])
        bookmark_dttm = strptime_to_utc(bookmark_date)
        stream_schema = catalog_entry.schema.to_dict()
        stream_metadata = metadata.to_map(catalog_entry.metadata)

        update_currently_syncing(state, stream.name)
        count = stream.sync(stream_schema, stream_metadata, bookmark_dttm)
        total_streams_count = total_streams_count + count
        update_currently_syncing(state, None)
        LOGGER.info('Finishied sync for %s: count: %d', stream.name, count)
    LOGGER.info('Finished Sync: total records: %d', total_streams_count)


def main():
    parsed_args = singer.utils.parse_args(required_config_keys=['token'])
    config = parsed_args.config

    client = SkubanaClient(config)

    if parsed_args.discover:
        discover(client=client)
    elif parsed_args.catalog:
        sync(client, config, parsed_args.catalog, parsed_args.state)


if __name__ == '__main__':
    main()
