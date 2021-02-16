import os
from datetime import timedelta

import singer
from singer import Transformer, metrics, utils
from singer.utils import DATETIME_PARSE, strftime, strptime_to_utc

LOGGER = singer.get_logger()


#pylint: disable=no-member
class BaseStream:
    name = None
    filter_params = {}
    data_location = None
    limit = 100
    single_page_response = False

    def __init__(self, client=None, config=None, catalog=None, state=None):
        self.client = client
        self.config = config
        self.catalog = catalog
        self.state = state

    @staticmethod
    def get_abs_path(path):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

    def load_schema(self):
        schema_path = self.get_abs_path('schemas')
        return singer.utils.load_json('{}/{}.json'.format(
            schema_path, self.name))

    def write_schema(self):
        schema = self.load_schema()
        return singer.write_schema(stream_name=self.name,
                                   schema=schema,
                                   key_properties=self.key_properties)

    def write_state(self):
        return singer.write_state(self.state)

    def get_bookmark(self, stream, default):
        if (self.state is None) or ('bookmarks' not in self.state) or (
                stream not in self.state['bookmarks']):
            return default
        if not self.state.get('bookmarks').get(stream):
            self.state['bookmarks'][stream] = {}
        return self.state.get('bookmarks', {}).get(stream, default)

    def update_bookmark(self, stream, value):
        if 'bookmarks' not in self.state:
            self.state['bookmarks'] = {}
        self.state['bookmarks'][stream] = value
        LOGGER.info('Stream: %s - Write state, bookmark value: %s', stream,
                    value)
        self.write_state()

    # Currently syncing sets the stream currently being delivered in the state.
    # If the integration is interrupted, this state property is used to identify
    #  the starting point to continue from.
    # Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
    def update_currently_syncing(self, stream_name):
        if (stream_name is None) and ('currently_syncing' in self.state):
            del self.state['currently_syncing']
        else:
            singer.set_currently_syncing(self.state, self.name)
        singer.write_state(self.state)

    # Returns max date time and preferred key for all replication key data in record
    def max_from_replication_dates(self, record):
        date_times = {
            dt: strptime_to_utc(record[dt])
            for dt in self.valid_replication_keys if record[dt] is not None
        }
        max_key = max(date_times)
        return self.valid_replication_keys[-1], date_times[max_key]

    def get_resources_by_date(self, date=None):
        if date:
            self.filter_params[self.bookmark_field] = strftime(
                date, DATETIME_PARSE)
        return self.client.get_resources(self.version,
                                         self.endpoint,
                                         limit=self.limit,
                                         filter_params=self.filter_params)

    def get_resources(self):
        return self.client.get_resources(self.version,
                                         self.endpoint,
                                         data_location=self.data_location,
                                         limit=self.limit,
                                         filter_params=self.filter_params,
                                         single_page=self.single_page_response)

    @staticmethod
    def transform_record(record):
        return record

    def sync(self, stream_schema, stream_metadata, bookmark=None):
        self.write_schema()
        if self.replication_method == 'INCREMENTAL':
            return self.sync_incremental(stream_schema, stream_metadata,
                                         bookmark)
        return self.sync_full_table(stream_schema, stream_metadata)

    def sync_incremental(self, stream_schema, stream_metadata, bookmark):
        new_bookmark = bookmark
        with metrics.record_counter(endpoint=self.name) as counter:
            with Transformer() as transformer:
                time_extracted = utils.now()
                for page in self.get_resources_by_date(bookmark):
                    for record in page:
                        replication_key, max_time = self.max_from_replication_dates(
                            record)
                        if replication_key not in record:
                            record[replication_key] = max_time
                        new_bookmark = max(new_bookmark, max_time)
                        if max_time > bookmark:
                            with Transformer() as transformer:
                                transformed_record = transformer.transform(
                                    record,
                                    stream_schema,
                                    stream_metadata,
                                )
                                singer.write_record(
                                    self.name,
                                    transformed_record,
                                    time_extracted=time_extracted)
                            counter.increment()
                    self.update_bookmark(
                        self.name, strftime(new_bookmark, DATETIME_PARSE))
            return counter.value

    def sync_full_table(self, stream_schema, stream_metadata):
        with singer.metrics.record_counter(endpoint=self.name) as counter:
            time_extracted = utils.now()
            for page in self.get_resources():
                for record in page:
                    transformed_record = self.transform_record(record)
                    with Transformer() as transformer:
                        singer.write_record(self.name,
                                            transformer.transform(
                                                transformed_record,
                                                stream_schema,
                                                stream_metadata),
                                            time_extracted=time_extracted)
                    counter.increment()
            return counter.value


class Product(BaseStream):
    name = 'product'
    key_properties = ['productId']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['createdDate', 'modifiedDate']
    bookmark_field = 'modifiedDateFrom'
    endpoint = 'products'
    version = 'v1.1'


class ProductStockTotal(BaseStream):
    name = 'product_stock_total'
    key_properties = ['masterSku']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = ['']
    endpoint = 'productstocks/total'
    version = 'v1'

    @staticmethod
    def transform_record(record):
        for key, value in record.get('product', '').items():
            record[key] = value
        del record['product']
        return record


class PurchaseOrder(BaseStream):
    name = 'purchase_order'
    key_properties = ['purchaseOrderId']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['dateCreated', 'modifiedDate']
    bookmark_field = 'modifiedDateFrom'
    endpoint = 'purchaseorders'
    version = 'v1.1'
    limit = 50


class PurchaseOrderFormat(BaseStream):
    name = 'purchase_order_format'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = ['']
    endpoint = 'purchaseorders/formats'
    version = 'v1'
    single_page_response = True


class Inventory(BaseStream):
    name = 'inventory'
    key_properties = ['productStockId']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = ['']
    endpoint = 'inventory'
    version = 'v1'


class IncotermShipRule(BaseStream):
    name = 'incoterm_ship_rule'
    key_properties = ['incotermShippingRuleId']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = ['']
    endpoint = 'incotermshiprules'
    version = 'v1'
    single_page_response = True


class PaymentType(BaseStream):
    name = 'payment_type'
    key_properties = ['orderPaymentTypeId']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = ['']
    endpoint = 'paymenttypes'
    version = 'v1'
    single_page_response = True


class Order(BaseStream):
    name = 'order'
    key_properties = ['orderId']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['createdDate', 'modifiedDate']
    bookmark_field = 'modifiedDateFrom'
    endpoint = 'orders'
    version = 'v1.1'


class StockTransfer(BaseStream):
    name = 'stock_transfer'
    key_properties = ['stockTransferOrderId']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['createdDate']
    bookmark_field = 'createdDateFrom'
    endpoint = 'inventory/stocktransfers'
    version = 'v1'


class Listing(BaseStream):
    name = 'listing'
    key_properties = ['listingId']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['created', 'lastModified']
    bookmark_field = 'modifiedDateFrom'
    endpoint = 'listings'
    version = 'v1'


class Rma(BaseStream):
    name = 'rma'
    key_properties = ['rmaId']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['issueDate']
    bookmark_field = 'createdFromDate'
    endpoint = 'shipments/rmas'
    version = 'v1'
    filter_params = {
        'createdFromDate': None,
        'createdToDate': strftime(utils.now(), DATETIME_PARSE)
    }


class Shipment(BaseStream):
    name = 'shipment'
    key_properties = ['shipmentId']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['created', '']
    bookmark_field = 'shipmentFromDate'
    endpoint = 'shipments'
    version = 'v1'
    limit = 50
    filter_params = {'shipmentFromDate': None, 'shipmentToDate': None}

    def sync(self, stream_schema, stream_metadata, bookmark=None):
        self.write_schema()
        window_start = bookmark
        new_bookmark = bookmark

        now = utils.now()
        window_next = bookmark + timedelta(days=7)

        while window_start <= now:
            with metrics.record_counter(endpoint=self.name) as counter:
                self.filter_params = {
                    'shipmentFromDate': strftime(window_start, DATETIME_PARSE),
                    'shipmentToDate': strftime(window_next, DATETIME_PARSE)
                }

                time_extracted = utils.now()
                for page in self.get_resources_by_date(bookmark):
                    for record in page:
                        record_bookmark = strptime_to_utc(
                            record.get(self.valid_replication_keys[0]))
                        new_bookmark = max(new_bookmark, record_bookmark)
                        if record_bookmark > bookmark:
                            with Transformer() as transformer:
                                transformed_record = transformer.transform(
                                    record,
                                    stream_schema,
                                    stream_metadata,
                                )
                                singer.write_record(
                                    self.name,
                                    transformed_record,
                                    time_extracted=time_extracted)
                            counter.increment()
                    self.update_bookmark(
                        self.name, strftime(new_bookmark, DATETIME_PARSE))
                    window_start = window_start + timedelta(days=7)
                    window_next = bookmark + timedelta(days=7)
                return counter.value


class CustomFieldDefinition(BaseStream):
    name = 'custom_field_definition'
    key_properties = ['customFieldDefId']
    replication_method = 'FULL_TABLE'
    valid_replication_keys = ['']
    endpoint = 'products/customfielddefinitions'
    version = 'v1.1'
    data_location = 'results'


class ShippingPackage(BaseStream):
    name = 'shipping_package'
    key_properties = ['packageTypeId']
    valid_replication_keys = ['']
    replication_method = 'FULL_TABLE'
    endpoint = 'shipment/packages'
    version = 'v1'
    single_page_response = True


class ShippingCarrier(BaseStream):
    name = 'shipping_carrier'
    key_properties = ['shippingCarrierId']
    valid_replication_keys = ['']
    replication_method = 'FULL_TABLE'
    endpoint = 'shipment/carriers'
    version = 'v1'
    single_page_response = True


class ShippingProvider(BaseStream):
    name = 'shipping_provider'
    key_properties = ['shippingProviderId']
    valid_replication_keys = ['']
    replication_method = 'FULL_TABLE'
    endpoint = 'shipment/providers'
    version = 'v1'
    single_page_response = True


class Vendor(BaseStream):
    name = 'vendor'
    key_properties = ['vendorId']
    valid_replication_keys = ['']
    replication_method = 'FULL_TABLE'
    endpoint = 'vendors'
    version = 'v1'


class VendorProduct(BaseStream):
    name = 'vendor_product'
    key_properties = ['vendorProductId']
    valid_replication_keys = ['']
    replication_method = 'FULL_TABLE'
    endpoint = 'vendorproducts'
    version = 'v1'


class AppProps(BaseStream):
    name = 'application_properties'
    key_properties = ['id']
    valid_replication_keys = ['']
    replication_method = 'FULL_TABLE'
    endpoint = 'apps/properties'
    version = 'v1'


class CompanyInfo(BaseStream):
    name = 'company_info'
    key_properties = ['name']
    valid_replication_keys = ['']
    replication_method = 'FULL_TABLE'
    endpoint = 'companyinfo'
    version = 'v1'
    single_page_response = True


class SalesChannel(BaseStream):
    name = 'sales_channel'
    key_properties = ['salesChannelId']
    valid_replication_keys = ['']
    replication_method = 'FULL_TABLE'
    endpoint = 'saleschannels'
    version = 'v1'
    single_page_response = True


class Warehouse(BaseStream):
    name = 'warehouse'
    key_properties = ['warehouseId']
    valid_replication_keys = ['']
    replication_method = 'FULL_TABLE'
    endpoint = 'warehouses'
    version = 'v1'
    single_page_response = True


AVAILABLE_STREAMS = {
    'application_properties': AppProps,
    'company_info': CompanyInfo,
    'custom_field_definition': CustomFieldDefinition,
    'incoterm_ship_rule': IncotermShipRule,
    'inventory': Inventory,
    'listing': Listing,
    'order': Order,
    'payment_type': PaymentType,
    'product': Product,
    'product_stock_total': ProductStockTotal,
    'purchase_order': PurchaseOrder,
    'purchase_order_format': PurchaseOrderFormat,
    'rma': Rma,
    'sales_channel': SalesChannel,
    'shipment': Shipment,
    'shipping_carrier': ShippingCarrier,
    'shipping_package': ShippingPackage,
    'shipping_provider': ShippingProvider,
    'stock_transfer': StockTransfer,
    'vendor': Vendor,
    'vendor_product': VendorProduct,
    'warehouse': Warehouse
}
