import urllib

import backoff
import requests
import singer
#pylint: disable=import-error
from ratelimit import RateLimitException, limits, sleep_and_retry
from urllib3.exceptions import ProtocolError

BACKOFF_MAX_TRIES = 10
BACKOFF_FACTOR = 2
BASE_URL = "https://api.skubana.com"
LOGGER = singer.get_logger()  # noqa
LIMIT = 100


class Server5xxError(Exception):
    pass


class Server42xRateLimitError(Exception):
    pass


class SkubanaError(Exception):
    pass


class SkubanaBadRequestError(SkubanaError):
    pass


def lookup_backoff_max_tries():
    return BACKOFF_MAX_TRIES


def lookup_backoff_factor():
    return BACKOFF_FACTOR


class SkubanaClient:
    version = "v1"

    def __init__(self, config):
        self.config = config
        self.session = requests.Session()
        self.access_token = config.get('token')

    @staticmethod
    def build_url(version, path, args):
        url_parts = list(urllib.parse.urlparse(BASE_URL))
        url_parts[2] = version + '/' + path
        url_parts[4] = urllib.parse.urlencode(args)
        return urllib.parse.urlunparse(url_parts)

    def get_resources(self,
                      version,
                      path,
                      limit=None,
                      data_location=None,
                      filter_params=None,
                      single_page=False):
        page = 1
        args = {'page': page, 'limit': limit}
        if filter_params:
            args = {**args, **filter_params}
        next_url = self.build_url(version, path, args)

        rows_in_response = 1
        while rows_in_response > 0:
            response = self.make_request(method='GET', url=next_url)

            if isinstance(response, dict):
                response = [response]

            if data_location:
                response = response[0].get(data_location)

            rows_in_response = len(response)

            if single_page:
                rows_in_response = 0
            else:
                rows_in_response = len(response)

            page = page + 1
            args['page'] = page
            next_url = self.build_url(version, path, args)

            if response:
                yield response
            yield []

    @backoff.on_exception(
        backoff.expo,
        (Server5xxError, ConnectionError, Server42xRateLimitError),
        max_tries=lookup_backoff_max_tries,
        factor=lookup_backoff_factor)
    @sleep_and_retry
    @limits(calls=1, period=5)
    def make_request(self, method, url=None, params=None):

        headers = {'Authorization': 'Bearer {}'.format(self.access_token)}

        if self.config.get('user_agent'):
            headers['User-Agent'] = self.config['user_agent']

        try:
            if method == "GET":
                LOGGER.info("Making %s request to %s with params: %s", method,
                            url, params)
                response = self.session.get(url, headers=headers)
            else:
                raise Exception("Unsupported HTTP method")
        except (ConnectionError, ProtocolError) as ex:
            LOGGER.info("Retrying on connection error %s", ex)
            raise ConnectionError from ex

        LOGGER.info("Received code: %s", response.status_code)

        if response.status_code == 429:
            retry_after = int(
                response.headers.get('x-skubana-quota-retryAfter', 15))
            message = 'Rate limit hit - 429 - retrying after {} seconds'.format(
                retry_after)
            LOGGER.info(message)
            raise RateLimitException(message, retry_after)

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code > 299:
            LOGGER.error('Error status_code: %s', response.status_code)
            raise SkubanaBadRequestError(response.reason)

        result = []
        try:
            result = response.json()
        except ConnectionError:
            LOGGER.error("Response json parse failed: %s", response)

        return result
