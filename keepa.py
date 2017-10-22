import csv
import datetime
import json
import logging
import math
import os
import requests
import time
import urllib.parse

PRODUCT_CSV = {
    'AMAZON': 0,            # Amazon price history
    'NEW': 1,               # Marketplace New price history.
    'USED': 2,              # Marketplace Used price history
    'SALES': 3,             # Sales Rank history. Not every product has a Sales Rank.
    'LISTPRICE': 4,         # List Price history
    'COLLECTIBLE': 5,       # Collectible price history
    'REFURBISHED': 6,       # Refurbished price history
    'NEW_FBM_SHIPPING': 7,  # 3rd party (not including Amazon) New price history including shipping costs, only fulfilled by merchant (FBM).
    'LIGHTNING_DEAL': 8,    # Lightning Deal price history
    'WAREHOUSE': 9,         # Amazon Warehouse price history.
    'NEW_FBA': 10,          # Price history of the lowest 3rd party (not including Amazon/Warehouse) New offer that is fulfilled by Amazon
    'COUNT_NEW': 11,        # New offer count history (= count of marketplace merchants selling the product as new)
    'COUNT_USED': 12,       # Used offer count history
    'COUNT_REFURBISHED': 13,    # Refurbished offer count history
    'COUNT_COLLECTIBLE': 14,    # Collectible offer count history
    'EXTRA_INFO_UPDATES': 15,   # History of past updates to all offers-parameter related data: offers, buyBoxSellerIdHistory, isSNS, isRedirectASIN and the csv types NEW_FBM_SHIPPING, WAREHOUSE, NEW_FBA, RATING, COUNT_REVIEWS, BUY_BOX_SHIPPING, USED_*_SHIPPING, COLLECTIBLE_*_SHIPPING and REFURBISHED_SHIPPING. As updates to those fields are infrequent it is important to know when our system updated them. The absolute value indicates the amount of offers fetched at the given time. If the value is positive it means all available offers were fetched. It's negative if there were more offers than fetched.
    'RATING': 16,               # The product's rating history. A rating is an integer from 0 to 50 (e.g. 45 = 4.5 stars)
    'COUNT_REVIEWS': 17,        # The product's review count history.
    'BUY_BOX_SHIPPING': 18,     # The price history of the buy box. If no offer qualified for the buy box the price has the value -1. Including shipping costs.
    'USED_NEW_SHIPPING': 19,    # "Used - Like New" price history including shipping costs.
    'USED_VERY_GOOD_SHIPPING': 20,          # "Used - Very Good" price history including shipping costs.
    'USED_GOOD_SHIPPING': 21,               # "Used - Good" price history including shipping costs.
    'USED_ACCEPTABLE_SHIPPING': 22,         # "Used - Acceptable" price history including shipping costs.
    'COLLECTIBLE_NEW_SHIPPING': 23,         # "Collectible - Like New" price history including shipping costs.
    'COLLECTIBLE_VERY_GOOD_SHIPPING': 24,   # "Collectible - Very Good" price history including shipping costs.
    'COLLECTIBLE_GOOD_SHIPPING': 25,        # "Collectible - Good" price history including shipping costs.
    'COLLECTIBLE_ACCEPTABLE_SHIPPING': 26,  # "Collectible - Acceptable" price history including shipping costs.
    'REFURBISHED_SHIPPING': 27,             # Refurbished price history including shipping costs.
    'RESERVED1': 28,                        # reserved for future use
    'RESERVED2': 29,                        # reserved for future use
    'TRADE_IN': 30,                         # the trade in price history. Amazon trade-in is not available for every locale.
}
DOMAINS = {
    'com': 1,
    'co.uk': 2,
    'de': 3,
    'fr': 4,
    'co.jp': 5,
    'ca': 6,
    'cn': 7,
    'it': 8,
    'es': 9,
    'in': 10,
    'com.mx': 11,
    'com.br': 12,
}
SELLER_CSV = {
    'RATING': 0,        # The merchant's rating in percent, Integer from 0 to 100.
    'RATING_COUNT': 1,  # The merchant's total rating count, Integer.
}
STATUS_CODES = {
    400: 'Bad Request the request was malformed or could not successfully execute.',
    402: 'The used API key does not grant access.',
    405: 'A request parameter is out of allowed range.',
    429: 'You are out of tokens.',
    500: 'An unexpected error occurred when executing this request.',
}

logger = logging.getLogger(__name__)


def formatted(csv_data, mintime=0):
    """ Tranlaste KeepaTime to timestamp and save as ((timestamp1, value1), ... (timestampN, valueN))."""
    if not csv_data:
        return tuple()
    timestamps = [(t + 21564000)*60 for t in csv_data[0::2]]
    values = csv_data[1::2]
    i = 0
    if mintime:
        for i, timestamp in enumerate(timestamps):
            if timestamp > mintime:
                break
        i = (i - 1) if i else 0
    return list(zip(timestamps[i:], values[i:]))


def gmdate(timestamp):
    """ Return GMT/UTC date(YY.mm.dd) correcponding to timestamp."""
    date = time.strftime('%Y.%m.%d', time.gmtime(timestamp))
    args = [int(v) for v in date.split('.')]
    return datetime.date(*args)


def interpolate(csv_data, func=min):
    """ Interpolate Keepa CSV data (e.g. SalesRank) daily (YY.mm.dd) in GMT/UTC timezone
    returning ((date1, value1), ... (dateN, valueN)).
    :param csv_data: Raw CSV data from Keepa
    :param func: The function which will use to calculate daily value if two or more values are available this day.
    """
    results = []
    data = iter(formatted(csv_data))
    try:
        timestamp1, value1 = next(data)
    except StopIteration:
        return results
    date1 = gmdate(timestamp1)
    date2, value2 = date1, value1
    today = datetime.date.today()
    day = datetime.timedelta(days=1)
    while date1 <= today:
        if date1 == date2:
            values = [value2]
            while True:
                try:
                    timestamp2, value2 = next(data)
                except StopIteration:
                    break
                date2 = gmdate(timestamp2)
                if date1 == date2:
                    values.append(value2)
                else:
                    break
            value1 = func(values)
        results.append((date1, value1))
        date1 = date1 + day
    return results


def save2csv(filepath, rows):
    with open(filepath, 'w', encoding='utf8') as f:
        writer = csv.writer(f, lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
        for row in rows:
            writer.writerow(row)


class KeepaAPI:
    """ How to make Requests: https://keepa.com/#!discuss/t/how-to-make-requests/767 """

    def __init__(self, key, domain=DOMAINS['com']):
        """
        :param key: API access key.
        :param domain: Integer value for the Amazon locale you want to access. Valid values: DOMAINS.values()
        """
        self.key = key
        self.domain = domain
        self.messages = []
        self.response = None

    def best_sellers(self, category, domain=None):
        """
        How to request Besrt Sellers: https://keepa.com/#!discuss/t/request-best-sellers/1298
        :param category: The node id of the category you request the best sellers list for. Example: category=2475895011
        :param domain: Integer value for the Amazon locale you want to access. Valid values: DOMAINS.values()
        :return: The response contains a a best sellers ASIN list of a specific category.
        """
        query = {
            'key': self.key,
            'domain': domain or self.domain,
            'category': str(category),
        }
        return self.request('/bestsellers', query)

    def categories(self, category, parents=0, domain=None):
        """
        How to request Besrt Sellers: https://keepa.com/#!discuss/t/request-best-sellers/1298
        :param category: The node id of the category you request the best sellers list for. Example: category=2475895011
        :param parents: Whether or not to include the category tree for each category: 1 = include, 0 = do not include.
        :param domain: Integer value for the Amazon locale you want to access. Valid values: DOMAINS.values()
        :return: The response contains a categories and, if the parents parameter was 1, a categoryParents field with
            all category objects found on the way to the tree's root.
        """
        if type(category) is str:
            cat_ids = category.split(',')
            assert len(category) <= 10, 'Error: More then 10 category ids.'
        elif type(category) is int:
            cat_ids = [str(category)]
        elif type(category) is list:
            cat_ids = [str(cat_id) for cat_id in category]
        else:
            raise KeepaException('Incorrect category type.')
        query = {
            'key': self.key,
            'domain': domain or self.domain,
            'category': ','.join(cat_ids),
        }
        if parents:
            query['parents'] = parents
        return self.request('/category/', query)

    def products(self, asins, stats=0, update=None, history=False, offers=None, rating=False, domain=None):
        """
        How to request Products: https://keepa.com/#!discuss/t/request-products/110
        :param asins: The list of ASINs of the products you want to request (up to 100).
            Example: asin=['B074DT46QR', 'B074N6XNQR']
        :param stats: If specified the product will have a stats field with quick access to current prices,
            min/max prices and the weighted mean values. Example: stats=180 (the last 180 days).
        :param update: If the product's last update is older than update hours force a refresh. The default value is 1
            hour. Example: update=48 (only trigger an update if the product's last update is older than 48 hours).
        :param history: If "True" the product object will include the "csv" field.
        :param offers: The number of up-to-date marketplace offers to retrieve. It must be between 20 and 100.
        :param rating: If "True" the RATING and COUNT_REVIEWS history will be  included in "csv" field.
        :param domain: Integer value for the Amazon locale you want to access. Valid values: DOMAINS.values()
        :return: List of products. Each product is a dictionary.
        """
        assert type(asins) is list, 'Param "asins" type must be a "list"'
        query = {
            'key': self.key,
            'domain': domain or self.domain,
            'asin': ','.join(asins),
            'history': '1' if history else '0',
            'rating': '1' if rating else '0',
        }
        if update is not None:
            query['update'] = str(update)
        if stats:
            query['stats'] = str(int(stats))
        if offers:
            assert 20 <= offers <= 100, 'Incorrect "offers" range!'
            query['offers'] = str(int(offers))
        return self.request('/product', query)

    def sellers(self, seller_ids, storefront=False, update=None, domain=None):
        """
        How to request seller information: https://keepa.com/#!discuss/t/request-seller-information/790
        :param seller_ids: A list seller ids of the merchants you want to request. Example: ['A2L77EE7U53NWQ']
        :param storefront: If specified the seller object will contain additional information about what items the
            seller is listing on Amazon. Valid values: 0 (false) and 1 (true).
        :param update: If the last live data collection from the Amazon storefront page is older than update hours force
            a new collection. Example: update=48 (only trigger an update if the last storefront collection is older than
            48 hours).
        :param domain: Integer value for the Amazon locale you want to access. Valid values: DOMAINS.values()
        :return: Seller data as a dictionary.
        """
        if storefront and len(seller_ids) > 1:
            raise KeepaException('Seller id batch requests are not allowed when requesting the storefront.')
        query = {
            'key': self.key,
            'domain': domain or self.domain,
            'seller': ','.join(seller_ids),
            'storefront': '1' if storefront else '0',
        }
        if update is not None:
            query['update'] = str(update)
        return self.request('/seller', query)

    def request(self, path, query):
        """ Make request to Keepa API and return data in json format."""
        url = urllib.parse.urlunparse(('https', 'api.keepa.com', path, '', urllib.parse.urlencode(query), ''))
        while True:
            counter = 3
            while True:
                try:
                    self.response = requests.get(url)
                    logger.info('GET {}'.format(url))
                    data = json.loads(self.response.text)
                    break
                except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError) as e:
                    logger.warning('Exception: "{}". counter = "{}"'.format(e, counter))
                    counter -= 1
                    if not counter:
                        raise
                    time.sleep(30)
            if self.response.status_code == 200:
                break
            elif self.response.status_code in STATUS_CODES:
                if self.response.status_code == 429:
                    delay = math.ceil(data['refillIn']/1000)
                    logger.info('Tokens left {}. Sleep {} sec.'.format(data['tokensLeft'], delay))
                    time.sleep(delay)
                else:
                    error = STATUS_CODES[self.response.status_code]
                    logger.error(error)
                    raise KeepaException(error)
            else:
                raise KeepaException('Unknown status code "{}"'.format(self.response.status_code))
        return data

    def token_status(self):
        """
        How to request Token Status: https://keepa.com/#!discuss/t/retrieve-token-status/1305
        :return: The response contains information about the current API access, just like every other API request.
            "refillRate": Integer,  # Number of tokens generated per minute.
            "refillIn": Integer,    # Time in milliseconds until your tokens will be refilled (happens every minute).
            "tokensLeft": Integer   # How many tokens you currently have left.
        """
        return self.request('/token', {'key': self.key})

    def tokens_left(self):
        return int(self.token_status()['tokensLeft'])


class KeepaException(Exception):
    pass
