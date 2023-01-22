import requests
import json
from requests.adapters import HTTPAdapter
from amo.logon import Tokens
from loguru import logger


class Extract:
    session = requests.Session()

    def __init__(self, tokens, entity):
        access_token = tokens.provide_access()
        header = {"Authorization": "Bearer " + access_token}
        self.session.headers.update(header)
        # And to prevent amo from dropping connection:
        self.session.mount('https://', HTTPAdapter(max_retries=5))
        self.basename = entity.basename
        self.truename = entity.truename        # name for writing and further pr...
        self.amo = tokens.subdomain
        self.url = entity.url


    def write(self, counter, content):
        with open(f'temp_data/{self.amo}/{self.truename}/{counter}.json', 'w',
                  encoding='utf-8') as f:
            json.dump(content, f)


    def parse_page(self, req):
        """All the amo API requests have the same Json schema."""
        return json.loads(req.text)


    def get_page(self, url, counter):
        req = self.session.get(url)
        page = self.parse_page(req)
        try:
            content = page['_embedded'][f'{self.basename}']
        except KeyError:
            logger.critical(page)
        self.write(counter, content)
        try:
            next_page = page['_links']['next']['href']
            return next_page
        except KeyError:
            return None


    def _all(self):
        counter = 1
        next_page = self.get_page(self.url, counter)
        while next_page is not None:
            counter += 1
            next_page = self.get_page(next_page, counter)
        logger.success(f"{counter} entities downloaded!")
