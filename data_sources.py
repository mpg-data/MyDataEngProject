import json
import requests

from abc import ABC, abstractmethod
from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime
from typing import LiteralString
from requests.structures import CaseInsensitiveDict


# Abstract basic classes
class DataSource(ABC):
    @abstractmethod
    def get_data(self) -> dict:
        pass


class StructuredDataSource(DataSource):
    @abstractmethod
    def _get_schema(self):
        pass


# Concrete classes
class APIDataSource(StructuredDataSource):
    def __init__(self, url: LiteralString):
        self.url = url

    def _get_response(self,
                      query_params: dict = None,
                      timeout=5,
                      stream=False,
                      head_only=False):
        if head_only is False:
            _response = requests.get(self.url,
                                     params=query_params,
                                     timeout=timeout,
                                     stream=stream)
        else:
            _response = requests.head(self.url,
                                      timeout=timeout,
                                      stream=False).headers
        return _response

    def get_data(self, query_params, **kwargs):
        self._response = self._get_response(query_params, **kwargs)
        self.data = self._response.json()


# Dataclass for parameterized input
@dataclass(kw_only=True)
class SoQLQueryParams():
    select: str = "*"
    where: str = None
    order: str = None
    group: str = None
    having: str = None
    limit: int = None
    offset: int = None


class SocrataAPISource(APIDataSource):
    _date_format: LiteralString = r"%a, %d %b %Y %H:%M:%S %Z"

    # Public methods
    def get_data(self,
                 query_params: SoQLQueryParams = None,
                 get_response_details=True,
                 **kwargs):
        # head_only=False because Socrata does not accept HEAD requests
        query_params = self._format_query_params(query_params)
        super().get_data(query_params=query_params,
                         head_only=False,
                         **kwargs)
        self._encoding: str = self._response.encoding
        self._headers: CaseInsensitiveDict = self._response.headers
        self._get_schema()

        self._content_last_modif_date = datetime.strptime(
            self._headers["X-SODA2-Truth-Last-Modified"], self._date_format)

        if get_response_details is True:
            self._elapsed_time: datetime.timedelta = self._response.elapsed
            self._request_date = datetime.strptime(
                self._headers["Date"], self._date_format)
            self._socrata_request_id = self._headers["X-Socrata-RequestId"]
        return self.data

    # Private methods
    def _format_query_params(self, query_params):
        _query_params = asdict(query_params)
        _formatted_query_params = dict()
        for param, value in _query_params.items():
            _formatted_query_params[f"${param}"] = value
        return _formatted_query_params

    def _get_schema(self) -> dict:
        _h = self._headers.copy()
        _h['X-SODA2-Fields'], _h['X-SODA2-Types'] = \
            (json.loads(_h[i])
             for i in ('X-SODA2-Fields', 'X-SODA2-Types'))
        self._schema = dict(zip(_h['X-SODA2-Fields'],
                                _h['X-SODA2-Types']))

    # Read-only attributes
    @property
    def content_last_modif_date(self):
        return self._content_last_modif_date

    @property
    def elapsed_time(self):
        return self._elapsed_time

    @property
    def encoding(self):
        return self._encoding

    @property
    def request_date(self):
        return self._request_date

    @property
    def schema(self):
        return self._schema

    @property
    def socrata_request_id(self):
        return self._socrata_request_id
