import datetime as DT
import importlib
import json
import logging
import math
import operator
import os
import sys
from copy import copy
from datetime import datetime
from pprint import pprint

from bson.objectid import ObjectId
from pymongo import MongoClient

import dateutil.parser as parser
import pytest
from fixtures import delete_test_orderbooks
from schema import And, Optional, Schema, SchemaError, Use
from simulate import find_trades

assert os.environ["MONGODB"], "MONGODB Not Defined"
remote_mongo_client = MongoClient(os.environ["MONGODB"])

history_db = remote_mongo_client.history


@pytest.mark.skip()
def test_find_trades_bench(benchmark):

    filter = {
        "e": 0,
        "x": "bittrex",
        "m": "btc-xrp",
        "ob": ObjectId("5bb2c21d30446c4fe546aed1"),
    }

    buy_trades, sell_trades = benchmark(
        find_trades,
        history_db.trades,
        filter,
    )

    assert len(buy_trades) == 0
    assert len(sell_trades) == 7


@pytest.mark.skip()
def test_find_no_trades_bench(benchmark):

    filter = {
        "e": 0,
        "x": "bittrex",
        "m": "btc-xrp",
        "ob": ObjectId("5b5268a9f48dc94bde385ce8"),
    }

    buy_trades, sell_trades = benchmark(find_trades, history_db.trades, filter)

    assert len(buy_trades) == 0
    assert len(sell_trades) == 0
