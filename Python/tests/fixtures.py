import os

import pytest
import sim_config as sim_config
from pymongo import MongoClient

buy_side: bool = None


@pytest.fixture()
def buying():
    global buy_side
    buy_side = True


@pytest.fixture()
def selling():
    global buy_side
    buy_side = False


@pytest.fixture()
def load_object_ids():
    sim_config.trades_collection.delete_many({})


@pytest.fixture()
def delete_test_orderbooks():

    remote_mongo_client = MongoClient(os.environ["MONGODB"])

    history_db = remote_mongo_client.history
    orderbooks = history_db.orderbooks

    orderbooks.delete_many({"e": int(99)})
