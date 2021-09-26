import os
import pytest
import common_sentient.sim_config as sim_config
from bson.objectid import ObjectId
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
    sim_config.sim_id = ObjectId()
    sim_config.partition_config["runId"] = ObjectId()
    sim_config.partition_config["simId"] = sim_config.sim_id
    sim_config.partition_config["_id"] = ObjectId()
    sim_config.partition_config["simVersion"] = "testing"
    sim_config.orderbook_id = ObjectId()
    # sim_config.quantity_precision = 8
    sim_config.trades_collection.delete_many({})

@pytest.fixture()
def delete_test_orderbooks():

    remote_mongo_client = MongoClient(os.environ['MONGODB'])

    history_db = remote_mongo_client.history
    orderbooks = history_db.orderbooks

    orderbooks.delete_many({"e": int(99)})


