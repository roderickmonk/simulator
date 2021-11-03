#!/usr/bin/env python

import os
import sys
import json
import pymongo
import math
from pymongo import MongoClient
from datetime import datetime
import dateutil.parser
import dateutil.parser as parser
import importlib
import logging
from schema import Schema, And, Use, Optional, SchemaError
from bson.objectid import ObjectId
import operator
import numpy
from sentient_util.matching_engine import MatchingEngine
from copy import copy
from orderbooks import Orderbooks
import numpy as np
from numpy import array
from sentient_util.match_result import MatchResult
import functools
import redis
from pickle import loads, dumps
import json

try:
    profile
except NameError:

    def profile(x):
        return x


def check(conf_schema, conf):
    try:
        conf_schema.validate(conf)
        return True
    except SchemaError:
        return False


def find_trades(trades, filter):

    buy_trades = []
    sell_trades = []

    for trade in trades.find(filter=filter):

        # Corral the buy and sell trades into separate lists
        if trade["buy"]:
            buy_trades.append(trade)
        else:
            sell_trades.append(trade)

        # Sort the buy trades into decreasing order
        buy_trades.sort(key=operator.itemgetter("r"), reverse=True)

        # Sort the sell trades into increasing order
        sell_trades.sort(key=operator.itemgetter("r"), reverse=False)

    return buy_trades, sell_trades


@profile
def run():

    logging.basicConfig(
        format="[%(levelname)-5s] %(message)s", level=logging.INFO, datefmt=""
    )

    logging.debug(f"simulate args: {sys.argv}")

    if len(sys.argv) == 2:
        partition_id = sys.argv[1]
    else:
        assert False, "Usage: simulate <Simulation ObjectId>"

    logging.debug("partition_id: " + str(partition_id))

    assert os.environ["MONGODB"], "MONGODB Not Defined"
    remote_mongo_client = MongoClient(os.environ["MONGODB"])
    sim_db = remote_mongo_client[os.environ["SIMULATOR_DB"]]

    assert os.environ["SIMULATOR_DB"], "SIMULATOR_DB Not Defined"
    sim_db = remote_mongo_client[os.environ["SIMULATOR_DB"]]

    r = redis.Redis(encoding="utf-8", decode_responses=True, db=0)

    # Move the sim:trades to the main db
    sim_trade_list_keys = ":".join([partition_id, "sim:trades"])
    sim_trade_keys = r.lrange(sim_trade_list_keys, 0, -1)

    logging.debug("sim_trade_keys: %r", sim_trade_keys)
    logging.debug("Sim Trades: %d", len(sim_trade_keys))
    logging.debug(
        "Before sim_db.trades.count_documents({}): %d",
        sim_db.trades.count_documents({}),
    )

    sim_trades = []
    for sim_trade_key in sim_trade_keys:

        sim_trade = r.hgetall(sim_trade_key)
        assert "runId" in sim_trade, "Corrupt Sim Trade"

        sim_trade["runId"] = ObjectId(sim_trade["runId"])
        sim_trade["ob"] = ObjectId(sim_trade["ob"])
        sim_trade["s"] = ObjectId(sim_trade["s"])
        sim_trade["p"] = ObjectId(sim_trade["p"])

        sim_trades.append(sim_trade)

    # Cleanup
    for sim_trade_key in sim_trade_keys:
        r.delete(sim_trade_key)

    if len(sim_trades) > 0:
        sim_db.trades.insert_many(sim_trades)

    logging.debug(
        "After sim_db.trades.count_documents({}): %d", sim_db.trades.count_documents({})
    )

    # Cleanup
    r.delete(sim_trade_list_keys)

    # Move the matchings to the main db
    matching_list_keys = ":".join([partition_id, "matchings"])
    matching_keys = r.lrange(matching_list_keys, 0, -1)

    # logging.error ("matching_keys: %r", matching_keys)
    logging.debug("len(matching_keys): %d", len(matching_keys))
    logging.debug(
        "Before sim_db.matchings.count_documents({}): %d",
        sim_db.matchings.count_documents({}),
    )

    matchings = []
    for matching_key in matching_keys:

        matching = r.hgetall(matching_key)

        matching["runId"] = ObjectId(matching["runId"])
        matching["ob"] = ObjectId(matching["ob"])
        matching["s"] = ObjectId(matching["s"])
        matching["p"] = ObjectId(matching["p"])

        # Cleanup as we go
        r.delete(matching_key)

        matchings.append(matching)

    sim_db.matchings.insert_many(matchings)

    # Cleanup as we go
    r.delete(matching_list_keys)

    logging.debug(
        "After sim_db.matchings.count_documents({}): %d",
        sim_db.matchings.count_documents({}),
    )


if __name__ == "__main__":
    run()
