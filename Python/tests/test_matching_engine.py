import os
from bson.objectid import ObjectId
import sim_config
from fixtures import load_object_ids, buying, selling
import logging
import math
from matching_engine import MatchingEngine
from pymongo import MongoClient
from random import randint
import numpy as np
import pytest

remote_mongo_client = MongoClient(os.environ['MONGODB'])
sim_db = remote_mongo_client.sim_dev
sim_config.trades_collection = sim_db.trades


def make_trade(r: float, q: float = None):

    if q == None:
        q = 0.01 / r

    return dict({"_id": ObjectId(), "r": r, "q": q, })


def test_many_obs_0_trades_each():

    start_funds = 5.0
    start_inventory = 50.0

    QL = 0.01
    IL = math.inf
    actual_fee_rate = 0.0027

    cycles = 100

    buy_rate = 0.25
    sell_rate = 0.25

    matching_engine = MatchingEngine(

        assets=np.array([start_funds, start_inventory]),
        QL=0.01,
        IL=math.inf,
        actual_fee_rate=actual_fee_rate,
        trades_collection=sim_db.trades,
    )

    for i in range(cycles):

        sim_config.orderbook_id = ObjectId()

        matching_engine.match(
            buy_rate=buy_rate,
            sell_rate=sell_rate,
            buy_trades=[],
            sell_trades=[],
        )

    # Ensure correct funds * inventory
    funds, inventory = matching_engine.assets
    assert math.isclose(funds, start_funds,)
    assert math.isclose(inventory, start_inventory,)


@pytest.mark.skip()
def test_bench(benchmark, load_object_ids):
    benchmark(test_many_obs_0_trades_each)


def test_track_multiple_buys(load_object_ids):

    start_funds = 5.0
    start_inventory = 50.0

    QL = 0.01
    IL = math.inf
    actual_fee_rate = 0.0027

    cycles = 100

    buy_rate = 0.25
    sell_rate = 0.25

    matching_engine = MatchingEngine(

        assets=np.array([start_funds, start_inventory]),
        QL=0.01,
        IL=math.inf,
        actual_fee_rate=actual_fee_rate,
        trades_collection=sim_db.trades,
    )

    for i in range(cycles):

        sim_config.orderbook_id = ObjectId()

        matching_engine.match(
            buy_rate=buy_rate,
            sell_rate=sell_rate,
            sell_trades=[make_trade(0.25)] * 1,
            buy_trades=[],
        )

    funds, inventory = matching_engine.assets

    # Ensure correct funds
    assert math.isclose(
        funds,
        start_funds - cycles * QL,
    )

    # Ensure correct inventory
    assert math.isclose(
        inventory,
        start_inventory
        + cycles *
        (make_trade(0.25)["q"] * (1 - actual_fee_rate)),
    )


def test_track_multiple_sells(load_object_ids):

    start_funds = 5.0
    start_inventory = 50.0

    QL = 0.01
    IL = math.inf
    actual_fee_rate = 0.0027

    cycles = 100

    buy_rate = 0.25
    sell_rate = 0.25

    matching_engine = MatchingEngine(

        assets=np.array([start_funds, start_inventory]),
        QL=0.01,
        IL=math.inf,
        actual_fee_rate=actual_fee_rate,
        trades_collection=sim_db.trades,
    )

    for i in range(cycles):

        sim_config.orderbook_id = ObjectId()

        matching_engine.match(
            buy_rate=buy_rate,
            sell_rate=sell_rate,
            sell_trades=[],
            buy_trades=[make_trade(0.25)] * 1,
        )

    funds, inventory = matching_engine.assets

    # Ensure the correct funds
    assert math.isclose(
        funds,
        start_funds + (cycles * QL) * (1 - actual_fee_rate),
    )

    # Ensure the correct inventory
    assert math.isclose(
        inventory,
        start_inventory - cycles * make_trade(0.25)["q"],
    )


def test_track_multiple_buys_and_sells(load_object_ids):

    start_funds = 5.0
    start_inventory = 50.0

    QL = 0.01
    IL = math.inf
    actual_fee_rate = 0.0027

    cycles = 100

    buy_rate = 0.25
    sell_rate = 0.25

    matching_engine = MatchingEngine(

        assets=np.array([start_funds, start_inventory]),
        QL=QL,
        IL=IL,
        actual_fee_rate=actual_fee_rate,
        trades_collection=sim_db.trades,
    )

    for i in range(cycles):

        sim_config.orderbook_id = ObjectId()

        matching_engine.match(
            buy_rate=buy_rate,
            sell_rate=sell_rate,
            buy_trades=[make_trade(0.25)] * 1,
            sell_trades=[make_trade(0.25)] * 1,
        )

    funds, inventory = matching_engine.assets

    # Ensure correct funds
    assert math.isclose(
        funds,
        start_funds + (cycles * QL) * (1 - actual_fee_rate) - cycles * QL,
    )

    # Ensure correct inventory
    assert math.isclose(
        inventory,
        start_inventory
        + cycles * (make_trade(0.25)["q"] * (1 - actual_fee_rate))
        - cycles * make_trade(0.25)["q"],
    )


def test_track_multiple_buys_and_sells_hi_QL(load_object_ids):

    start_funds = 5.0
    start_inventory = 50.0

    QL = 0.02
    IL = math.inf
    actual_fee_rate = 0.0027

    cycles = 100

    buy_rate = 0.25
    sell_rate = 0.25

    matching_engine = MatchingEngine(

        assets=np.array([start_funds, start_inventory]),
        QL=QL,
        IL=IL,
        actual_fee_rate=actual_fee_rate,
        trades_collection=sim_db.trades,
    )

    for i in range(cycles):

        sim_config.orderbook_id = ObjectId()

        buy_trades = []
        for i in range(2):
            buy_trades.append(make_trade(0.25))

        sell_trades = []
        for i in range(2):
            sell_trades.append(make_trade(0.25))

        matching_engine.match(
            buy_rate=buy_rate,
            sell_rate=sell_rate,
            buy_trades=buy_trades,
            sell_trades=sell_trades,
        )

    funds, inventory = matching_engine.assets

    # Ensure correct funds
    assert math.isclose(
        funds,
        start_funds + cycles * QL * (1 - actual_fee_rate) - cycles * QL,
    )

    # Ensure correct inventory
    assert math.isclose(
        inventory,
        start_inventory
        + 2 * cycles * (make_trade(0.25)["q"] * (1 - actual_fee_rate))
        - 2 * cycles * make_trade(0.25)["q"],
    )


def test_track_multiple_buys_and_sells_hi_QL_extra_trades(load_object_ids):

    start_funds = 5.0
    start_inventory = 50.0

    QL = 0.02
    IL = math.inf
    actual_fee_rate = 0.0027

    cycles = 100

    buy_rate = 0.25
    sell_rate = 0.25

    matching_engine = MatchingEngine(

        assets=np.array([start_funds, start_inventory]),
        QL=QL,
        IL=IL,
        actual_fee_rate=actual_fee_rate,
        trades_collection=sim_db.trades,
    )

    for i in range(cycles):

        sim_config.orderbook_id = ObjectId()

        buy_trades = []
        for i in range(2):
            buy_trades.append(make_trade(0.25))

        sell_trades = []
        for i in range(2):
            sell_trades.append(make_trade(0.25))

        matching_engine.match(
            buy_rate=buy_rate,
            sell_rate=sell_rate,
            buy_trades=buy_trades,
            sell_trades=sell_trades,
        )

    funds, inventory = matching_engine.assets

    assert math.isclose(
        funds,
        start_funds + cycles * QL * (1 - actual_fee_rate) - cycles * QL,
    )

    # Ensure correct inventory
    assert math.isclose(
        inventory,
        start_inventory
        + 2 * cycles * (make_trade(0.25)["q"] * (1 - actual_fee_rate))
        - 2 * cycles * make_trade(0.25)["q"],
    )


def test_IL_equals_0(load_object_ids):
    """ Blocks buying
    """
    start_funds = 5.0
    start_inventory = 50.0

    QL = 0.01
    IL = 0
    actual_fee_rate = 0.0027

    cycles = 100

    buy_rate = 0.25
    sell_rate = 0.25

    trades_per_cycle = 1

    matching_engine = MatchingEngine(

        assets=np.array([start_funds, start_inventory]),
        QL=QL,
        IL=IL,
        actual_fee_rate=actual_fee_rate,
        trades_collection=sim_db.trades,
    )

    for i in range(cycles):

        sim_config.orderbook_id = ObjectId()

        buy_trades = []
        for i in range(trades_per_cycle):
            buy_trades.append(make_trade(0.25))

        sell_trades = []
        for i in range(trades_per_cycle):
            sell_trades.append(make_trade(0.25))

        matching_engine.match(
            buy_rate=buy_rate,
            sell_rate=sell_rate,
            buy_trades=buy_trades,
            sell_trades=sell_trades,
        )

    funds, inventory = matching_engine.assets

    assert math.isclose(
        funds,
        start_funds + (cycles * QL) * (1 - actual_fee_rate),
    )

    assert math.isclose(
        inventory,
        start_inventory - cycles * make_trade(0.25)["q"],
    )


def test_IL_equals_0_sell_everthing(load_object_ids):
    """ Blocks buying, sell everything
    """
    start_funds = 5.0
    start_inventory = 1.0

    QL = 0.01
    IL = 0
    actual_fee_rate = 0.0027
    cycles = 100

    rate = 0.25
    buy_rate = rate
    sell_rate = rate

    trades_per_cycle = 1

    matching_engine = MatchingEngine(

        assets=np.array([start_funds, start_inventory]),
        QL=QL,
        IL=IL,
        actual_fee_rate=actual_fee_rate,
        trades_collection=sim_db.trades,
    )

    # Monitor the depleting inventory
    while matching_engine.assets[1] > 0:

        sim_config.orderbook_id = ObjectId()

        buy_trades = []
        for i in range(trades_per_cycle):
            buy_trades.append(make_trade(rate))

        sell_trades = []
        for i in range(trades_per_cycle):
            sell_trades.append(make_trade(rate))

        matching_engine.match(
            buy_rate=buy_rate,
            sell_rate=sell_rate,
            buy_trades=buy_trades,
            sell_trades=sell_trades,
        )

    funds, inventory = matching_engine.assets

    assert math.isclose(
        funds,
        start_funds + (25 * QL) * (1 - actual_fee_rate),
    )

    assert inventory == 0

import pytest
@pytest.mark.skip(reason="ToDo")
def test_toggle(load_object_ids):
    """
    If cycle count is odd, then inventory must be 0
    otherwise the end inventory must be the start inventory
    """

    for cycle in [1, 2]:

        logging.debug ('cycle: ' + str(cycle))

        """ Toggle between buying and selling
        """
        start_inventory = 0.01

        QL = 0.01
        IL = QL
        actual_fee_rate = 0.0

        rate = 1.00
        buy_rate = rate
        sell_rate = rate

        trades_per_cycle = 1

        matching_engine = MatchingEngine(
            assets=np.array([math.inf, start_inventory]),
            QL=QL,
            IL=IL,
            actual_fee_rate=actual_fee_rate,
            trades_collection=sim_db.trades,
        )

        for i in range(cycle):

            sim_config.orderbook_id = ObjectId()

            buy_trades = []
            for i in range(trades_per_cycle):
                buy_trades.append(make_trade(rate))

            sell_trades = []
            for i in range(trades_per_cycle):
                sell_trades.append(make_trade(rate))

            matching_engine.match(
                buy_rate=buy_rate,
                sell_rate=sell_rate,
                buy_trades=buy_trades,
                sell_trades=sell_trades,
            )

            _, inventory = matching_engine.assets

            logging.debug('inventory: %f', inventory)

        _, inventory = matching_engine.assets

        if cycle % 2 == 0:
            assert math.isclose(inventory, inventory)
        else:
            assert math.isclose(inventory, 0)
