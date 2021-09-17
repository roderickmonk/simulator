import os
import pytest
import sim_config
import logging
from bson.objectid import ObjectId
from pymongo import MongoClient
from matching_engine import MatchingEngine
import json
import math
import sys
from fixtures import load_object_ids, buying, selling
import fixtures
import numpy as np
from match_result import MatchResult

remote_mongo_client = MongoClient(os.environ["MONGODB"])
sim_db = remote_mongo_client.sim_dev
sim_config.trades_collection = sim_db.trades

matching_engine = MatchingEngine(
    QL=0.02,
    IL=0.02,
    assets=np.array([math.inf, 0]),
    actual_fee_rate=0.0027,
    min_notional=0.0005,
    trades_collection=sim_db.trades,
)


def conduct_buy_test(
    buy_rate: float,
    sell_rate: float,
    expected_trade_count: int,
    sell_trades: [dict],
    start_assets: np.array,
    first_used_trade: int = 0,
):

    assert not sim_config.sim_id == None

    matching_engine.buy(
        start_assets=start_assets,
        buy_rate=buy_rate,
        sell_rate=sell_rate,
        sell_trades=sell_trades,
    )

    sim_trade_checker(
        buy_side=True,
        expected_trade_count=expected_trade_count,
        trades=sell_trades,
        first_used_trade=first_used_trade,
    )


def conduct_sell_test(
    sell_rate: float,
    expected_trade_count: int,
    buy_trades: [dict],
    start_assets: np.array = matching_engine.assets,
    first_used_trade: int = 0,
):

    assert not sim_config.sim_id == None

    matching_engine.sell(
        start_assets=start_assets,
        sell_rate=sell_rate,
        buy_trades=buy_trades,
    )

    sim_trade_checker(
        buy_side=False,
        expected_trade_count=expected_trade_count,
        trades=buy_trades,
        first_used_trade=first_used_trade,
    )


def conduct_buy_low_ceiling_test(
    buy_rate: float,
    sell_rate: float,
    expected_trade_count: int,
    sell_trades: [],
):

    assert not sim_config.sim_id == None

    matching_engine.buy(
        start_assets=np.array([math.inf, 0]),
        buy_rate=buy_rate,
        sell_rate=sell_rate,
        sell_trades=sell_trades,
    )

    sim_trades = list(
        sim_config.trades_collection.find(
            {"o": sim_config.orderbook_id}, no_cursor_timeout=True
        )
    )

    assert len(sim_trades) == expected_trade_count


def conduct_sell_low_ceiling_test(
    sell_rate: float,
    expected_trade_count: int,
    buy_trades: [],
    start_assets: np.array,
):

    assert not sim_config.sim_id == None

    matching_engine.sell(
        start_assets=start_assets,
        sell_rate=sell_rate,
        buy_trades=buy_trades,
    )

    sim_trades = list(
        sim_config.trades_collection.find(
            {"o": sim_config.orderbook_id}, no_cursor_timeout=True
        )
    )

    assert len(sim_trades) == expected_trade_count


def sim_trade_checker(
    buy_side: bool,
    expected_trade_count: int,
    trades: [],
    first_used_trade: int = 0,
):

    trades = trades[first_used_trade:]

    sim_trades = list(
        sim_config.trades_collection.find(
            {"o": sim_config.orderbook_id}, no_cursor_timeout=True
        )
    )

    logging.debug(f"sim_trades: {sim_trades}")
    logging.debug(f"trades: {trades}")

    assert len(sim_trades) == expected_trade_count

    for sim_trade in sim_trades:

        if buy_side:

            fee = sim_trade["q"] * matching_engine.actual_fee_rate
            b = sim_trade["q"] * sim_trade["r"]

            logging.debug(f"buy fee: {round(fee,8)}, " f"buy b: {round(b,8)}")

            assert math.isclose(sim_trade["buyFee"], fee)

        else:

            fee = abs(sim_trade["q"]) * sim_trade["r"] * matching_engine.actual_fee_rate
            b = abs(sim_trade["q"]) * sim_trade["r"]

            logging.debug(f"sell fee: {round(fee,8)}, " f"sell b: {round(b,8)}")
            assert math.isclose(sim_trade["sellFee"], fee, abs_tol=1e-7)

        assert sim_trade["s"] == sim_config.sim_id
        assert sim_trade["p"] == sim_config.partition_config["_id"]
        assert sim_trade["o"] == sim_config.orderbook_id
        assert math.isclose(abs(sim_trade["b"]), b, abs_tol=1e-7)


def test_buy_0_from_0_trades(buying, load_object_ids):

    trade_Qs = [0.02 / 0.20] * 0

    trades = []
    for i in range(len(trade_Qs)):
        trades.append(
            {
                "_id": ObjectId(),
                "r": 0.20,
                "q": trade_Qs[i],
            }
        )

    conduct_buy_test(
        buy_rate=0.25,
        sell_rate=0.26,
        expected_trade_count=0,
        start_assets=matching_engine.assets,
        sell_trades=trades,
    )


def buy_n_from_m(
    expected_trade_count: int,
    trade_count: int,
):
    QL = 0.02
    r = 0.20
    actual_fee_rate = 0.0027
    init_funds = math.inf
    init_inventory = 0

    matching_engine.actual_fee_rate = actual_fee_rate
    matching_engine.QL = QL
    matching_engine.IL = QL
    matching_engine.assets = np.array([init_funds, init_inventory], dtype=float)

    trades = []
    for i in range(trade_count):
        trades.append(
            {
                "_id": ObjectId(),
                "r": r,
                "q": QL / (2 * r),
            }
        )

    expected_base = -QL
    expected_inventory = (QL / r) * (1 - actual_fee_rate)

    conduct_buy_test(
        start_assets=matching_engine.assets,
        buy_rate=r,
        sell_rate=r + 0.0001,
        expected_trade_count=expected_trade_count,
        sell_trades=trades,
    )


def sell_n_from_m(
    expected_trade_count: int,
    trade_count: int,
):
    quantity = 0.02
    r = 0.30
    matching_engine.actual_fee_rate = 0.0027

    trades = []
    for i in range(trade_count):
        trades.append(
            {
                "_id": ObjectId(),
                "r": r,
                "q": quantity / (2 * r),
            }
        )

    fee = quantity * matching_engine.actual_fee_rate
    expected_base = quantity - fee
    expected_inventory = -quantity / r

    conduct_sell_test(
        sell_rate=r,
        expected_trade_count=expected_trade_count,
        buy_trades=trades,
    )


def test_sell_0_from_0_trades(selling, load_object_ids):

    trade_Qs = [0.01] * 0

    trades = []
    for i in range(len(trade_Qs)):
        trades.append(
            {
                "_id": ObjectId(),
                "r": 0.30,
                "q": trade_Qs[i],
            }
        )

    conduct_sell_test(
        sell_rate=0.20,
        expected_trade_count=0,
        buy_trades=trades,
    )


def test_buy_1_from_1_trades(buying, load_object_ids):

    trade_count = 1
    buy_rate = 0.25
    quantity = 0.01
    matching_engine.actual_fee_rate = 0.0027

    trades = []
    r = 0.20
    q = round(quantity / 0.20, 8)
    b = round(r * q, 8)

    for i in range(trade_count):
        trades.append({"_id": ObjectId(), "r": r, "q": q, "b": b})

    logging.debug("trades:\n" + str(trades))

    expected_funds = -quantity
    expected_inventory = (quantity / buy_rate) * (1 - matching_engine.actual_fee_rate)

    conduct_buy_test(
        buy_rate=buy_rate,
        sell_rate=buy_rate + 1,
        expected_trade_count=1,
        start_assets=matching_engine.assets,
        sell_trades=trades,
    )

    funds, inventory = matching_engine.assets

    # assert expected_funds == funds
    # assert expected_inventory = inventory


def test_sell_1_from_1_trades(selling, load_object_ids):

    init_funds = 10
    init_inventory = 10
    QL = 0.005
    IL = QL

    matching_engine.QL = QL
    matching_engine.IL = QL
    matching_engine.actual_fee_rate = 0.0027
    matching_engine.assets = np.array([init_funds, init_inventory], dtype=float)

    trade_count = 1
    sell_rate = 0.20

    # Create trades
    r = sell_rate * 1.1  # 10% bigger
    q = (QL / sell_rate) * 1.1  # 10% bigger

    trades = []
    for i in range(trade_count):
        trades.append({"_id": ObjectId(), "r": r, "q": q, "b": r * q})

    fee = QL * matching_engine.actual_fee_rate

    logging.debug(
        "min (QL, trade[q] * rate): %r ",
        min(QL, trades[0]["q"] * sell_rate),
    )

    conduct_sell_test(
        start_assets=matching_engine.assets,
        sell_rate=sell_rate,
        expected_trade_count=1,
        buy_trades=trades,
        first_used_trade=0,
    )

    funds, inventory = matching_engine.assets
    logging.debug("assets: %r", matching_engine.assets)
    logging.debug("expected funds: %r", init_funds + QL - fee)
    logging.debug("expected inventory: %r", init_inventory - QL / sell_rate)

    assert math.isclose(funds, init_funds + QL - fee)
    assert math.isclose(inventory, init_inventory - QL / sell_rate)


def test_buy_2_from_2_trades(buying, load_object_ids):

    buy_n_from_m(
        expected_trade_count=2,
        trade_count=2,
    )


def test_sell_2_from_2_trades(selling, load_object_ids):

    sell_n_from_m(
        expected_trade_count=2,
        trade_count=2,
    )


def test_buy_2_from_3_trades(buying, load_object_ids):

    buy_n_from_m(
        expected_trade_count=2,
        trade_count=3,
    )


def test_sell_2_from_3_trades(selling, load_object_ids):

    sell_n_from_m(
        expected_trade_count=2,
        trade_count=3,
    )


def test_buy_2_from_many_trades(buying, load_object_ids):

    buy_n_from_m(
        expected_trade_count=2,
        trade_count=100,
    )


def test_sell_2_from_many_trades(selling, load_object_ids):

    sell_n_from_m(
        expected_trade_count=2,
        trade_count=10,
    )


def test_sell_3_from_10_trades_low_ceiling(selling, load_object_ids):

    QL = 0.02
    r = 0.3
    start_inventory = 10

    matching_engine.QL = QL
    matching_engine.actual_fee_rate = 0.0027
    matching_engine.assets = np.array([math.inf, start_inventory], dtype=float)

    trades = []
    for i in range(10):
        trades.append(
            {
                "_id": ObjectId(),
                "r": r,
                "q": QL / (2 * r),
            }
        )

    conduct_sell_low_ceiling_test(
        sell_rate=0.25,
        start_assets=matching_engine.assets,
        expected_trade_count=3,
        # quantity=quantity/4,
        buy_trades=trades,
    )


def test_buy_3_from_100_trades(buying, load_object_ids):

    QL = 0.01
    IL = QL

    matching_engine.QL = QL
    matching_engine.IL = QL
    matching_engine.actual_fee_rate = 0.0
    matching_engine.assets = np.array([math.inf, IL - 3 * 0.04], dtype=float)

    trades = []
    trade_Qs = [0.01] * 100
    for i in range(len(trade_Qs)):
        trades.append(
            {
                "_id": ObjectId(),
                "r": 0.20,
                "q": trade_Qs[i],
            }
        )

    conduct_buy_low_ceiling_test(
        expected_trade_count=4,
        sell_trades=trades,
        buy_rate=0.25,
        sell_rate=0.26,
    )


def test_sell_3_from_100_trades_zero_ceiling(selling, load_object_ids):

    start_inventory = 10
    matching_engine.QL = 0.01
    matching_engine.IL = 0.01
    matching_engine.actual_fee_rate = 0.0027
    matching_engine.assets = np.array([math.inf, start_inventory], dtype=float)

    trades = []
    trade_Qs = [0.01] * 100

    for i in range(len(trade_Qs)):
        trades.append(
            {
                "_id": ObjectId(),
                "r": 0.30,
                "q": trade_Qs[i],
            }
        )

    conduct_sell_low_ceiling_test(
        sell_rate=0.25,
        start_assets=matching_engine.assets,
        expected_trade_count=4,
        buy_trades=trades,
    )


def test_first_buy_trades_rate_too_high(buying, load_object_ids):

    trades = []
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.15,
            "q": 0.01,
        }
    )
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.20,
            "q": 0.01,
        }
    )
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.25,
            "q": 0.01,
        }
    )
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.26,
            "q": 0.01,
        }
    )
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.27,
            "q": 0.01,
        }
    )

    expected_base = 0
    expected_inventory = 0
    matching_engine.actual_fee_rate = 0.0027
    matching_engine.assets = np.array([math.inf, 0], dtype=float)

    first_used_trade = 2

    for trade in trades[first_used_trade:]:
        expected_base -= trade["q"] * trade["r"]
        expected_inventory += trade["q"] * (1 - matching_engine.actual_fee_rate)

        logging.debug(
            f"expected_base: {expected_base}, "
            f"expected_inventory: {expected_inventory}"
        )

    conduct_buy_test(
        buy_rate=0.25,
        sell_rate=0.26,
        expected_trade_count=3,
        sell_trades=trades,
        first_used_trade=first_used_trade,
        start_assets=matching_engine.assets,
    )


def test_first_sell_trades_rate_too_low(selling, load_object_ids):

    matching_engine.QL = 0.1
    matching_engine.actual_fee_rate = 0.0027

    trades = []
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.23,
            "q": 0.01,
        }
    )
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.24,
            "q": 0.01,
        }
    )
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.24999,
            "q": 0.01,
        }
    )
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.25,
            "q": 0.01,
        }
    )
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.26,
            "q": 0.01,
        }
    )
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.27,
            "q": 0.01,
        }
    )
    expected_base = 0
    expected_inventory = 0

    first_used_trade = 3

    for trade in trades[first_used_trade:]:
        expected_base += trade["q"] * trade["r"] * (1 - matching_engine.actual_fee_rate)
        expected_inventory -= trade["q"]
        logging.debug(
            f"expected_base: {expected_base}, "
            f"expected_inventory: {expected_inventory}"
        )

    conduct_sell_test(
        expected_trade_count=3,
        sell_rate=0.25,
        buy_trades=trades,
        first_used_trade=first_used_trade,
    )


def test_buy_all_trade_rates_too_low(buying, load_object_ids):

    matching_engine.assets = np.array([math.inf, 0], dtype=float)

    trades = []
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.29,
            "q": 0.01,
        }
    )
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.27,
            "q": 0.01,
        }
    )
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.28,
            "q": 0.01,
        }
    )
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.26,
            "q": 0.01,
        }
    )
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.25,
            "q": 0.01,
        }
    )

    conduct_buy_test(
        buy_rate=0.24,
        sell_rate=0.25,
        expected_trade_count=0,
        sell_trades=trades,
        start_assets=matching_engine.assets,
    )


def test_sell_all_trade_rates_too_high(selling, load_object_ids):

    trades = []
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.21,
            "q": 0.01,
        }
    )
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.22,
            "q": 0.01,
        }
    )
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.23,
            "q": 0.01,
        }
    )
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.24,
            "q": 0.01,
        }
    )
    trades.append(
        {
            "_id": ObjectId(),
            "r": 0.249999,
            "q": 0.01,
        }
    )

    conduct_sell_test(
        sell_rate=1.0,
        expected_trade_count=0,
        buy_trades=trades,
    )


def test_buy_side_no_trades(
    buying,
    load_object_ids,
):

    sell_trades = []

    result = matching_engine.buy(
        start_assets=matching_engine.assets,
        buy_rate=1.0,
        sell_rate=2.0,
        sell_trades=sell_trades,
    )

    assert result == MatchResult.NO_TRADES


def test_buy_side_blocked_no_ceiling(
    buying,
    load_object_ids,
):

    matching_engine.QL = 0.01
    matching_engine.IL = 0.01

    sell_trades = []
    sell_trades.append(
        {
            "_id": ObjectId(),
            "r": 1.0,
            "q": 0.01,
        }
    )

    result = matching_engine.buy(
        start_assets=np.array([math.inf, 0.01]),
        buy_rate=1.0,
        sell_rate=2.0,
        sell_trades=sell_trades,
    )

    assert result == MatchResult.BLOCKED


def test_buy_side_blocked_no_funds(
    buying,
    load_object_ids,
):

    matching_engine.QL = 0.01
    matching_engine.IL = 0.01

    sell_trades = []
    sell_trades.append(
        {
            "_id": ObjectId(),
            "r": 1.0,
            "q": 0.01,
        }
    )

    result = matching_engine.buy(
        start_assets=np.array([0, 0]),
        buy_rate=1.0,
        sell_rate=2.0,
        sell_trades=sell_trades,
    )

    assert result == MatchResult.BLOCKED


def test_buy_side_unmatchable(
    buying,
    load_object_ids,
):

    buy_rate = 1.0

    sell_trades = []
    # Set the minimum rate slightly larger than the buy_rate
    sell_trades.append(
        {
            "_id": ObjectId(),
            "r": (buy_rate * 1.00001),
            "q": 0.01,
        }
    )

    result = matching_engine.buy(
        start_assets=np.array([math.inf, 0]),
        buy_rate=buy_rate,
        sell_rate=buy_rate + 1,
        sell_trades=sell_trades,
    )

    assert result == MatchResult.UNMATCHABLE


def test_buy_side_min_notional_failure(
    buying,
    load_object_ids,
):

    matching_engine.min_notional = 0.011

    sell_trades = []
    sell_trades.append(
        {
            "_id": ObjectId(),
            "r": 1.0,
            "q": 0.01,
        }
    )

    result = matching_engine.buy(
        start_assets=np.array([math.inf, 0]),
        buy_rate=1.0,
        sell_rate=2.0,
        sell_trades=sell_trades,
    )

    assert result == MatchResult.MIN_NOTIONAL_FAILURE


def test_buy_side_matched(
    buying,
    load_object_ids,
):

    matching_engine.min_notional = 0.01

    sell_trades = []
    sell_trades.append(
        {
            "_id": ObjectId(),
            "r": 1.0,
            "q": 0.01,
        }
    )

    result = matching_engine.buy(
        start_assets=np.array([math.inf, 0]),
        buy_rate=1.0,
        sell_rate=2.0,
        sell_trades=sell_trades,
    )

    assert result == MatchResult.MATCHED


def test_sell_side_no_trades(
    buying,
    load_object_ids,
):

    buy_trades = []

    result = matching_engine.sell(
        start_assets=matching_engine.assets,
        sell_rate=2.0,
        buy_trades=buy_trades,
    )

    assert result == MatchResult.NO_TRADES


def test_sell_side_blocked_no_inventory(
    buying,
    load_object_ids,
):

    matching_engine.QL = 0.01
    matching_engine.IL = 0.01

    buy_trades = []
    buy_trades.append(
        {
            "_id": ObjectId(),
            "r": 1.0,
            "q": 0.01,
        }
    )

    result = matching_engine.sell(
        start_assets=np.array([0, 0]),
        sell_rate=2.0,
        buy_trades=buy_trades,
    )

    assert result == MatchResult.BLOCKED


def test_sell_side_unmatchable(
    buying,
    load_object_ids,
):

    sell_rate = 1.0

    buy_trades = []
    # Set the minimum buy rate slightly larger than the sell rate
    buy_trades.append(
        {
            "_id": ObjectId(),
            "r": sell_rate * 0.99999,
            "q": 0.01,
        }
    )

    result = matching_engine.sell(
        start_assets=np.array([math.inf, 0.1]),
        sell_rate=sell_rate,
        buy_trades=buy_trades,
    )

    assert result == MatchResult.UNMATCHABLE


def test_sell_side_min_notional_failure(
    buying,
    load_object_ids,
):

    matching_engine.min_notional = 0.011

    buy_trades = []
    buy_trades.append(
        {
            "_id": ObjectId(),
            "r": 1.0,
            "q": 0.01,
        }
    )

    result = matching_engine.sell(
        start_assets=np.array([math.inf, 0.01]),
        sell_rate=1.0,
        buy_trades=buy_trades,
    )

    assert result == MatchResult.MIN_NOTIONAL_FAILURE


def test_sell_side_matched(
    buying,
    load_object_ids,
):

    matching_engine.min_notional = 0.01

    buy_trades = []
    buy_trades.append(
        {
            "_id": ObjectId(),
            "r": 1.0,
            "q": 0.01,
        }
    )

    result = matching_engine.sell(
        start_assets=np.array([math.inf, 0.01]),
        sell_rate=1.0,
        buy_trades=buy_trades,
    )

    assert result == MatchResult.MATCHED
