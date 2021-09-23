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
from operator import itemgetter
from pprint import pprint

import dateutil.parser as parser
import numpy as np
import pytest
import sim_config
from bson.objectid import ObjectId
from matching_engine import MatchingEngine
from pymongo import MongoClient
from schema import And, Optional, Schema, SchemaError, Use
from traders import co1

from fixtures import delete_test_orderbooks


def test_co1_test0():

    try:

        sim_config.partition_config = {
            "_id": "00000000",
            "name": "config-name",
            "quantityLimit": 0.1,
            "inventoryLimit": 0.1,
            "feeRate": 0.002,
            "actualFeeRate": 0.002,
            "allowOrderConflicts": False,
            "tick": 1e-8,
            "pdf": None,
        }

        pdf_x = np.array([0.01, 0.1])
        pdf_y = np.array([0.8, 0.2])

        sim_config.init(sim_config.partition_config)

        trader = co1.Trader(sim_config, pdf_x, pdf_y)

        buyob = np.array([[0.011, 0.05], [0.010, 0.1]])
        sellob = np.array([[0.012, 0.05], [0.013, 0.1]])

        buy_rate, sell_rate = trader.compute_orders(buyob, sellob)

        assert round(buy_rate, sim_config.rate_precision) == 0.01000001
        assert round(sell_rate, sim_config.rate_precision) == 0.01299999

    except StopIteration:
        assert False  # Must not be here


@pytest.mark.skip()
def test_CO1_test0_bench(benchmark):
    benchmark(test_CO1_test0)


def test_co1_test1():

    try:

        sim_config.partition_config = {
            "_id": "00000000",
            "name": "config-name",
            "quantityLimit": 0.1,
            "inventoryLimit": 0.1,
            "feeRate": 0.002,
            "actualFeeRate": 0.002,
            "tick": 1e-6,
            "pdf": "not-used",
            "allowOrderConflicts": False,
        }

        pdf_x = np.array([0.01, 0.1])
        pdf_y = np.array([0.8, 0.2])

        sim_config.init(sim_config.partition_config)

        trader = co1.Trader(sim_config, pdf_x, pdf_y)

        buyob = np.array([[0.011, 0.05], [0.010, 0.1]])
        sellob = np.array([[0.012, 0.05], [0.013, 0.1]])

        buy_rate, sell_rate = trader.compute_orders(buyob, sellob)

        logging.warning("buy_rate: " + str(buy_rate))
        logging.warning("sim_config.rate_precision: " + str(sim_config.rate_precision))

        assert round(buy_rate, sim_config.rate_precision) == 0.010001
        assert round(sell_rate, sim_config.rate_precision) == 0.012999

    except StopIteration:
        assert False  # Must not be here


import redis


def redis_get(r, cycle_time, field):
    raw = r.get(":".join([cycle_time, field]))
    raw = raw[0:-1]  # Remove the dangling comma
    raw = raw.split(",")
    raw = [float(i) for i in raw]
    return np.array([raw])


@pytest.mark.skip(reason="Test Not Routinely Carried Out")
def test_traders_in_real_time():

    try:

        r = redis.Redis(
            host="localhost", port=6379, encoding="utf-8", decode_responses=True, db=0
        )

        bot_id = r.get("testBotId")
        logging.error("bot_id: %s", bot_id)

        bot_config = r.hgetall(bot_id.encode())
        logging.error("bot_config: %r", bot_config)

        allow_order_conflicts = False

        bot_config["quantityLimit"] = float(bot_config["quantityLimit"])
        bot_config["inventoryLimit"] = float(bot_config["quantityLimit"])
        bot_config["feeRate"] = float(bot_config["feeRate"])
        bot_config["tick"] = float(bot_config["tick"])

        tick = bot_config["tick"]

        if bot_config["allowOrderConflicts"] == "true":
            bot_config["allowOrderConflicts"] = True
        else:
            bot_config["allowOrderConflicts"] = False

        sim_config.partition_config = {
            "_id": "00000000",
            "name": "config-name",
            "quantityLimit": bot_config["quantityLimit"],
            "inventoryLimit": bot_config["inventoryLimit"],
            "feeRate": bot_config["feeRate"],
            "actualFeeRate": 0.0027,
            "tick": bot_config["tick"],
            "pdf": "not-used",
            "allowOrderConflicts": bot_config["allowOrderConflicts"],
        }

        logging.error("Trader: %s", bot_config["trader"])
        if "evol_a_cycler" in bot_config["trader"]:
            import evol_a_cycler as trader_under_test
        elif "co1" in bot_config["trader"]:
            import co1 as trader_under_test
        else:
            raise "Unknown Trader"

        p = (
            r.pubsub()
        )  # See https://github.com/andymccurdy/redis-py/#publish--subscribe
        p.psubscribe("*")

        rust_python_same_count = 0
        rust_python_diff_count = 0

        while True:
            # print("Waiting...")
            message = p.get_message()

            if message != None:

                if message["type"] == "pmessage":

                    np.set_printoptions(precision=12)
                    np.set_printoptions(suppress=True)

                    logging.fatal("")

                    rx_msg = json.loads(message["data"])

                    cycle_time, rust_buy_rate, rust_sell_rate = itemgetter(
                        "cycle_time", "buy_rate", "sell_rate"
                    )(rx_msg)

                    logging.error("Cycle Time: %r", cycle_time)
                    # pdf_x
                    sim_config.pdf_x = redis_get(r, cycle_time, "pdf_x")[0]
                    assert sim_config.pdf_x.size > 0

                    logging.debug("sim_config.pdf_x:\n%r", sim_config.pdf_x)

                    # pdf_y
                    sim_config.pdf_y = redis_get(r, cycle_time, "pdf_y")[0]
                    assert sim_config.pdf_y.size > 0

                    assert sim_config.pdf_x.size == sim_config.pdf_y.size

                    logging.debug("sim_config.pdf_y:\n%r", sim_config.pdf_y)

                    sim_config.init(sim_config.partition_config)

                    logging.debug("pdf_x:\n%r", sim_config.pdf_x)
                    logging.debug("pdf_y:\n%r", sim_config.pdf_y)

                    # buyOB
                    buy_rates = redis_get(r, cycle_time, "buy_rates")
                    buy_quantities = redis_get(r, cycle_time, "buy_quantities")

                    assert buy_rates.size == buy_quantities.size

                    buyob = np.vstack((buy_rates, buy_quantities)).T
                    logging.debug("buyob:\n%r", buyob)

                    # sellOB
                    sell_rates = redis_get(r, cycle_time, "sell_rates")
                    sell_quantities = redis_get(r, cycle_time, "sell_quantities")

                    assert sell_rates.size == sell_quantities.size

                    sellob = np.vstack((sell_rates, sell_quantities)).T
                    logging.debug("sellob:\n%r", sellob)

                    # buy_rates
                    buy_rates_ref = np.flip(
                        redis_get(r, cycle_time, "buy_candidate_rates")[0]
                    )
                    assert buy_rates_ref.size > 0

                    # sell_rates
                    sell_rates_ref = np.flip(
                        redis_get(r, cycle_time, "sell_candidate_rates")[0]
                    )
                    assert sell_rates_ref.size > 0

                    # buy_ev
                    buy_ev_ref = np.flip(redis_get(r, cycle_time, "buy_ev")[0])
                    assert buy_ev_ref.size > 0

                    # sell_ev
                    sell_ev_ref = np.flip(redis_get(r, cycle_time, "sell_ev")[0])
                    assert sell_ev_ref.size > 0

                    # buy_pv
                    buy_pv_ref = np.flip(redis_get(r, cycle_time, "buy_pv")[0])
                    assert buy_ev_ref.size > 0

                    # sell_pv
                    sell_pv_ref = np.flip(redis_get(r, cycle_time, "sell_pv")[0])
                    assert sell_pv_ref.size > 0

                    trader = trader_under_test.Trader(sim_config)

                    t1 = datetime.utcnow()
                    buy_rate, sell_rate = trader.compute_orders(buyob, sellob)

                    rates_identical = (
                        trader.buy_rates.size == buy_rates_ref.size
                        and trader.sell_rates.size == sell_rates_ref.size
                        and trader.buy_rates.shape == buy_rates_ref.shape
                        and trader.sell_rates.shape == sell_rates_ref.shape
                        and trader.buy_rates.dtype == buy_rates_ref.dtype
                        and trader.sell_rates.dtype == sell_rates_ref.dtype
                        and np.allclose(
                            trader.buy_rates, buy_rates_ref, atol=0.000000005
                        )
                        and np.allclose(
                            trader.sell_rates, sell_rates_ref, atol=0.000000005
                        )
                    )

                    logging.debug("rates_identical: %r", rates_identical)

                    if not rates_identical:

                        logging.error("local buy_rates: %r", trader.buy_rates)
                        logging.error("remote buy_rates_ref: %r", buy_rates_ref)

                        logging.error("local sell_rates: %r", trader.sell_rates)
                        logging.error("remote sell_rates_ref: %r", sell_rates_ref)

                        os._exit(0)

                    EVs_identical = (
                        trader.buy_ev.size == buy_ev_ref.size
                        and trader.sell_ev.size == sell_ev_ref.size
                        and trader.buy_ev.shape == buy_ev_ref.shape
                        and trader.sell_ev.shape == sell_ev_ref.shape
                        and trader.buy_ev.dtype == buy_ev_ref.dtype
                        and trader.sell_ev.dtype == sell_ev_ref.dtype
                        and np.allclose(trader.buy_ev, buy_ev_ref, atol=0.000000005)
                        and np.allclose(trader.sell_ev, sell_ev_ref, atol=0.000000005)
                    )

                    logging.debug("EVs_identical: %r", EVs_identical)

                    if not EVs_identical:

                        logging.error("local buy_ev: %r", trader.buy_ev)
                        logging.error("remote buy_ev: %r", buy_ev_ref)

                        logging.error("local sell_ev: %r", trader.sell_ev)
                        logging.error("remote sell_ev: %r", sell_ev_ref)

                        ox._exit(0)

                    PVs_identical = (
                        trader.buy_pv.size == buy_pv_ref.size
                        and trader.sell_pv.size == sell_pv_ref.size
                        and trader.buy_pv.shape == buy_pv_ref.shape
                        and trader.sell_pv.shape == sell_pv_ref.shape
                        and trader.buy_pv.dtype == buy_pv_ref.dtype
                        and trader.sell_pv.dtype == sell_pv_ref.dtype
                        and np.allclose(trader.buy_pv, buy_pv_ref, atol=0.000000005)
                        and np.allclose(trader.sell_pv, sell_pv_ref, atol=0.000000005)
                    )

                    logging.debug("PVs_identical: %r", PVs_identical)

                    if not PVs_identical:

                        logging.error("local buy_pv: %r", trader.buy_pv)
                        logging.error("remote buy_pv: %r", buy_pv_ref)

                        logging.error("local sell_pv: %r", trader.sell_pv)
                        logging.error("remote sell_pv: %r", sell_pv_ref)

                        ox._exit(0)

                    output_format = "{0:13}{1:10}, {2:6} {3:10}"
                    logging.error(
                        output_format.format(
                            "Python Buy:",
                            "%.8f" % round(buy_rate, sim_config.rate_precision),
                            "Sell:",
                            "%.8f" % round(sell_rate, sim_config.rate_precision),
                        )
                    )

                    logging.error(
                        output_format.format(
                            "Rust   Buy:",
                            "%.8f" % rust_buy_rate,
                            "Sell:",
                            "%.8f" % rust_sell_rate,
                        )
                    )

                    rust_python_identical = (
                        abs(rust_buy_rate - buy_rate) < bot_config["tick"]
                        and abs(rust_sell_rate - sell_rate) < bot_config["tick"]
                    )

                    if rust_python_identical:
                        rust_python_same_count += 1
                    else:
                        rust_python_diff_count += 1

                    logging.error(
                        f"Rust / Python Same: {rust_python_same_count}, Diff: {rust_python_diff_count}"
                    )

        assert round(buy_rate, sim_config.rate_precision) == -1
        assert round(sell_rate, sim_config.rate_precision) == -1

    except StopIteration:
        assert False  # Must not be here

    except:
        assert False  # Must not be here


@pytest.mark.skip(reason="Test Not Routinely Carried Out")
def test_compare_bots():

    try:

        r = redis.Redis(
            host="localhost", port=6379, encoding="utf-8", decode_responses=True, db=0
        )

        bot_id = r.get("testBotId")
        logging.error("bot_id: %s", bot_id)

        bot_config = r.hgetall(bot_id.encode())
        logging.error("bot_config: %r", bot_config)

        # Float fields need an explicit conversion from string to float
        bot_config["quantityLimit"] = float(bot_config["quantityLimit"])
        bot_config["inventoryLimit"] = float(bot_config["quantityLimit"])
        bot_config["feeRate"] = float(bot_config["feeRate"])
        bot_config["tick"] = float(bot_config["tick"])
        bot_config["priceDepthLimit"] = float(bot_config["priceDepthLimit"])

        tick = bot_config["tick"]

        # Boolean fields need an explicit conversion from string to boolean
        if bot_config["allowOrderConflicts"] == "true":
            bot_config["allowOrderConflicts"] = True
        else:
            bot_config["allowOrderConflicts"] = False

        sim_config.partition_config = {
            "_id": "00000000",
            "name": "config-name",
            "quantityLimit": bot_config["quantityLimit"],
            "inventoryLimit": bot_config["inventoryLimit"],
            "feeRate": bot_config["feeRate"],
            "actualFeeRate": 0.0027,
            "tick": bot_config["tick"],
            "pdf": "not-used",
            "allowOrderConflicts": bot_config["allowOrderConflicts"],
            "priceDepthLimit": bot_config["priceDepthLimit"],
        }

        logging.error("priceDepthLimit: %f", bot_config["priceDepthLimit"])

        logging.error("Trader: %s", bot_config["trader"])
        if "evol_a_cycler" in bot_config["trader"]:
            import evol_a_cycler as trader_under_test
        elif "co1_pdepth1" in bot_config["trader"]:
            import co1_pdepth1 as trader_under_test
        elif "co1" in bot_config["trader"]:
            import co1 as trader_under_test
        else:
            raise "Unknown Trader"

        p = (
            r.pubsub()
        )  # See https://github.com/andymccurdy/redis-py/#publish--subscribe
        p.psubscribe("*")

        rust_python_same_count = 0
        rust_python_diff_count = 0

        while True:
            # print("Waiting...")
            message = p.get_message()

            if message != None:

                if message["type"] == "pmessage":

                    np.set_printoptions(precision=12)
                    np.set_printoptions(suppress=True)

                    logging.fatal("")

                    rx_msg = json.loads(message["data"])

                    cycle_time, rust_buy_rate, rust_sell_rate = itemgetter(
                        "cycle_time", "buy_rate", "sell_rate"
                    )(rx_msg)

                    logging.error("Cycle Time: %r", cycle_time)

                    # pdf_x
                    sim_config.pdf_x = redis_get(r, cycle_time, "pdf_x")[0]
                    assert sim_config.pdf_x.size > 0

                    logging.debug("sim_config.pdf_x:\n%r", sim_config.pdf_x)

                    # pdf_y
                    sim_config.pdf_y = redis_get(r, cycle_time, "pdf_y")[0]
                    assert sim_config.pdf_y.size > 0

                    assert sim_config.pdf_x.size == sim_config.pdf_y.size

                    logging.debug("sim_config.pdf_y:\n%r", sim_config.pdf_y)

                    sim_config.init(sim_config.partition_config)

                    logging.debug("pdf_x:\n%r", sim_config.pdf_x)
                    logging.debug("pdf_y:\n%r", sim_config.pdf_y)

                    # buyOB
                    buy_rates = redis_get(r, cycle_time, "buy_rates")
                    buy_quantities = redis_get(r, cycle_time, "buy_quantities")

                    assert buy_rates.size == buy_quantities.size

                    buyob = np.vstack((buy_rates, buy_quantities)).T
                    logging.debug("buyob:\n%r", buyob)

                    # sellOB
                    sell_rates = redis_get(r, cycle_time, "sell_rates")
                    sell_quantities = redis_get(r, cycle_time, "sell_quantities")

                    assert sell_rates.size == sell_quantities.size

                    sellob = np.vstack((sell_rates, sell_quantities)).T
                    logging.debug("sellob:\n%r", sellob)

                    # buy_rates
                    buy_rates_ref = np.flip(
                        redis_get(r, cycle_time, "buy_candidate_rates")[0]
                    )
                    assert buy_rates_ref.size > 0

                    # sell_rates
                    sell_rates_ref = np.flip(
                        redis_get(r, cycle_time, "sell_candidate_rates")[0]
                    )
                    assert sell_rates_ref.size > 0

                    # buy_ev
                    buy_ev_ref = np.flip(redis_get(r, cycle_time, "buy_ev")[0])
                    assert buy_ev_ref.size > 0

                    # sell_ev
                    sell_ev_ref = np.flip(redis_get(r, cycle_time, "sell_ev")[0])
                    assert sell_ev_ref.size > 0

                    # buy_pv
                    buy_pv_ref = np.flip(redis_get(r, cycle_time, "buy_pv")[0])
                    assert buy_ev_ref.size > 0

                    # sell_pv
                    sell_pv_ref = np.flip(redis_get(r, cycle_time, "sell_pv")[0])
                    assert sell_pv_ref.size > 0

                    trader = trader_under_test.Trader(sim_config)

                    t1 = datetime.utcnow()
                    buy_rate, sell_rate = trader.compute_orders(buyob, sellob)

                    rates_identical = (
                        trader.buy_rates.size == buy_rates_ref.size
                        and trader.sell_rates.size == sell_rates_ref.size
                        and trader.buy_rates.shape == buy_rates_ref.shape
                        and trader.sell_rates.shape == sell_rates_ref.shape
                        and trader.buy_rates.dtype == buy_rates_ref.dtype
                        and trader.sell_rates.dtype == sell_rates_ref.dtype
                        and np.allclose(
                            trader.buy_rates, buy_rates_ref, atol=0.000000005
                        )
                        and np.allclose(
                            trader.sell_rates, sell_rates_ref, atol=0.000000005
                        )
                    )

                    logging.debug("rates_identical: %r", rates_identical)

                    if not rates_identical:

                        logging.error("local buy_rates: %r", trader.buy_rates)
                        logging.error("remote buy_rates_ref: %r", buy_rates_ref)

                        logging.error("local sell_rates: %r", trader.sell_rates)
                        logging.error("remote sell_rates_ref: %r", sell_rates_ref)

                        os._exit(0)

                    EVs_identical = (
                        trader.buy_ev.size == buy_ev_ref.size
                        and trader.sell_ev.size == sell_ev_ref.size
                        and trader.buy_ev.shape == buy_ev_ref.shape
                        and trader.sell_ev.shape == sell_ev_ref.shape
                        and trader.buy_ev.dtype == buy_ev_ref.dtype
                        and trader.sell_ev.dtype == sell_ev_ref.dtype
                        and np.allclose(trader.buy_ev, buy_ev_ref, atol=0.000000005)
                        and np.allclose(trader.sell_ev, sell_ev_ref, atol=0.000000005)
                    )

                    logging.debug("EVs_identical: %r", EVs_identical)

                    if not EVs_identical:

                        logging.error("local buy_ev: %r", trader.buy_ev)
                        logging.error("remote buy_ev: %r", buy_ev_ref)

                        logging.error("local sell_ev: %r", trader.sell_ev)
                        logging.error("remote sell_ev: %r", sell_ev_ref)

                        ox._exit(0)

                    PVs_identical = (
                        trader.buy_pv.size == buy_pv_ref.size
                        and trader.sell_pv.size == sell_pv_ref.size
                        and trader.buy_pv.shape == buy_pv_ref.shape
                        and trader.sell_pv.shape == sell_pv_ref.shape
                        and trader.buy_pv.dtype == buy_pv_ref.dtype
                        and trader.sell_pv.dtype == sell_pv_ref.dtype
                        and np.allclose(trader.buy_pv, buy_pv_ref, atol=0.000000005)
                        and np.allclose(trader.sell_pv, sell_pv_ref, atol=0.000000005)
                    )

                    logging.debug("PVs_identical: %r", PVs_identical)

                    if not PVs_identical:

                        logging.error("local buy_pv: %r", trader.buy_pv)
                        logging.error("remote buy_pv: %r", buy_pv_ref)

                        logging.error("local sell_pv: %r", trader.sell_pv)
                        logging.error("remote sell_pv: %r", sell_pv_ref)

                        os._exit(0)

                    output_format = "{0:13}{1:10}, {2:6} {3:10}"
                    logging.error(
                        output_format.format(
                            "Python Buy:",
                            "%.8f" % round(buy_rate, sim_config.rate_precision),
                            "Sell:",
                            "%.8f" % round(sell_rate, sim_config.rate_precision),
                        )
                    )

                    logging.error(
                        output_format.format(
                            "Rust   Buy:",
                            "%.8f" % rust_buy_rate,
                            "Sell:",
                            "%.8f" % rust_sell_rate,
                        )
                    )

                    rust_python_identical = (
                        rust_buy_rate <= 0
                        and buy_rate <= 0
                        or (
                            abs(rust_buy_rate - buy_rate) < bot_config["tick"]
                            and abs(rust_sell_rate - sell_rate) < bot_config["tick"]
                        )
                    )

                    if rust_python_identical:
                        rust_python_same_count += 1
                    else:
                        rust_python_diff_count += 1

                    logging.error(
                        f"Rust / Python Same: {rust_python_same_count}, Diff: {rust_python_diff_count}"
                    )

        assert round(buy_rate, sim_config.rate_precision) == -1
        assert round(sell_rate, sim_config.rate_precision) == -1

    except StopIteration:
        assert False  # Must not be here

    except:
        assert False  # Must not be here


def test_CO1_BUY_get_pv_and_rates_allow_order_conflicts_false_1():

    try:

        sim_config.partition_config = {
            "_id": "00000000",
            "name": "config-name",
            "quantityLimit": 0.1,
            "inventoryLimit": 0.1,
            "feeRate": 0.002,
            "actualFeeRate": 0.002,
            "tick": 0.01,
            "pdf": "not-used",
            "allowOrderConflicts": False,
        }

        pdf_x = np.array([0.01, 0.1])
        pdf_y = np.array([0.8, 0.2])

        sim_config.init(sim_config.partition_config)

        trader = co1.Trader(sim_config, pdf_x, pdf_y)

        ob = np.array([[0.4, 10], [0.3, 20], [0.2, 30], [0.1, 40]])

        pv, rates = trader.get_pv_and_rates(ob, is_buy=True)

        assert np.array_equal(pv, np.array([0.0, 4.0, 10.0, 16.0]))
        assert np.array_equal(
            np.round(rates, 8),
            np.array([0.41, 0.31, 0.21, 0.11]),
        )

    except:
        assert False


def test_CO1_BUY_get_pv_and_rates_allow_order_conflicts_true_1():

    try:

        sim_config.partition_config = {
            "_id": "00000000",
            "name": "config-name",
            "quantityLimit": 0.1,
            "inventoryLimit": 0.1,
            "feeRate": 0.002,
            "actualFeeRate": 0.002,
            "tick": 0.01,
            "pdf": "not-used",
            "allowOrderConflicts": True,
        }

        pdf_x = np.array([0.01, 0.1])
        pdf_y = np.array([0.8, 0.2])

        sim_config.init(sim_config.partition_config)

        trader = co1.Trader(sim_config, pdf_x, pdf_y)

        ob = np.array([[0.4, 10], [0.3, 20], [0.2, 30], [0.1, 40]])

        pv, rates = trader.get_pv_and_rates(ob, is_buy=True)

        assert np.array_equal(pv, np.array([0.0, 4.0, 10.0, 16.0]))
        assert np.array_equal(
            np.round(rates, 8),
            np.array([0.41, 0.31, 0.21, 0.11]),
        )

    except:
        assert False


def test_CO1_BUY_get_pv_and_rates_allow_order_conflicts_false_2():
    """
    No room for a new order near the top
    """

    try:

        sim_config.partition_config = {
            "_id": "00000000",
            "name": "config-name",
            "quantityLimit": 0.1,
            "inventoryLimit": 0.1,
            "feeRate": 0.002,
            "actualFeeRate": 0.002,
            "tick": 0.1,
            "pdf": "not-used",
            "allowOrderConflicts": False,
        }

        pdf_x = np.array([0.01, 0.1])
        pdf_y = np.array([0.8, 0.2])

        sim_config.init(sim_config.partition_config)

        trader = co1.Trader(sim_config, pdf_x, pdf_y)

        ob = np.array(
            [
                [0.4, 10],
                [0.3, 20],
                [0.2, 30],
                [0.1, 40],
            ]
        )

        pv, rates = trader.get_pv_and_rates(ob, is_buy=True)

        logging.debug("pv: %r", pv)
        logging.debug("rates: %r", rates)

        assert np.array_equal(pv, np.array([0]))
        assert np.array_equal(rates, np.array([0.5]))

    except:
        assert False


def test_CO1_BUY_get_pv_and_rates_allow_order_conflicts_true_2():
    """
    No room for a new order near the top
    """

    sim_config.partition_config = {
        "_id": "00000000",
        "name": "config-name",
        "quantityLimit": 0.1,
        "inventoryLimit": 0.1,
        "feeRate": 0.002,
        "actualFeeRate": 0.002,
        "tick": 0.1,
        "pdf": "not-used",
        "allowOrderConflicts": True,
    }

    pdf_x = np.array([0.01, 0.1])
    pdf_y = np.array([0.8, 0.2])

    sim_config.init(sim_config.partition_config)

    trader = co1.Trader(sim_config, pdf_x, pdf_y)

    ob = np.array(
        [
            [0.4, 10],
            [0.3, 20],
            [0.2, 30],
            [0.1, 40],
        ]
    )

    pv, rates = trader.get_pv_and_rates(ob, is_buy=True)

    logging.debug("pv: %r", pv)
    logging.debug("rates: %r", rates)

    assert np.array_equal(pv, np.array([0.0, 4.0, 10.0, 16.0]))
    assert np.array_equal(rates, np.array([0.5, 0.4, 0.3, 0.2]))


def test_CO1_BUY_get_pv_and_rates_3():
    """
    Only 1 OB entry
    """

    try:

        sim_config.partition_config = {
            "_id": "00000000",
            "name": "config-name",
            "quantityLimit": 0.1,
            "inventoryLimit": 0.1,
            "feeRate": 0.002,
            "actualFeeRate": 0.002,
            "tick": 0.01,
            "pdf": "not-used",
            "allowOrderConflicts": False,
        }

        pdf_x = np.array([0.01, 0.1])
        pdf_y = np.array([0.8, 0.2])

        sim_config.init(sim_config.partition_config)

        trader = co1.Trader(sim_config, pdf_x, pdf_y)

        ob = np.array(
            [
                [0.1, 10],
            ]
        )

        pv, rates = trader.get_pv_and_rates(ob, is_buy=True)

        assert np.array_equal(pv, np.array([0]))
        assert np.array_equal(rates, np.array([0.11]))

    except:
        assert False


def test_CO1_SELL_get_pv_and_rates_1():

    try:

        sim_config.partition_config = {
            "_id": "00000000",
            "name": "config-name",
            "quantityLimit": 0.1,
            "inventoryLimit": 0.1,
            "feeRate": 0.002,
            "actualFeeRate": 0.002,
            "tick": 0.01,
            "pdf": "not-used",
            "allowOrderConflicts": False,
        }

        pdf_x = np.array([0.01, 0.1])
        pdf_y = np.array([0.8, 0.2])

        sim_config.init(sim_config.partition_config)

        trader = co1.Trader(sim_config, pdf_x, pdf_y)

        ob = np.array([[0.2, 10], [0.3, 20], [0.4, 30], [0.5, 40]])

        pv, rates = trader.get_pv_and_rates(ob, is_buy=False)

        assert np.array_equal(pv, np.array([0, 2, 8, 20]))
        assert np.array_equiv(
            np.round(rates, 8),
            np.array([0.19, 0.29, 0.39, 0.49]),
        )

    except:
        assert False


def test_CO1_SELL_get_pv_and_rates_2():
    """
    No room for a new order near the top
    """

    try:

        sim_config.partition_config = {
            "_id": "00000000",
            "name": "config-name",
            "quantityLimit": 0.1,
            "inventoryLimit": 0.1,
            "feeRate": 0.002,
            "actualFeeRate": 0.002,
            "tick": 0.1,
            "pdf": "not-used",
            "allowOrderConflicts": False,
        }

        pdf_x = np.array([0.01, 0.1])
        pdf_y = np.array([0.8, 0.2])

        sim_config.init(sim_config.partition_config)

        trader = co1.Trader(sim_config, pdf_x, pdf_y)

        ob = np.array(
            [
                [0.2, 10],
                [0.3, 20],
                [0.4, 30],
                [0.5, 40],
            ]
        )

        pv, rates = trader.get_pv_and_rates(ob, is_buy=False)

        assert np.array_equal(pv, np.array([0]))
        assert np.array_equal(rates, np.array([0.1]))

    except:
        assert False


def test_CO1_SELL_get_pv_and_rates_3():
    """
    Only 1 OB entry
    """

    try:

        sim_config.partition_config = {
            "_id": "00000000",
            "name": "config-name",
            "quantityLimit": 0.1,
            "inventoryLimit": 0.1,
            "feeRate": 0.002,
            "actualFeeRate": 0.002,
            "tick": 0.01,
            "pdf": "not-used",
            "allowOrderConflicts": False,
        }

        pdf_x = np.array([0.01, 0.1])
        pdf_y = np.array([0.8, 0.2])

        sim_config.init(sim_config.partition_config)

        trader = co1.Trader(sim_config, pdf_x, pdf_y)

        ob = np.array(
            [
                [0.1, 10],
            ]
        )

        pv, rates = trader.get_pv_and_rates(ob, is_buy=False)

        assert np.array_equal(pv, np.array([0]))
        assert np.array_equal(np.around(rates, 8), np.array([0.09]))

    except:
        assert False


@pytest.mark.skip()
def test_CO1_SELL_get_pv_and_rates_3_bench(benchmark):
    benchmark(test_CO1_SELL_get_pv_and_rates_3)


def test_evol_1():
    """ """

    try:

        sim_config.partition_config = {
            "_id": "00000000",
            "name": "config-name",
            "quantityLimit": 0.1,
            "inventoryLimit": 0.1,
            "feeRate": 0.002,
            "actualFeeRate": 0.002,
            "tick": 1e-8,
            "pdf": "not-used",
            "allowOrderConflicts": False,
        }

        pdf_x = np.array([0.1])
        pdf_y = np.array([1])

        sim_config.init(sim_config.partition_config)

        trader = co1.Trader(sim_config, pdf_x, pdf_y)

        pv = np.array([0, 10, 20, 30])

        evol = trader.evol(pv)

        assert np.array_equal(evol, [0.1, 0, 0, 0])

    except:
        assert False


@pytest.mark.skip()
def test_evol1_bench(benchmark):
    benchmark(test_evol_1)


def test_evol_2():
    """ """

    try:

        sim_config.partition_config = {
            "_id": "00000000",
            "name": "config-name",
            "quantityLimit": 0.1,
            "inventoryLimit": 0.1,
            "feeRate": 0.002,
            "actualFeeRate": 0.002,
            "tick": 1e-8,
            "pdf": "not-used",
            "allowOrderConflicts": False,
        }

        pdf_x = np.array([0.1, 1.0])
        pdf_y = np.array([0.9, 0.1])

        sim_config.init(sim_config.partition_config)

        trader = co1.Trader(sim_config, pdf_x, pdf_y)

        pv = np.array([0, 10, 20, 30])

        evol = trader.evol(pv)

        assert np.array_equal(evol, [0.1, 0, 0, 0])

    except StopIteration:
        assert False


@pytest.mark.skip()
def test_evol2_bench(benchmark):
    benchmark(test_evol_2)


def test_evol_3():
    """ """

    try:

        sim_config.partition_config = {
            "_id": "00000000",
            "name": "config-name",
            "quantityLimit": 0.1,
            "inventoryLimit": 0.1,
            "feeRate": 0.002,
            "actualFeeRate": 0.002,
            "tick": 1e-8,
            "pdf": "not-used",
            "allowOrderConflicts": False,
        }

        pdf_x = np.array([0.1, 1.0, 2.0])
        pdf_y = np.array([0.8, 0.1, 0.1])

        sim_config.init(sim_config.partition_config)

        trader = co1.Trader(sim_config, pdf_x, pdf_y)

        pv = np.array([0, 10, 20, 30])
        evol = trader.evol(pv)

        assert np.array_equal(np.around(evol, 8), [0.1, 0, 0, 0])

    except StopIteration:
        assert False


def test_evol_4():
    """ """

    try:

        sim_config.partition_config = {
            "_id": "00000000",
            "name": "config-name",
            "quantityLimit": 0.05,
            "inventoryLimit": 0.1,
            "feeRate": 0.002,
            "actualFeeRate": 0.002,
            "tick": 1e-8,
            "pdf": "not-used",
            "allowOrderConflicts": False,
        }

        pdf_x = np.array([0.1, 1.0, 2.0])
        pdf_y = np.array([0.8, 0.1, 0.1])

        sim_config.init(sim_config.partition_config)

        trader = co1.Trader(sim_config, pdf_x, pdf_y)

        pv = np.array([0, 10, 20, 30])

        evol = trader.evol(pv)

        assert np.array_equal(np.around(evol, 8), [0.05, 0, 0, 0])

    except StopIteration:
        assert False


def test_evol_5():
    """ """

    try:

        sim_config.partition_config = {
            "_id": "00000000",
            "name": "config-name",
            "quantityLimit": 0.05,
            "inventoryLimit": 0.1,
            "feeRate": 0.002,
            "actualFeeRate": 0.002,
            "tick": 1e-8,
            "pdf": "not-used",
            "allowOrderConflicts": False,
        }

        pdf_x = np.array([0.1, 1.0])
        pdf_y = np.array([0.5, 0.5])

        sim_config.init(sim_config.partition_config)

        trader = co1.Trader(sim_config, pdf_x, pdf_y)

        pv = np.array([0, 0.05, 0.10, 0.15])

        evol = trader.evol(pv)

        assert np.array_equal(np.around(evol, 8), [0.05, 0.05, 0.025, 0.025])

    except StopIteration:
        assert False


def test_evol_6():
    """
    Ensure pre-condition failure works

    The shape of pdf_x and pdf_y are not the same
    """

    try:

        sim_config.partition_config = {
            "_id": "00000000",
            "name": "config-name",
            "quantityLimit": 0.05,
            "inventoryLimit": 0.1,
            "feeRate": 0.002,
            "actualFeeRate": 0.002,
            "tick": 1e-8,
            "pdf": "not-used",
            "allowOrderConflicts": False,
        }

        pdf_x = np.array([0.1, 1.0, 2.0])
        pdf_y = np.array([0.5, 0.25])

        sim_config.init(sim_config.partition_config)

        trader = co1.Trader(sim_config, pdf_x, pdf_y)

        pv = np.array([0, 0.05, 0.10, 0.15])

        evol = trader.evol(pv)

        assert np.array_equal(np.around(evol, 8), [0.05, 0.05, 0.025, 0.025])

        assert False

    except StopIteration:
        assert True

    except:
        assert True
