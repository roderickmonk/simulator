import os
import pytest
import logging
from bson.objectid import ObjectId
from pymongo import MongoClient
import json
import math
import sys
import numpy as np
from tuning_generator import TuningGenerator

# The following is a generic TuningGenerator configuration
config = {
    "priceDepthStart": 0.0000000001,
    "priceDepthEnd": 1.0,
    "priceDepthSamples": 100,
    "depthStart": 0.01,
    "depthEnd": 100.0,
    "depthSamples": 100,
    "inventoryLimit": 200,
}


def compare1D(x, y) -> bool:
    if len(x) != len(y):
        return False
    for x1, y1 in zip(x, y):
        if not math.isclose(x1, y1):
            return False
    return True


def compare2D(x, y) -> bool:
    if len(x) != len(y):
        return False
    for x1, y1 in zip(x, y):
        if len(x1) != len(y1):
            return False
        for x2, y2 in zip(x1, y1):
            if not math.isclose(x2, y2):
                return False
    return True


"""
Generate Depths
"""


def test_load_depths_0():

    config = {
        "priceDepthStart": 0.0000000001,
        "priceDepthEnd": 1.0,
        "priceDepthSamples": 4,
        "depthStart": 0.01,
        "depthEnd": 100.0,
        "depthSamples": 10,
    }

    tg = TuningGenerator(config=config)

    # trades_volumes
    logging.debug("depths:\n%r", tg.depths)
    expected = [
        0.0, 0.01, 0.03162277660168379, 0.1, 0.31622776601683794, 1.0,
        3.1622776601683795, 10.0, 31.622776601683793, 100.0
    ]
    assert compare1D(tg.depths, expected)


"""
Generate Price Depths
"""


def test_load_price_depths_0():

    config = {
        "priceDepthStart": 0.0000000001,
        "priceDepthEnd": 1.0,
        "priceDepthSamples": 12,
        "depthStart": 0.01,
        "depthEnd": 100.0,
        "depthSamples": 10,
    }

    tg = TuningGenerator(config=config)

    # trades_volumes
    logging.debug("price_depths:\n%r", tg.price_depths)
    expected = [
        1.0, 1.0000000001, 1.000000001, 1.00000001, 1.0000001, 1.000001,
        1.00001, 1.0001, 1.001, 1.01, 1.1, 2.0
    ]
    assert compare1D(tg.price_depths, expected)


""" 
Get Total Volume
"""


def test_load_total_volume_0():

    trades = [
        [12345, 1234, 3, 50, False],
        [12345, 1233, 6, 30, False],
        [12345, 1235, 3, 100, False],
        [98765, 1235, 3, 100, False],
        [98766, 1235, 3, 50, True],
    ]

    config = {
        "priceDepthStart": 0.0000000001,
        "priceDepthEnd": 1.0,
        "priceDepthSamples": 3,
        "depthStart": 0.01,
        "depthEnd": 100.0,
        "depthSamples": 4,
        "inventoryLimit": 200,
    }

    tg = TuningGenerator(config=config)
    tg.load_trades(trades=trades)
    tg.trades_price_depth()

    tg.load_remaining_depth()
    tg.load_total_volume()

    logging.error("remaining_depth:\n%r", tg.remaining_depth)
    logging.error("total_volume:\n%r", tg.total_volume)
    expected = 550
    assert math.isclose(tg.total_volume, expected)


"""
Remaining Volumes
"""


def test_load_trades_volumes_1():

    config = {
        "priceDepthStart": 0.0000000001,
        "priceDepthEnd": 1.0,
        "priceDepthSamples": 100,
        "depthStart": 0.01,
        "depthEnd": 100.0,
        "depthSamples": 100,
        "inventoryLimit": math.inf,
    }

    tg = TuningGenerator(config=config)

    tg.meta_trade = [
        [12345, 1234, 3, 50, True],
        [12345, 1233, 6, 30, True],
        [12345, 1235, 3, 40, True],
    ]

    tg.meta_trade.sort(key=lambda x: (x[0], x[4], x[1]))
    tg.meta_trade.sort(key=lambda x: x[2], reverse=not tg.meta_trade[0][4])

    tg.load_trades_volumes()

    # trades_volumes
    logging.debug("tg.trades_volumes:\n%r", tg.trades_volumes)
    expected = [[450, 300, 180]]
    assert compare2D(tg.trades_volumes, expected)


def test_load_trades_volumes_2():

    config = {
        "priceDepthStart": 0.0000000001,
        "priceDepthEnd": 1.0,
        "priceDepthSamples": 100,
        "depthStart": 0.01,
        "depthEnd": 100.0,
        "depthSamples": 100,
        "inventoryLimit": 200,
    }

    tg = TuningGenerator(config=config)

    tg.meta_trade = [
        [12345, 1234, 3, 50, True],
        [12345, 1233, 6, 30, True],
        [12345, 1235, 3, 40, True],
    ]

    tg.meta_trade.sort(key=lambda x: (x[0], x[4], x[1]))
    tg.meta_trade.sort(key=lambda x: x[2], reverse=not tg.meta_trade[0][4])

    tg.load_trades_volumes()

    # trades_volumes
    logging.debug("tg.trades_volumes:\n%r", tg.trades_volumes)
    expected = [[200, 200, 180]]
    assert compare2D(tg.trades_volumes, expected)


def test_load_trades_volumes_3():

    config = {
        "priceDepthStart": 0.0000000001,
        "priceDepthEnd": 1.0,
        "priceDepthSamples": 100,
        "depthStart": 0.01,
        "depthEnd": 100.0,
        "depthSamples": 100,
        "inventoryLimit": math.inf,
    }

    tg = TuningGenerator(config=config)

    tg.meta_trade = [
        [12345, 1234, 3, 50, False],
        [12345, 1233, 6, 30, False],
        [12345, 1235, 3, 40, False],
    ]

    tg.meta_trade.sort(key=lambda x: (x[0], x[4], x[1]))
    tg.meta_trade.sort(key=lambda x: x[2], reverse=tg.meta_trade[0][4])

    tg.load_trades_volumes()

    # trades_volumes
    logging.debug("tg.trades_volumes:\n%r", tg.trades_volumes)
    expected = [[450, 300, 180]]
    assert compare2D(tg.trades_volumes, expected)


def test_load_trades_volumes_4():

    config = {
        "priceDepthStart": 0.0000000001,
        "priceDepthEnd": 1.0,
        "priceDepthSamples": 100,
        "depthStart": 0.01,
        "depthEnd": 100.0,
        "depthSamples": 100,
        "inventoryLimit": 200,
    }

    tg = TuningGenerator(config=config)

    tg.meta_trade = [
        [12345, 1234, 3, 50, False],
        [12345, 1233, 6, 30, False],
        [12345, 1235, 3, 40, False],
    ]

    tg.meta_trade.sort(key=lambda x: (x[0], x[4], x[1]))
    tg.meta_trade.sort(key=lambda x: x[2], reverse=tg.meta_trade[0][4])

    tg.load_trades_volumes()

    # trades_volumes
    logging.debug("tg.trades_volumes:\n%r", tg.trades_volumes)
    expected = [[200, 200, 180]]
    assert compare2D(tg.trades_volumes, expected)


"""
Remaining Price Depths
"""


def test_load_trades_price_depths_1():

    config = {
        "priceDepthStart": 0.0000000001,
        "priceDepthEnd": 1.0,
        "priceDepthSamples": 100,
        "depthStart": 0.01,
        "depthEnd": 100.0,
        "depthSamples": 100,
        "inventoryLimit": 200,
    }

    tg = TuningGenerator(config=config)

    tg.meta_trade = [
        [12345, 1234, 3, 50, True],
        [12345, 1233, 6, 30, True],
        [12345, 1235, 3, 100, True],
    ]

    tg.meta_trade.sort(key=lambda x: (x[0], x[4], x[1]))
    tg.meta_trade.sort(key=lambda x: x[2], reverse=False)

    tg.load_trades_price_depths()

    # trades_price_depths
    logging.debug("tg.trades_price_depths:\n%r", tg.trades_price_depths)
    expected = [[1.0, 1.0, 2.0]]
    assert compare2D(tg.trades_price_depths, expected)


def test_load_trades_price_depths_2():

    config = {
        "priceDepthStart": 0.0000000001,
        "priceDepthEnd": 1.0,
        "priceDepthSamples": 100,
        "depthStart": 0.01,
        "depthEnd": 100.0,
        "depthSamples": 100,
        "inventoryLimit": 200,
    }

    tg = TuningGenerator(config=config)

    tg.meta_trade = [
        [12345, 1234, 3, 50, False],
        [12345, 1233, 6, 30, False],
        [12345, 1235, 3, 100, False],
    ]

    tg.meta_trade.sort(key=lambda x: (x[0], x[4], x[1]))
    tg.meta_trade.sort(key=lambda x: x[2], reverse=True)

    tg.load_trades_price_depths()

    # trades_price_depths
    logging.debug("tg.trades_price_depths:\n%r", tg.trades_price_depths)
    expected = [[1.0, 2.0, 2.0]]
    assert compare2D(tg.trades_price_depths, expected)


"""
Trades Price Depth
"""


def test_trades_price_depth_0():

    trades = [
        [12345, 1234, 3, 50, True],
        [12345, 1233, 6, 30, True],
        [12345, 1235, 3, 100, True],
    ]

    config = {
        "priceDepthStart": 0.0000000001,
        "priceDepthEnd": 1.0,
        "priceDepthSamples": 100,
        "depthStart": 0.01,
        "depthEnd": 100.0,
        "depthSamples": 100,
        "inventoryLimit": math.inf,
    }

    tg = TuningGenerator(config=config)
    tg.load_trades(trades=trades)
    tg.trades_price_depth()

    # trades_volumes
    logging.debug("tg.trades_volumes:\n%r", tg.trades_volumes)
    expected = [[630, 480, 180]]
    assert compare2D(tg.trades_volumes, expected)

    # trades_price_depths
    logging.debug("tg.trades_price_depths:\n%r", tg.trades_price_depths)
    expected = [[1.0, 1.0, 2.0]]
    assert compare2D(tg.trades_price_depths, expected)


def test_trades_price_depth_1():

    trades = [
        [12345, 1234, 3, 50, True],
        [12345, 1233, 6, 30, True],
        [12345, 1235, 3, 100, True],
    ]

    config = {
        "priceDepthStart": 0.0000000001,
        "priceDepthEnd": 1.0,
        "priceDepthSamples": 100,
        "depthStart": 0.01,
        "depthEnd": 100.0,
        "depthSamples": 100,
        "inventoryLimit": 200,
    }

    tg = TuningGenerator(config=config)
    tg.load_trades(trades=trades)
    tg.trades_price_depth()

    # trades_volumes
    logging.debug("tg.trades_volumes:\n%r", tg.trades_volumes)
    expected = [[200, 200, 180]]
    assert compare2D(tg.trades_volumes, expected)

    # trades_price_depths
    logging.debug("tg.trades_price_depths:\n%r", tg.trades_price_depths)
    expected = [[1.0, 1.0, 2.0]]
    assert compare2D(tg.trades_price_depths, expected)


def test_trades_price_depth_2():

    trades = [
        [12345, 1234, 3, 50, False],
        [12345, 1233, 6, 30, False],
        [12345, 1235, 3, 100, False],
        [98765, 1235, 3, 100, False],
        [98766, 1235, 3, 50, True],
    ]

    config = {
        "priceDepthStart": 0.0000000001,
        "priceDepthEnd": 1.0,
        "priceDepthSamples": 100,
        "depthStart": 0.01,
        "depthEnd": 100.0,
        "depthSamples": 100,
        "inventoryLimit": math.inf,
    }

    tg = TuningGenerator(config=config)
    tg.load_trades(trades=trades)
    tg.trades_price_depth()

    # trades_volumes
    logging.debug("tg.trades_volumes:\n%r", tg.trades_volumes)
    expected = [[630, 450, 300], [300], [150]]
    assert compare2D(tg.trades_volumes, expected)

    # trades_price_depths
    logging.debug("tg.trades_price_depths:\n%r", tg.trades_price_depths)
    expected = [[1.0, 2.0, 2.0], [1.0], [1.0]]
    assert compare2D(tg.trades_price_depths, expected)


def test_trades_price_depth_3():

    trades = [
        [12345, 1234, 3, 50, False],
        [12345, 1233, 6, 30, False],
        [12345, 1235, 3, 100, False],
        [98765, 1235, 3, 100, False],
        [98766, 1235, 3, 50, True],
    ]

    config = {
        "priceDepthStart": 0.0000000001,
        "priceDepthEnd": 1.0,
        "priceDepthSamples": 100,
        "depthStart": 0.01,
        "depthEnd": 100.0,
        "depthSamples": 100,
        "inventoryLimit": 200,
    }

    tg = TuningGenerator(config=config)
    tg.load_trades(trades=trades)
    tg.trades_price_depth()

    # trades_volumes
    logging.debug("tg.trades_volumes:\n%r", tg.trades_volumes)
    expected = [[200, 200, 200], [200], [150]]
    assert compare2D(tg.trades_volumes, expected)

    # trades_price_depths
    logging.debug("tg.trades_price_depths:\n%r", tg.trades_price_depths)
    expected = [[1.0, 2.0, 2.0], [1.0], [1.0]]
    assert compare2D(tg.trades_price_depths, expected)


def test_trades_price_depth_4():

    trades = [
        [12345, 1234, 3, 50, True],
        [12345, 1233, 6, 30, True],
        [12345, 1235, 3, 100, True],
        [98765, 1235, 3, 100, True],
        [98766, 1235, 3, 50, False],
    ]

    config = {
        "priceDepthStart": 0.0000000001,
        "priceDepthEnd": 1.0,
        "priceDepthSamples": 100,
        "depthStart": 0.01,
        "depthEnd": 100.0,
        "depthSamples": 100,
        "inventoryLimit": math.inf,
    }

    tg = TuningGenerator(config=config)
    tg.load_trades(trades=trades)
    tg.trades_price_depth()

    # trades_volumes
    logging.debug("tg.trades_volumes:\n%r", tg.trades_volumes)
    expected = [[630, 480, 180], [300], [150]]
    assert compare2D(tg.trades_volumes, expected)

    # trades_price_depths
    logging.debug("tg.trades_price_depths:\n%r", tg.trades_price_depths)
    expected = [[1.0, 1.0, 2.0], [1.0], [1.0]]
    assert compare2D(tg.trades_price_depths, expected)


def test_trades_price_depth_5():

    trades = [
        [12345, 1234, 3, 50, True],
        [12345, 1233, 6, 30, True],
        [12345, 1235, 3, 100, True],
        [98765, 1235, 3, 100, True],
        [98766, 1235, 3, 50, False],
    ]

    config = {
        "priceDepthStart": 0.0000000001,
        "priceDepthEnd": 1.0,
        "priceDepthSamples": 100,
        "depthStart": 0.01,
        "depthEnd": 100.0,
        "depthSamples": 100,
        "inventoryLimit": 200,
    }

    tg = TuningGenerator(config=config)
    tg.load_trades(trades=trades)
    tg.trades_price_depth()

    # trades_volumes
    logging.debug("tg.trades_volumes:\n%r", tg.trades_volumes)
    expected = [[200, 200, 180], [200], [150]]
    assert compare2D(tg.trades_volumes, expected)

    # trades_price_depths
    logging.debug("tg.trades_price_depths:\n%r", tg.trades_price_depths)
    expected = [[1.0, 1.0, 2.0], [1.0], [1.0]]
    assert compare2D(tg.trades_price_depths, expected)


def test_load_remaining_depth_0():

    trades = [
        [12345, 1234, 3, 50, True],
        [12345, 1233, 6, 30, True],
        [12345, 1235, 3, 100, True],
        [98765, 1235, 3, 100, True],
        [98766, 1235, 3, 50, False],
    ]

    config = {
        "priceDepthStart": 0.0000000001,
        "priceDepthEnd": 1.0,
        "priceDepthSamples": 4,
        "depthStart": 0.01,
        "depthEnd": 100.0,
        "depthSamples": 3,
        "inventoryLimit": math.inf,
    }

    tg = TuningGenerator(config=config)
    tg.load_trades(trades=trades)
    tg.trades_price_depth()

    tg.load_remaining_depth()

    # trades_volumes
    logging.debug("tg.remaining_depth:\n%r", tg.remaining_depth)
    expected = [[630.0, 300.0, 150.0], [629.99, 299.99, 149.99],
                [530.0, 200.0, 50.0]]
    assert compare2D(tg.remaining_depth, expected)


def test_load_remaining_depth_1():

    trades = [
        [12345, 1234, 3, 50, True],
        [12345, 1233, 6, 30, True],
        [12345, 1235, 3, 100, True],
        [98765, 1235, 3, 100, True],
        [98766, 1235, 3, 50, False],
    ]

    config = {
        "priceDepthStart": 0.0000000001,
        "priceDepthEnd": 1.0,
        "priceDepthSamples": 4,
        "depthStart": 0.01,
        "depthEnd": 100.0,
        "depthSamples": 3,
        "inventoryLimit": 200,
    }

    tg = TuningGenerator(config=config)
    tg.load_trades(trades=trades)
    tg.trades_price_depth()

    tg.load_remaining_depth()

    # trades_volumes
    logging.debug("tg.remaining_depth:\n%r", tg.remaining_depth)
    expected = [[200.0, 200.0, 150.0], [199.99, 199.99, 149.99],
                [100.0, 100.0, 50.0]]
    assert compare2D(tg.remaining_depth, expected)
