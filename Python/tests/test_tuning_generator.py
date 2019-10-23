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
Remaining Volumes
"""


def test_remaining_volumes_1():

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

    tg.remaining_volumes()

    # trades_volumes
    logging.debug("tg.trades_volumes:\n%r", tg.trades_volumes)
    expected = [[450, 300, 180]]
    assert compare2D(tg.trades_volumes, expected)


def test_remaining_volumes_2():

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

    tg.remaining_volumes()

    # trades_volumes
    logging.debug("tg.trades_volumes:\n%r", tg.trades_volumes)
    expected = [[200, 200, 180]]
    assert compare2D(tg.trades_volumes, expected)


def test_remaining_volumes_3():

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

    tg.remaining_volumes()

    # trades_volumes
    logging.debug("tg.trades_volumes:\n%r", tg.trades_volumes)
    expected = [[450, 300, 180]]
    assert compare2D(tg.trades_volumes, expected)


def test_remaining_volumes_4():

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

    tg.remaining_volumes()

    # trades_volumes
    logging.debug("tg.trades_volumes:\n%r", tg.trades_volumes)
    expected = [[200, 200, 180]]
    assert compare2D(tg.trades_volumes, expected)


"""
Remaining Price Depths
"""


def test_remaining_price_depths_1():

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

    tg.remaining_price_depths()

    # trades_price_depths
    logging.debug("tg.trades_price_depths:\n%r", tg.trades_price_depths)
    expected = [[1.0, 1.0, 2.0]]
    assert compare2D(tg.trades_price_depths, expected)


def test_remaining_price_depths_2():

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

    tg.remaining_price_depths()

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
