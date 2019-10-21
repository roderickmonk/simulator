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


def test_trades_price_depth_1():

    trades = [
        [12345, 1234, 30, 504, True],
        [12345, 1233, 31, 1003, True],
        [12345, 1235, 30, 1003, True],
        [98765, 1235, 31, 1003, True],
        [98765, 1235, 31, 1003, False],
    ]

    config = {
        "envId": 0,
        "exchange": "bittrex",
        "market": "btc-eth",
        "priceDepthStart": 0.0000000001,
        "priceDepthEnd": 1.0,
        "priceDepthSamples": 100,
        "depthStart": 0.01,
        "depthEnd": 100.0,
        "depthSamples": 100,
        "inventoryLimit": 0.2,
    }

    tg = TuningGenerator(config=config)
    tg.load_trades(trades=trades)
    tg.trades_price_depth()

    assert np.array_equal(
        np.around(np.array([[1.0], [1.0], [1.0333333333333332], [1.0], [1.0]]),
                  8), np.around(np.array(tg.trades_price_depths), 8))

    assert np.array_equal(np.array([[0.2, 0.2, 0.2], [0.2], [0.2]]),
                          np.array(tg.trades_volumes))
