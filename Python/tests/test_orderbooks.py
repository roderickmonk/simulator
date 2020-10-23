
import os
import sys
import json
import datetime as DT
import pymongo
import math
from pprint import pprint
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
from matching_engine import MatchingEngine
import sim_config
from copy import copy
from matching_engine import MatchingEngine
from orderbooks import Orderbooks
import pytest
from fixtures import delete_test_orderbooks
import numpy as np

remote_mongo_client = MongoClient(os.environ['MONGODB'])
history_db = remote_mongo_client.history
orderbooks = history_db.orderbooks


buy_ref = [
    [10, 100],
    [9, 101],
    [8, 102],
    [7, 103],
    [6, 104],
]

sell_ref = [
    [11, 100],
    [12, 101],
    [13, 102],
    [14, 103],
    [15, 104],
]


def insert_snapshot():

    orderbooks.insert_one({
        "V": "V",
        "e": int(99),
        "x": "test-exchange",
        "m": "base-quote",
        "ts": dateutil.parser.parse("2025-01-01T00:00:00.000+0000"),
        "N": int(0),
        "s": True,
        "buy": buy_ref,
        "sell": sell_ref,
    })


def insert_delta(*, ts: str, buy: list, sell: list):

    orderbooks.insert_one({
        "V": "V",
        "e": int(99),
        "x": "test-exchange",
        "m": "base-quote",
        "ts": dateutil.parser.parse(ts),
        "N": int(0),
        "s": False,
        "buy": buy,
        "sell": sell,
    })


def test_orderbook_snapshot_found_immediately(delete_test_orderbooks):

    try:

        orderbooks.insert_one({
            "V": "V",
            "e": int(99),
            "x": "test-exchange",
            "m": "base-quote",
            "ts": dateutil.parser.parse("2025-01-01T00:00:00.000+0000"),
            "N": int(0),
            "s": True,
            "buy": buy_ref,
            "sell": sell_ref,
        })

        orderbooks.insert_one({
            "V": "V",
            "e": int(99),
            "x": "test-exchange",
            "m": "base-quote",
            "ts": dateutil.parser.parse("2025-01-01T01:00:00.000+0000"),
            "N": int(0),
            "s": True,
            "buy": buy_ref,
            "sell": sell_ref,
        })

        Orderbooks(
            ob_collection=orderbooks,
            envId=int(99),
            exchange="test-exchange",
            market="base-quote",
            depth=12,
            start=dateutil.parser.parse("2025-01-01T00:00:00.000+0000"),
            end=dateutil.parser.parse("2025-01-01T02:00:00.000+0000"),
        )

    except StopIteration:
        assert False  # Must not be here


def test_orderbook_snapshot_missing(delete_test_orderbooks):

    try:

        Orderbooks(
            ob_collection=orderbooks,
            envId=int(99),
            exchange="test-exchange",
            market="base-quote",
            depth=12,
            start=dateutil.parser.parse("2026-01-01T00:00:00.000+0000"),
            end=dateutil.parser.parse("2026-01-01T00:00:01.000+0000"),
        )

        assert False, "Unexpected Snapshot Found"

    except StopIteration:
        pass


def test_orderbook_snapshot_found_later(delete_test_orderbooks):

    try:
        orderbooks.insert_one({
            "V": "V",
            "e": int(99),
            "x": "test-exchange",
            "m": "base-quote",
            "ts": dateutil.parser.parse("2025-01-01T04:00:00.000+0000"),
            "N": int(0),
            "s": True,
            "buy": buy_ref,
            "sell": sell_ref,
        })

        orderbooks.insert_one({
            "V": "V",
            "e": int(99),
            "x": "test-exchange",
            "m": "base-quote",
            "ts": dateutil.parser.parse("2025-01-01T04:00:00.000+0000"),
            "N": int(0),
            "s": True,
            "buy": buy_ref,
            "sell": sell_ref,
        })

        Orderbooks(
            ob_collection=orderbooks,
            envId=int(99),
            exchange="test-exchange",
            market="base-quote",
            depth=12,
            start=dateutil.parser.parse("2025-01-01T03:00:00.000+0000"),
            end=dateutil.parser.parse("2025-01-01T04:00:01.000+0000"),
        )

    except StopIteration:
        assert False, "No Snapshot Found"


def test_orderbook_snapshot_not_found_within_last_3_hours(delete_test_orderbooks):

    try:

        orderbooks.insert_one({
            "V": "V",
            "e": int(99),
            "x": "test-exchange",
            "m": "base-quote",
            "ts": dateutil.parser.parse("2025-01-01T00:00:00.000+0000"),
            "N": int(0),
            "s": True,
            "buy": buy_ref,
            "sell": sell_ref,
        })

        Orderbooks(
            ob_collection=orderbooks,
            envId=int(99),
            exchange="test-exchange",
            market="base-quote",
            depth=12,
            start=dateutil.parser.parse("2025-01-01T03:00:01.000+0000"),
            end=dateutil.parser.parse("2025-01-01T03:00:02.000+0000"),
        )

        assert false

    except StopIteration:
        pass


def test_orderbook_next_0_deltas(delete_test_orderbooks):

    try:
        orderbooks.insert_one({
            "V": "V",
            "e": int(99),
            "x": "test-exchange",
            "m": "base-quote",
            "ts": dateutil.parser.parse("2025-01-01T00:00:00.000+0000"),
            "N": int(0),
            "s": True,
            "buy": buy_ref,
            "sell": sell_ref,
        })

        orderbooks.insert_one({
            "V": "V",
            "e": int(99),
            "x": "test-exchange",
            "m": "base-quote",
            "ts": dateutil.parser.parse("2025-01-01T00:00:01.000+0000"),
            "N": int(0),
            "s": True,
            "buy": buy_ref,
            "sell": sell_ref,
        })

        OBs = Orderbooks(
            ob_collection=orderbooks,
            envId=int(99),
            exchange="test-exchange",
            market="base-quote",
            depth=12,
            start=dateutil.parser.parse("2025-01-01T00:00:00.000+0000"),
            end=dateutil.parser.parse("2025-01-01T00:00:02.000+0000"),
        )

        OBs.next()

    except StopIteration:
        assert False


def test_orderbook_1_late_delta_then_next(delete_test_orderbooks):

    try:
        orderbooks.insert_one({
            "V": "V",
            "e": int(99),
            "x": "test-exchange",
            "m": "base-quote",
            "ts": dateutil.parser.parse("2025-01-01T01:00:00.000+0000"),
            "N": int(0),
            "s": True,
            "buy": buy_ref,
            "sell": sell_ref,
        })

        # This delta is too late to be considered
        insert_delta(
            ts="2025-01-01T00:00:00.000+0000",
            buy=[[2, 10, 200]],
            sell=[[2, 11, 200]],
        )

        orderbooks.insert_one({
            "V": "V",
            "e": int(99),
            "x": "test-exchange",
            "m": "base-quote",
            "ts": dateutil.parser.parse("2025-01-01T01:01:00.000+0000"),
            "N": int(0),
            "s": True,
            "buy": buy_ref,
            "sell": sell_ref,
        })

        OBs = Orderbooks(
            ob_collection=orderbooks,
            envId=int(99),
            exchange="test-exchange",
            market="base-quote",
            depth=10000,
            start=dateutil.parser.parse("2025-01-01T01:00:00.000+0000"),
            end=dateutil.parser.parse("2025-01-01T02:00:00.000+0000"),
        )

        orderbook = OBs.next()

        assert np.array_equal(orderbook["buy"], np.array(buy_ref))
        assert np.array_equal(orderbook["sell"], np.array(sell_ref))

    except StopIteration:
        assert False


def test_orderbook_1_delta(delete_test_orderbooks):

    buy_delta = [
        [2, 10, 200],
        [2, 9, 201],
        [2, 8, 202],
        [2, 7, 203],
        [2, 6, 204],
    ]

    sell_delta = [
        [2, 11, 200],
        [2, 12, 201],
        [2, 13, 202],
        [2, 14, 203],
        [2, 15, 204],
    ]

    buy_expected = [
        [10, 200],
        [9, 201],
        [8, 202],
        [7, 203],
        [6, 204],
    ]

    sell_expected = [
        [11, 200],
        [12, 201],
        [13, 202],
        [14, 203],
        [15, 204],
    ]

    try:
        insert_snapshot()

        # This delta is too late to be considered
        insert_delta(
            ts="2025-01-01T00:00:01.000+0000",
            buy=buy_delta,
            sell=sell_delta,
        )

        OBs = Orderbooks(
            ob_collection=orderbooks,
            envId=int(99),
            exchange="test-exchange",
            market="base-quote",
            depth=100000,
            start=dateutil.parser.parse("2025-01-01T00:00:00.000+0000"),
            end=dateutil.parser.parse("2025-01-01T01:00:00.000+0000"),
        )

        orderbook = OBs.next()

        assert np.array_equal(orderbook["buy"], np.array(buy_ref))
        assert np.array_equal(orderbook["sell"], np.array(sell_ref))

        orderbook = OBs.next()

        assert np.array_equal(orderbook["buy"], np.array(buy_expected))
        assert np.array_equal(orderbook["sell"], np.array(sell_expected))

    except StopIteration:
        assert False


def test_apply_depth_1():

    orderbook = np.array([
        [10, 100],
        [9, 100],
        [8, 100],
        [7, 100],
        [6, 100],
    ], dtype=float)

    try:
        ob = Orderbooks.apply_depth(2000, orderbook)

        assert np.array_equal(
            ob,
            np.array(
                [
                    [10, 100],
                    [9, 100],
                    [8, 100],
                ]
            )
        )

    except:
        assert False


def test_apply_depth_1():
    # Subset of the OB returned

    orderbook = np.array([
        [10, 100],
        [9, 100],
        [8, 100],
        [7, 100],
        [6, 100],
    ], dtype=float)

    try:
        ob = Orderbooks.apply_depth(2000, orderbook)

        assert np.array_equal(
            ob,
            np.array(
                [
                    [10, 100],
                    [9, 100],
                    [8, 100],
                ], dtype=float
            )
        )

    except:
        assert False


def test_apply_depth_2():

    # First element exactly equals depth

    orderbook = np.array([
        [10, 100],
        [9, 100],
        [8, 100],
        [7, 100],
        [6, 100],
    ], dtype=float)

    try:
        ob = Orderbooks.apply_depth(1000, orderbook)

        assert np.array_equal(
            ob,
            np.array(
                [
                    [10, 100],
                ], dtype=float
            )
        )

    except:
        assert False


def test_apply_depth_3():

    # First element > depth

    orderbook = np.array([
        [10, 100],
        [9, 100],
        [8, 100],
        [7, 100],
        [6, 100],
    ], dtype=float)

    try:
        ob = Orderbooks.apply_depth(999, orderbook)

        assert np.array_equal(
            ob,
            np.array(
                [
                    [10, 100],
                ], dtype=float
            )
        )

    except:
        assert False


def test_apply_depth_4():

    # Very large depth - return the entire OB

    orderbook = np.array([
        [10, 100],
        [9, 100],
        [8, 100],
        [7, 100],
        [6, 100],
    ], dtype=float)

    try:
        ob = Orderbooks.apply_depth(100000, orderbook)

        assert np.array_equal(
            ob,
            np.array(orderbook, dtype=float)
        )

    except:
        assert False
