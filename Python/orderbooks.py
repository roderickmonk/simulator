import logging
import os
from datetime import datetime, timedelta
from functools import reduce

import numpy as np
import pymongo
from pymongo.collection import Collection


class Orderbooks:
    def __init__(self, **kwargs):

        self.ob_collection = kwargs["ob_collection"]
        self.envId = kwargs["envId"]
        self.exchange = kwargs["exchange"]
        self.market = kwargs["market"]
        self.depth = kwargs["depth"]
        self.start = kwargs["startTime"]
        self.end = kwargs["endTime"]

        assert self.start < self.end

        self.buy_orderbook = []
        self.sell_orderbook = []

        self.corrupt_order_book_count = 0

        start_snapshot = self.get_start_snapshot()

        self.buy_orderbook = start_snapshot["buy"]
        self.sell_orderbook = start_snapshot["sell"]

        # Ensure both buy and sell OBs are sorted
        self.buy_orderbook.sort(reverse=True)
        self.sell_orderbook.sort(reverse=False)

        last_orderbook = self.get_last_orderbook()

        if __debug__:
            logging.debug("start_snapshot: %r", start_snapshot)
            logging.debug("actual_end: %r", last_orderbook["ts"])

        # Setup an iterator to drive the simulation
        self.iter = iter(
            self.ob_collection.find(
                filter={
                    "e": self.envId,
                    "x": self.exchange,
                    "m": self.market,
                    "ts": {"$gte": start_snapshot["ts"], "$lt": self.end},
                    "s": {"$exists": True},
                    "V": "V",  # valid flag
                },
                no_cursor_timeout=True,
            )
        )

    """
            ).sort([
                ("e", pymongo.ASCENDING),
                ("x", pymongo.ASCENDING),
                ("m", pymongo.ASCENDING),
                ("ts", pymongo.ASCENDING),
            ])
    """

    def __str__(self):
        return (
            str(self.__class__)
            + "\n"
            + "\n".join(
                "{} = {}".format(item, self.__dict__[item]) for item in self.__dict__
            )
        )

    def __iter__(self):
        return self

    def get_start_snapshot(self):

        earlier_snapshots = list(
            self.ob_collection.find(
                filter={
                    "e": self.envId,
                    "x": self.exchange,
                    "m": self.market,
                    # Look for a snapshot within the last 3 hours
                    "ts": {
                        "$lte": self.start,
                        "$gte": self.start - timedelta(milliseconds=3 * 3600000),
                    },
                    "s": True,
                },
                no_cursor_timeout=True,
            )
        )

        if len(earlier_snapshots) > 0:

            # Work with the most recent one
            start_snapshot = reduce(
                lambda x, y: x if x["ts"] > y["ts"] else y,
                earlier_snapshots,
            )

        else:

            # Nothing earlier; look for the next snapshot
            snapshots = list(
                self.ob_collection.find(
                    filter={
                        "e": self.envId,
                        "x": self.exchange,
                        "m": self.market,
                        "ts": {
                            "$gte": self.start,
                        },
                        "s": True,
                    },
                    no_cursor_timeout=True,
                )
                .sort("ts", 1)
                .limit(1)
            )
            if len(snapshots) == 0:
                raise StopIteration
            else:
                start_snapshot = snapshots[0]

        return start_snapshot

    def get_first_orderbook(self):

        # Nothing earlier; look for the next snapshot
        orderbooks = list(
            self.ob_collection.find(
                filter={
                    "e": self.envId,
                    "x": self.exchange,
                    "m": self.market,
                    "ts": {
                        "$gte": self.start,
                    },
                },
                no_cursor_timeout=True,
            )
            .sort("ts", 1)
            .limit(1)
        )
        if len(orderbooks) == 0:
            return None
        else:
            return orderbooks[0]

    def get_last_orderbook(self):

        # Nothing earlier; look for the next snapshot
        orderbooks = list(
            self.ob_collection.find(
                filter={
                    "e": self.envId,
                    "x": self.exchange,
                    "m": self.market,
                    "ts": {
                        "$gt": self.start,
                        "$lt": self.end,
                    },
                },
                no_cursor_timeout=True,
            )
            .sort("ts", -1)
            .limit(1)
        )

        if len(orderbooks) == 0:
            return None
        else:
            return orderbooks[0]

    def count_orderbooks(self):

        return self.ob_collection.count_documents(
            filter={
                "e": self.envId,
                "x": self.exchange,
                "m": self.market,
                "ts": {
                    "$gte": self.start,
                    "$lt": self.end,
                },
                "V": "V",
                "s": {"$exists": True},
            }
        )

    def count_corrupt_orderbooks(self):

        return self.ob_collection.count_documents(
            filter={
                "e": self.envId,
                "x": self.exchange,
                "m": self.market,
                "ts": {
                    "$gte": self.start,
                    "$lte": self.end,
                },
                "V": "C",
            }
        )

    def next(self):

        while True:  # Until a clean OB

            while True:  # Until prelimary OBs are removed

                orderbook = self.iter.next()

                if orderbook == None:
                    raise StopIteration

                assert "s" in orderbook

                if orderbook["s"] == True:

                    # Snapshot
                    self.buy_orderbook = orderbook["buy"]
                    self.sell_orderbook = orderbook["sell"]

                else:

                    # Delta
                    self.apply_deltas(orderbook)

                    orderbook["buy"] = self.buy_orderbook
                    orderbook["sell"] = self.sell_orderbook

                # First orderbooks not needed
                if self.start.replace(tzinfo=None) <= orderbook["ts"].replace(
                    tzinfo=None
                ):
                    break

            # Sanity check: best buy strictly less than best sell
            try:
                assert orderbook["buy"][0][0] < orderbook["sell"][0][0]
                break

            except AssertionError as err:
                # Log the first instance, otherwise just keep a count
                if self.corrupt_order_book_count == 0:
                    msg = f'OB Corruption: bestBuy={orderbook["buy"][0][0]} >= bestSell={orderbook["sell"][0][0]} @ {orderbook["ts"]}'
                    logging.error(msg)
                self.corrupt_order_book_count += 1

        orderbook["buy"] = np.array(orderbook["buy"], dtype=float)
        orderbook["sell"] = np.array(orderbook["sell"], dtype=float)

        assert len(orderbook["buy"]) > 0
        assert len(orderbook["sell"]) > 0

        return orderbook

    def apply_deltas(self, orderbook):

        assert self.buy_orderbook != None
        assert self.sell_orderbook != None

        # Apply the deltas to both sides
        self.apply_delta(self.buy_orderbook, orderbook["buy"])
        self.apply_delta(self.sell_orderbook, orderbook["sell"])

        self.buy_orderbook.sort(reverse=True)
        self.sell_orderbook.sort(reverse=False)

        # Ensure buy_orderbook has unique entries
        assert len(self.buy_orderbook) == len(set([x[0] for x in self.buy_orderbook]))

        # Ensure sell_orderbook has unique entries
        assert len(self.sell_orderbook) == len(set([x[0] for x in self.sell_orderbook]))

    def apply_delta(self, orderbook, deltas):

        for delta in deltas:

            for idx, entry in enumerate(orderbook):
                if entry[0] == delta[1]:
                    del orderbook[idx]

            if delta[0] == 0:  # Insert
                orderbook.append([delta[1], delta[2]])

            elif delta[0] == 1:  # Remove
                pass

            elif delta[0] == 2:  # Update
                orderbook.append([delta[1], delta[2]])

    @staticmethod
    def apply_depth(depth: float, orderbook: np.ndarray) -> np.ndarray:

        if depth == 0:
            return orderbook

        i = 0

        for entry in np.cumsum(np.prod(orderbook, axis=1)):

            i += 1

            if entry >= depth:
                break

        return orderbook[0:i]
