import logging
import pymongo
from pymongo import MongoClient
from pymongo.collection import Collection
from functools import reduce
from datetime import datetime, timedelta
import numpy as np

try:
    profile
except NameError:
    profile = lambda x: x

class Orderbooks:

    time_of_start_snapshot = None
    actual_end = None

    def __init__(
            self,
            *,
            orderbooks_collection: Collection,
            envId: int,
            exchange: str,
            market: str,
            depth: float,
            start: datetime,
            end: datetime,
    ):

        self.start = start
        self.end = end
        self.depth = depth

        assert start < end

        self.buy_orderbook = []
        self.sell_orderbook = []

        start_snapshot = Orderbooks.get_start_snapshot(
            envId,
            exchange,
            market,
            start,
            orderbooks_collection=orderbooks_collection)

        self.buy_orderbook = start_snapshot["buy"]
        self.sell_orderbook = start_snapshot["sell"]

        # Ensure both buy and sell OBs are sorted
        self.buy_orderbook.sort(reverse=True)
        self.sell_orderbook.sort(reverse=False)

        self.time_of_start_snapshot = start_snapshot["ts"]

        last_orderbook = Orderbooks.get_last_orderbook(
            envId,
            exchange,
            market,
            start,
            end,
            orderbooks_collection=orderbooks_collection)

        self.actual_end = last_orderbook["ts"]

        if __debug__:
            logging.debug('start_snapshot: %r', start_snapshot)
            logging.debug('actual_end: %r', self.actual_end)

        # Setup an iterator to drive the simulation
        self.iter = iter(
            orderbooks_collection.find(
                filter={
                    "e": envId,
                    "x": exchange,
                    "m": market,
                    "ts": {
                        "$gte": self.time_of_start_snapshot,
                        "$lt": end
                    },
                    "s": {
                        "$exists": True
                    },
                    "V": "V", # valid flag
                }
            ).sort([
                ("e", pymongo.ASCENDING),
                ("x", pymongo.ASCENDING),
                ("m", pymongo.ASCENDING),
                ("ts", pymongo.ASCENDING),
            ])
        )

    def __str__(self):
        return str(self.__class__) + '\n' + '\n'.join(
            '{} = {}'.format(
                item,
                self.__dict__[item]) for item in self.__dict__)

    def __iter__(self):
        return self

    @staticmethod
    def get_start_snapshot(
        envId: int,
        exchange: str,
        market: str,
        start: datetime,
        orderbooks_collection
    ):

        earlier_snapshots = list(orderbooks_collection.find(
            filter={
                "e": envId,
                "x": exchange,
                "m": market,
                # Look for a snapshot within the last 3 hours
                "ts": {
                    "$lte": start,
                    "$gte": start - timedelta(milliseconds=3*3600000)
                },
                "s": True,
            }))

        if len(earlier_snapshots) > 0:

            # Work with the most recent one
            start_snapshot = reduce(
                lambda x, y: x if x['ts'] > y['ts'] else y,
                earlier_snapshots,
            )

        else:

            # Nothing earlier; look for the next snapshot
            snapshots = list(
                orderbooks_collection.find(
                    filter={
                        "e": envId,
                        "x": exchange,
                        "m": market,
                        "ts": {
                            "$gte": start,
                        },
                        "s": True,
                    }).sort("ts", 1).limit(1)
            )
            if len(snapshots) == 0:
                raise StopIteration
            else:
                start_snapshot = snapshots[0]

        return start_snapshot

    @staticmethod
    def get_first_orderbook(
        envId: int,
        exchange: str,
        market: str,
        start: datetime,
        orderbooks_collection,
    ):

        # Nothing earlier; look for the next snapshot
        orderbooks = list(
            orderbooks_collection.find(
                filter={
                    "e": envId,
                    "x": exchange,
                    "m": market,
                    "ts": {
                        "$gte": start,
                    },
                })
            .sort("ts", 1).limit(1)
        )
        assert len(orderbooks) > 0

        return orderbooks[0]

    @staticmethod
    def get_last_orderbook(
        envId: int,
        exchange: str,
        market: str,
        start: datetime,
        end: datetime,
        orderbooks_collection,
    ):

        # Nothing earlier; look for the next snapshot
        orderbooks = list(
            orderbooks_collection.find(
                filter={
                    "e": envId,
                    "x": exchange,
                    "m": market,
                    "ts": {
                        "$gt": start,
                        "$lt": end,
                    },
                }).sort("ts", -1).limit(1)
        )
        assert len(orderbooks) == 1, 'Unable to Find Last Orderbook'

        if len(orderbooks) == 0:
            return None
        else:
            return orderbooks[0]

    @staticmethod
    def count_orderbooks(
        envId: int,
        exchange: str,
        market: str,
        start: datetime,
        end: datetime,
        orderbooks_collection,
    ):

        return orderbooks_collection.count_documents(
            filter={
                "e": envId,
                "x": exchange,
                "m": market,
                "ts": {
                    "$gte": start,
                    "$lt": end,
                },
                "V": "V",
                "s": {
                    "$exists": True
                }
            })

    @staticmethod
    def count_corrupt_orderbooks(
        envId: int,
        exchange: str,
        market: str,
        start: datetime,
        end: datetime,
        orderbooks_collection,
    ):

        return orderbooks_collection.count_documents(
            filter={
                "e": envId,
                "x": exchange,
                "m": market,
                "ts": {
                    "$gte": start,
                    "$lte": end,
                },
                "V": "C",
            })


    @profile
    def next(self):

        while True:

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
            if self.start.replace(tzinfo=None) <= orderbook["ts"].replace(tzinfo=None):
                break

        # Sanity check: best buy strictly less than best sell
        assert orderbook["buy"][0][0] < orderbook["sell"][0][0]

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
        assert len(self.buy_orderbook) == len(
            set([x[0] for x in self.buy_orderbook]))

        # Ensure sell_orderbook has unique entries
        assert len(self.sell_orderbook) == len(
            set([x[0] for x in self.sell_orderbook]))

    def apply_delta(self, orderbook, deltas):

        for delta in deltas:

            for idx, entry in enumerate(orderbook):
                if entry[0] == delta[1]:
                    del orderbook[idx]

            if delta[0] == 0:   # Insert
                orderbook.append([delta[1], delta[2]])

            elif delta[0] == 1:  # Remove
                pass

            elif delta[0] == 2:  # Update
                orderbook.append([delta[1], delta[2]])

    @staticmethod
    def apply_depth(depth: float, orderbook: np.array) -> np.array:

        if depth == 0:
            return orderbook

        i = 0

        for entry in np.cumsum(np.prod(orderbook, axis=1)):

            i += 1

            if entry >= depth:
                break

        return orderbook[0:i]
