import numpy as np
from operator import sub
import os
from pymongo import MongoClient
from bson.objectid import ObjectId
import sys
import logging
from datetime import datetime, timedelta
import math
import redis
import json
from pprint import pprint

mongodb = None
config_db = None
BUY = 1


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


class TuningGenerator:
    def __init__(self, config: dict = None, configName: str = None):
        """
        Must initialize with either a config dict or a name of a config.
        If the latter, then configName is used to retrieve the config
        from the database.
        """
        assert not (config == None and configName == None)
        assert not (config != None and configName != None)

        self.depths = []
        self.price_depths = []
        self.meta_remaining_volumes = []
        self.meta_price_depths = []
        self.remaining_depth = []
        self.meta_trades = []

        if config:
            self.config = config
        else:
            self.load_config(configName=configName)

        self.load_price_depths()
        self.load_depths()

    def load_trades_volumes(self) -> None:

        IL = self.config["inventoryLimit"]

        self.meta_remaining_volumes.append([
            v if v <= IL else IL for v in list(
                np.cumsum([t[2] * t[3] for t in self.meta_trade][::-1]))
        ][::-1])

    def load_trades_price_depths(self) -> None:

        best_rate = self.meta_trade[0][2]

        self.meta_price_depths.append(
            list(
                map(
                    lambda x: x[2] / best_rate
                    if x[2] > best_rate else best_rate / x[2],
                    self.meta_trade)))

    def process_meta_trade(self) -> None:

        assert len(self.meta_trade) > 0

        logging.debug("meta_trade-before: %r", self.meta_trade)

        self.meta_trade.sort(key=lambda x: x[2],
                             reverse=not self.meta_trade[0][4])

        logging.debug("meta_trade-after: %r", self.meta_trade)

        self.meta_trades.append(self.meta_trade)

        self.load_trades_price_depths()
        self.load_trades_volumes()

    def trades_price_depth(self) -> None:

        # Preliminary sort by ts, id, and buy
        self.trades.sort(key=lambda x: (x[0], x[4], x[1]))

        self.meta_trade = []
        ref_trade = self.trades[0]

        try:
            for trade in self.trades:

                # New meta_trade?
                if trade[0] != ref_trade[0] or trade[4] != ref_trade[4]:

                    # Process the existing meta_trade and...
                    self.process_meta_trade()

                    # ...start again with an empty one
                    self.meta_trade = []
                    ref_trade = trade

                self.meta_trade.append(trade)

        finally:
            # Handle the last meta_trade
            self.process_meta_trade()

    def load_total_volume(self):
        self.total_volume = sum(max(x) for x in self.meta_remaining_volumes)

    def load_remaining_depth(self) -> None:

        volume = [max(x) for x in self.meta_remaining_volumes]
        logging.debug('volume:\n%r', volume)

        self.remaining_depth = [[x if x > 0 else 0 for x in volume - depth]
                                for depth in self.depths]

    def load_remaining_price_depths(self) -> None:

        logging.debug("meta_remaining_volumes:\n%r",
                      self.meta_remaining_volumes)
        logging.debug("meta_price_depths:\n%r", self.meta_price_depths)
        logging.debug("price_depths:\n%r", self.price_depths)

        self.remaining_price_depths = []

        for price_depth in self.price_depths:

            tmp = []

            for idx_x, trade_price_depth in enumerate(self.meta_price_depths):

                found = False
                best_trade_price_depth = math.inf
                best_idx: int = 0
                for idx, pd in enumerate(trade_price_depth):
                    if price_depth <= pd and pd < best_trade_price_depth:
                        best_trade_price_depth = pd
                        best_idx = idx
                        found = True

                if found:
                    tmp.append(self.meta_remaining_volumes[idx_x][best_idx])
                else:
                    tmp.append(0.0)

            self.remaining_price_depths.append(tmp)

    def get_values(self) -> None:

        self.trades_price_depth()

        logging.debug('meta_price_depths:\n%r', self.meta_price_depths)
        logging.info('meta_remaining_volumes:\n%r',
                     self.meta_remaining_volumes)

        self.load_remaining_depth()

        logging.debug("remaining_depth.shape: %r",
                      np.array(self.remaining_depth).shape)
        logging.debug('remaining_depth:\n%r', self.remaining_depth)

        self.load_remaining_price_depths()

        logging.debug("remaining_price_depths.shape: %r",
                      np.array(self.remaining_price_depths).shape)
        logging.debug('remaining_price_depths:\n%r',
                      self.remaining_price_depths)

        self.load_total_volume()

        return [[
            self.quadrant(self.remaining_depth[j],
                          self.remaining_price_depths[i]) / self.total_volume
            for i in range(len(self.price_depths))
        ] for j in range(len(self.depths))]

    def quadrant(self, remaining_depth: list,
                 remaining_price_depths: list) -> None:

        assert len(remaining_depth) == len(remaining_price_depths)
        return sum([
            min(x, y) for x, y in zip(remaining_depth, remaining_price_depths)
        ])

    def load_price_depths(self) -> None:

        self.price_depths = list(
            np.insert(
                10**np.linspace(math.log10(self.config["priceDepthStart"]),
                                math.log10(self.config["priceDepthEnd"]),
                                self.config["priceDepthSamples"] - 1), 0, 0) +
            1.0)

    def load_depths(self) -> None:

        self.depths = list(
            np.insert(
                10**np.linspace(math.log10(self.config["depthStart"]),
                                math.log10(self.config["depthEnd"]),
                                self.config["depthSamples"] - 1), 0, 0))

    def load_trades(self, trades: list = None) -> None:

        if trades == None:

            # Retrieve trades from the database

            if "window" in self.config:
                ts_range = {
                    "$gte":
                    datetime.now() -
                    timedelta(milliseconds=self.config["window"])
                }
            else:
                ts_range = {
                    "$gte": self.config["startTime"],
                    "$lte": self.config["endTime"]
                }

            self.trades = list(mongodb["history"]["bittrex-v1-trades"].find(
                filter={
                    "e": self.config["envId"],
                    "x": self.config["exchange"],
                    "m": self.config["market"],
                    "ts": ts_range
                }).sort("ts", 1))

            # Extract salient fields from each trade
            self.trades = [[
                int(t["ts"].timestamp() * 1000), t["id"], t["r"], t["q"],
                t["buy"]
            ] for t in self.trades]

            logging.error("First trade: %r", self.trades[0])
            logging.error("Last trade: %r", self.trades[-1])

            logging.error(
                datetime.utcfromtimestamp(self.trades[0][0] /
                                          1000).strftime('%Y-%m-%d %H:%M:%S'))
            logging.error(
                datetime.utcfromtimestamp(self.trades[-1][0] /
                                          1000).strftime('%Y-%m-%d %H:%M:%S'))
        else:
            self.trades = trades

        logging.error("Trade Count: %d", len(self.trades))

    def load_config(self, configName: dict) -> None:

        config_collection = config_db["generate.tuning"]
        self.config = config_collection.find_one({"name": configName})


def save_tuning(config: dict, tuning: dict) -> None:

    if not "output" in config or config["output"] is None:
        output_name = {"name": config["name"]}
    else:
        output_name = {"name": config["output"]}

    # Delete existing if one existing
    config_db["tuning"].delete_one(output_name)

    document = {"$set": {**output_name, **tuning}}

    config_db["tuning"].update_one(output_name, document, upsert=True)


if __name__ == '__main__':

    logging.basicConfig(format='[%(levelname)-5s] %(message)s',
                        level=logging.ERROR,
                        datefmt='')

    logging.debug(f'sys.argv: {sys.argv}')

    assert os.environ['MONGODB'], 'MONGODB Not Defined'
    mongodb = MongoClient(os.environ['MONGODB'])
    config_db = mongodb["configuration"]

    tg = TuningGenerator(configName=sys.argv[1])
    tg.load_trades()

    if len(tg.trades) == 0:
        logging.debug("(%s) No Trades!", sys.argv[1])
        exit(0)

    logging.debug("(%s) Trades Count: %d", sys.argv[1], len(tg.trades))
    logging.debug(tg.trades)

    values = tg.get_values()

    now = datetime.now()

    tuning = {
        "ts": now,
        "price_depths": tg.price_depths,
        "depths": tg.depths,
        "values": values,
        #"remainingDepths": tg.remaining_depth,
        #"remainingPriceDepths": tg.remaining_price_depths,
        #"metaPriceDepths": tg.meta_price_depths,
        #"metaRemainingVolumes": tg.meta_remaining_volumes,
    }

    save_tuning(tg.config, tuning)

    r = redis.Redis(host='3.11.7.67',
                    port=6379,
                    encoding=u'utf-8',
                    decode_responses=True,
                    db=0)

    # Save to redis as well

    r.hmset(
        tg.config["name"],
        {
            # "ts": now,
            "depths": tg.depths.tolist(),
            "price_depths": str(tg.price_depths),
            # "values": json.dumps(values),
        })

    pprint(tg.depths.tolist())
    """
    r.hmset (
        tg.config["name"], {
        "_id": str(t["_id"]).encode(),
        "e": t['e'],
        "x": t['x'].encode(),
        "m": t['m'].encode(),
        "ts": str(t['ts']).encode(),
        "ob": str(t['ob']).encode(),
        "r": t['r'],
        "q": t['q'],
    }) 
    """

    print("That's All Folks")
