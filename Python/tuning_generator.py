import numpy as np
from operator import sub
import os
from pymongo import MongoClient
from bson.objectid import ObjectId
import sys
import logging
from datetime import datetime, timedelta
from math import log10

mongodb = None
config_db = None
BUY = 1


class TuningGenerator:

    config: dict
    IL: float
    bucket: list
    trades: list
    depths: list
    price_depths: list
    trades_volumes: list
    trades_price_depths: list

    def __init__(self, config: dict = None, configName: str = None) -> None:

        assert not (config == None and configName == None)
        assert not (config != None and configName != None)

        self.trades_volumes = []
        self.trades_price_depths = []

        if config:
            self.config = config
        else:
            self.load_config(configName=configName)

        self.IL = self.config["inventoryLimit"]

        self.generate_price_depths()
        self.generate_depths()

    def remaining_price_depths(self):

        assert len (self.bucket) > 0

        logging.error("bucket: %r", self.bucket)

        tmp = []
        for trade in self.bucket:
            ratio = self.bucket[0][2] / trade[2]
            if trade[4] == BUY:
                tmp.append(ratio)
            else:
                tmp.append(1 / ratio)

        self.trades_price_depths.append(tmp)

    def remaining_volumes(self):

        self.trades_volumes.append([
            v if v <= self.IL else self.IL
            for v in list(np.cumsum([t[2] * t[3] for t in self.bucket]))
        ])

    def process_bucket(self):

        assert len (self.bucket) > 0

        
        # logging.error("bucket-before: %r", self.bucket)

        self.bucket.sort(key=lambda x: x[2],
                         reverse=(self.bucket[0][4] == True))

        # logging.error("bucket-after: %r", self.bucket)


        self.remaining_price_depths()
        self.remaining_volumes()

    def trades_price_depth(self):

        # Preliminary sort by ts, id, and buy
        self.trades.sort(key=lambda x: (x[0], x[4], x[1]))

        self.bucket = []
        ref_trade = self.trades[0]

        try:
            for trade in self.trades:

                # New bucket?
                if trade[0] != ref_trade[0] or trade[4] != ref_trade[4]:

                    # Process the existing bucket and...
                    self.process_bucket()

                    # ...start again with an empty one
                    self.bucket = []
                    ref_trade = trade

                self.bucket.append(trade)

        finally:
            # Handle the last bucket
            self.process_bucket()

    def remaining_size_depths(
            self,
            remaining_volumes: list,
    ):

        volume = [max(x) for x in remaining_volumes]
        logging.debug('volume:\n%r', volume)

        remainders = []
        for depth in self.depths:

            logging.debug('depth:\n%r', depth)

            remainder = volume - depth
            logging.debug('remainder:\n%r', remainder)

            # Ensure that if the remainder is less than 0 we only have 0
            remainders.append([(x if x > 0 else 0) for x in volume - depth])

        return remainders

    def get_values(self):

        price_depths = self.price_depths()
        logging.debug(price_depths)

        depths = self.depths()
        logging.debug(depths)

        self.trades_price_depth()

        logging.debug('trades_price_depths:\n%r', self.trades_price_depths)
        logging.info('trades_volumes:\n%r', self.trades_volumes)

        remaining_depth = self.remaining_size_depths(self.trades_volumes)
        logging.debug("remaining_depth.shape: %r",
                      np.array(remaining_depth).shape)
        logging.debug('remaining_depth:\n%r', remaining_depth)

        remaining_pdepth = self.remaining_price_depth(self.trades_price_depths,
                                                      self.trades_volumes)

        logging.debug("remaining_pdepth.shape: %r",
                      np.array(remaining_pdepth).shape)
        logging.debug('remaining_pdepth:\n%r', remaining_pdepth)

        total_volume = 1  # sum(max(x) for x in trades_volumes)

        # create empty matrix to be filled
        values = np.empty(shape=(len(self.depths), len(self.price_depths)),
                          dtype=np.float64)

        logging.debug("values.shape: %r", values.shape)

        for i in range(len(self.depths)):

            for j in range(len(price_depths)):

                values[i][j] = quadrant(remaining_depth[i],
                                        remaining_pdepth[j]) / total_volume

        return values.tolist()

    def quadrant(self, remaining_depth: list, remaining_pdepth: list):
        # logging.error ("remaining_depth.shape: %r", np.array(remaining_depth).shape)
        return sum(
            [min(x, y) for x, y in zip(remaining_depth, remaining_pdepth)])

    def remaining_price_depth(
            self,
            pdepth: list,
            remain: list,
    ):

        logging.debug("remain:\n%r", remain)

        remaining_pdepth = []

        remaining_pdepth_ele = []

        for price_depth in self.price_depths:

            try:
                for idx_x, x in enumerate(pdepth):

                    val, idx = min((val, idx) for (idx, val) in enumerate(x))

                    if price_depth <= val:
                        remaining_pdepth_ele.append(remain[idx_x][idx])
                    else:
                        remaining_pdepth_ele.append(0.0)

            finally:
                remaining_pdepth.append(remaining_pdepth_ele)
                remaining_pdepth_ele = []

        return remaining_pdepth

    def generate_price_depths(self) -> list:

        self.price_depths = list(
            np.insert(
                10**np.linspace(log10(self.config["priceDepthStart"]),
                                log10(self.config["priceDepthEnd"]),
                                self.config["priceDepthSamples"]), 0, 0) + 1.0)

        return self.price_depths

    def generate_depths(self) -> list:

        self.depths = list(
            np.insert(
                10**np.linspace(log10(self.config["depthStart"]),
                                log10(self.config["depthEnd"]),
                                self.config["depthSamples"]), 0, 0))

        return self.depths

    def load_trades(self, *, trades: list = None):

        print("trades")
        print(trades)

        if trades == None:

            # Retrieve trades from the database

            if "window" in self.config:
                range = {
                    "$gte":
                    datetime.now() -
                    timedelta(milliseconds=self.config["window"])
                }
            else:
                range = {
                    "$gte": self.config["startTime"],
                    "$lt": self.config["endTime"]
                }

            self.trades = list(mongodb["history"].trades.find(
                filter={
                    "e": self.config["envId"],
                    "x": self.config["exchange"],
                    "m": self.config["market"],
                    "ts": range
                }).sort("ts", 1))

            # Extract salient fields from each trade
            self.trades = [[
                int(t["ts"].timestamp() * 1000), t["id"], t["r"], t["q"],
                t["buy"]
            ] for t in self.trades]

        else:
            self.trades = trades

        logging.warning("Trade Count: %d", len(self.trades))

    def load_config(self, configName: dict):

        config_collection = config_db["generate.tuning"]
        self.config = config_collection.find_one({"name": configName})


def save_tuning(config: dict, tuning: dict):

    if not "output" in config or config["output"] is None:
        output_name = {"name": config["name"]}
    else:
        output_name = {"name": config["output"]}

    document = {"$set": {**output_name, **tuning}}

    config_db["tuning"].update_one(output_name, document, upsert=True)


if __name__ == '__main__':

    logging.basicConfig(format='[%(levelname)-5s] %(message)s',
                        level=logging.DEBUG,
                        datefmt='')

    logging.debug(f'sys.argv: {sys.argv}')

    assert os.environ['MONGODB'], 'MONGODB Not Defined'
    mongodb = MongoClient(os.environ['MONGODB'])
    config_db = mongodb["configuration"]

    tg = TuningGenerator(configName=sys.argv[1])
    tg.load_trades()

    if len(tg.trades) == 0:
        logging.error("(%s) No Trades!", sys.argv[1])
        exit(0)

    logging.error("(%s) Trades Count: %d", sys.argv[1], len(tg.trades))
    logging.debug(tg.trades)

    values = tg.get_values()

    tuning = {"price_depths": tg.depths, "depths": tg.depths, "values": values}

    save_tuning(tuning)

    logging.info("That's All Folks")
