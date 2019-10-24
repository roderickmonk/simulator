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
    meta_trade: list
    trades: list
    depths: list
    price_depths: list
    trades_volumes: list
    trades_price_depths: list

    def __init__(self, config: dict = None, configName: str = None):
        """
        Must initialize with either a config dict or a name of a config.
        If the latter, then configName is used to retrieve the config
        from the database.
        """
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

    def remaining_volumes(self) -> None:

        assert len(self.meta_trade) > 0

        # Ensure meta_trade contains only trades with the same timestamp
        assert all([t[0] == self.meta_trade[0][0] for t in self.meta_trade])

        # Ensure meta_trade contains only trades from one side
        assert all([t[4] == self.meta_trade[0][4] for t in self.meta_trade])

        self.trades_volumes.append([
            v if v <= self.IL else self.IL for v in list(
                np.cumsum([t[2] * t[3] for t in self.meta_trade][::-1]))
        ][::-1])

    def remaining_price_depths(self) -> None:

        assert len(self.meta_trade) > 0

        logging.debug("meta_trade: %r", self.meta_trade)

        tmp = []
        for trade in self.meta_trade:
            ratio = self.meta_trade[0][2] / trade[2]
            if trade[4] == BUY:
                tmp.append(1 / ratio)
            else:
                tmp.append(ratio)

        self.trades_price_depths.append(tmp)

    def process_meta_trade(self) -> None:

        assert len(self.meta_trade) > 0

        logging.debug("meta_trade-before: %r", self.meta_trade)

        self.meta_trade.sort(key=lambda x: x[2],
                             reverse=not self.meta_trade[0][4])

        logging.debug("meta_trade-after: %r", self.meta_trade)

        self.remaining_price_depths()
        self.remaining_volumes()

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

    def remaining_size_depths(
            self,
            remaining_volumes: list,
    ) -> None:

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

    def remaining_price_depth(
            self,
            pdepth: list,
            remain: list,
    ) -> None:

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

    def get_values(self) -> None:

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

        # total_volume = 1
        total_volume = sum(max(x) for x in self.trades_volumes)

        # create empty matrix to be filled
        values = np.empty(shape=(len(self.depths), len(self.price_depths)),
                          dtype=np.float64)

        logging.debug("values.shape: %r", values.shape)

        for i in range(len(self.depths)):

            for j in range(len(self.price_depths)):

                values[i][j] = self.quadrant(
                    remaining_depth[i], remaining_pdepth[j]) / total_volume

        return values.tolist()

    def quadrant(self, remaining_depth: list, remaining_pdepth: list) -> None:

        assert len(remaining_depth) == len(remaining_pdepth)
        # logging.debug ("remaining_depth.shape: %r", np.array(remaining_depth).shape)
        return sum(
            [min(x, y) for x, y in zip(remaining_depth, remaining_pdepth)])

 
    def generate_price_depths(self) -> None:

        self.price_depths = list(
            np.insert(
                10**np.linspace(log10(self.config["priceDepthStart"]),
                                log10(self.config["priceDepthEnd"]),
                                self.config["priceDepthSamples"]), 0, 0) + 1.0)

    def generate_depths(self) -> None:

        self.depths = list(
            np.insert(
                10**np.linspace(log10(self.config["depthStart"]),
                                log10(self.config["depthEnd"]),
                                self.config["depthSamples"]), 0, 0))

    def load_trades(self, trades: list = None) -> None:

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

        logging.critical("Trade Count: %d", len(self.trades))

    def load_config(self, configName: dict) -> None:

        config_collection = config_db["generate.tuning"]
        self.config = config_collection.find_one({"name": configName})


def save_tuning(config: dict, tuning: dict) -> None:

    if not "output" in config or config["output"] is None:
        output_name = {"name": config["name"]}
    else:
        output_name = {"name": config["output"]}

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

    tuning = {"price_depths": tg.depths, "depths": tg.depths, "values": values}

    save_tuning(tg.config, tuning)

    logging.critical("That's All Folks")
