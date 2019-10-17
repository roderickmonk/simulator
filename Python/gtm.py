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
config = None
trades = None
IL = None
BUY = 1


def trades_price_depth(trades: list):
    def remaining_price_depths():

        trades_price_depths.append([
            1.0 /
            (bucket[0][2] / trade[2]) if trade[4] == BUY else bucket[0][2] /
            trade[2] for trade in bucket
        ])

    def remaining_volumes():

        l = list(np.cumsum([t[2] * t[3] for t in bucket]))
        trades_volumes.append([v if v <= IL else IL for v in l])

    def process_bucket():

        bucket.sort(key=lambda x: x[2])

        remaining_price_depths()
        remaining_volumes()

    # Preliminary sort by ts, id, and buy
    trades.sort(key=lambda x: (x[0], x[4], x[1]))

    trades_price_depths = []
    trades_volumes = []

    bucket = []
    ref_trade = trades[0]

    try:
        for trade in trades:

            # New bucket?
            if trade[0] != ref_trade[0] or trade[4] != ref_trade[4]:

                # Process the existing bucket and...
                process_bucket()

                # ...start again with an empty one
                bucket = []
                ref_trade = trade

            bucket.append(trade)

    finally:

        # Handle the last bucket
        process_bucket()

    return trades_price_depths, trades_volumes


def remaining_size_depths(
        remaining_volumes: list,
        depths: list,
):

    volume = [max(x) for x in remaining_volumes]
    logging.debug('volume:\n%r', volume)

    remainders = []
    for depth in depths:

        logging.debug('depth:\n%r', depth)

        remainder = volume - depth
        logging.debug('remainder:\n%r', remainder)

        # Ensure that if the remainder is less than 0 we only have 0
        remainders.append([(x if x > 0 else 0) for x in volume - depth])

    return remainders


def get_values(price_depths: list, depths: list):

    trades_price_depths, trades_volumes = trades_price_depth(trades)

    logging.debug('trades_price_depths:\n%r', trades_price_depths)
    logging.debug('trades_volumes:\n%r', trades_volumes)

    remaining_depth = remaining_size_depths(trades_volumes, depths)
    logging.error("remaining_depth.shape: %r", np.array(remaining_depth).shape)
    logging.debug('remaining_depth:\n%r', remaining_depth)

    remaining_pdepth = remaining_price_depth(trades_price_depths,
                                             trades_volumes, price_depths)

    logging.error("remaining_pdepth.shape: %r",
                  np.array(remaining_pdepth).shape)
    logging.debug('remaining_pdepth:\n%r', remaining_pdepth)

    total_volume = sum(max(x) for x in trades_volumes)

    # create empty matrix to be filled
    values = np.empty(shape=(len(depths), len(price_depths)), dtype=np.float64)

    logging.error("values.shape: %r", values.shape)

    for i in range(len(depths)):

        for j in range(len(price_depths)):

            values[i][j] = quadrant(remaining_depth[i],
                                    remaining_pdepth[j]) / total_volume

    return values.tolist()


def quadrant(remaining_depth: list, remaining_pdepth: list):
    # logging.error ("remaining_depth.shape: %r", np.array(remaining_depth).shape)
    return sum([min(x, y) for x, y in zip(remaining_depth, remaining_pdepth)])

def remaining_price_depth(
        pdepth: list,
        remain: list,
        price_depths: list,
):

    logging.debug("remain:\n%r", remain)

    remaining_pdepth = []

    remaining_pdepth_ele = []

    for price_depth in price_depths:

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


def price_depths():

    return list(
        np.insert(
            10**np.linspace(log10(config["priceDepthStart"]),
                            log10(config["priceDepthEnd"]),
                            config["priceDepthSamples"]), 0, 0) + 1.0)


def depths():

    return list(
        np.insert(
            10**np.linspace(log10(config["depthStart"]),
                            log10(config["depthEnd"]), config["depthSamples"]),
            0, 0))


def load_config():

    global config
    global IL

    config_collection = config_db["generate.tuning"]
    config = config_collection.find_one({"name": sys.argv[1]})
    IL = config["inventoryLimit"]


def load_trades():

    global trades

    if "window" in config:
        range = {"$gte": datetime.now() - timedelta(milliseconds=config["window"])}
    else:
        range = {"$gte": config["startTime"], "$lt": config["endTime"]}

    trades = list(mongodb["history"].trades.find(
        filter={
            "e": config["envId"],
            "x": config["exchange"],
            "m": config["market"],
            "ts": range
        }).sort("ts", 1))

    logging.warning("Trade Count: %d", len(trades))
    # logging.error (trades)
    # for t in trades:
    # print ("t.ts: ", int(t["ts"].timestamp()*1000))

    # Extract salient fields from each trade
    trades = [[
        int(t["ts"].timestamp() * 1000), t["id"], t["r"], t["q"], t["buy"]
    ] for t in trades]

def save_tuning(tuning: dict):

    if config["output"] is None:
        output_name = ":".join([
            "auto",
            str(config["envId"]), config["exchange"], config["market"]
        ])
        output_name = {"name": output_name}
    else:
        output_name = {"name": config["output"]}

    document = {"$set": {**output_name, **tuning}}

    config_db["tuning"].update_one(output_name, document, upsert=True)


if __name__ == '__main__':

    logging.basicConfig(format='[%(levelname)-5s] %(message)s',
                        level=logging.INFO,
                        datefmt='')

    logging.debug(f'sys.argv: {sys.argv}')

    assert os.environ['MONGODB'], 'MONGODB Not Defined'
    mongodb = MongoClient(os.environ['MONGODB'])
    config_db = mongodb["configuration"]

    load_config()
    logging.error(config)

    load_trades()

    if len(trades) == 0:
        logging.info ("No Trades!")
        exit (0)

    logging.debug(trades)

    price_depths = price_depths()
    logging.debug(price_depths)

    depths = depths()
    logging.debug(depths)

    values = get_values(price_depths, depths)

    tuning = {"x": price_depths, "y": depths, "values": values}

    save_tuning(tuning)

    logging.info("That's All Folks")
    """
    trades=[
        [12345, 1234, 30, 504, True],
        [12345, 1233, 31, 1003, True],
        [12345, 1235, 30, 1003, True],
        [98765, 1235, 31, 1003, True],
        [98765, 1235, 31, 1003, False],
    ]
    """
