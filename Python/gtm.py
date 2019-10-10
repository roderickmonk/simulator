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
    logging.error('volume:\n%r', volume)

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
    logging.error ("remaining_depth.shape: %r", np.array (remaining_depth).shape)
    logging.error('remaining_depth:\n%r', remaining_depth)

    total_volume = sum(max(x) for x in trades_volumes)

    # create empty matrix to be filled
    values = np.empty(shape=(len(depths), len(price_depths)), dtype=np.float64)

    for i in range(len(depths)):

        for j in range(len(price_depths)):

            # remaining_depth and remaining_price_depth
            # used to populate griddata values
            values[i, j] = quadrant(remaining_depth[i],
                                    trades_price_depths[j]) / total_volume

    return list(values)


def quadrant(remaining_depth: list, remaining_pdepth: list):
    return sum(list(map(min, zip(*[remaining_depth, remaining_pdepth]))))


def remaining_price_depth(
        pdepth: list,
        remain: list,
        price_depths: list,
):

    remaining_pdepth = []

    for pd in price_depths:
        """
    remaining_pdepth <- sapply (1:length(pdepth), function (x)
    {
    if(any(price.depth <= pdepth[[x]]) )
    {
      remain[[x]][min(which(price.depth <= pdepth[[x]]))]
    }
    else
    {0}
    })
    """

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

    window = {
        "$gte": datetime.now() - timedelta(milliseconds=config["window"])
    }
    range = {"$gte": config["startTime"], "$lt": config["endTime"]}

    trades = list(mongodb["history"].trades.find(
        filter={
            "e": config["envId"],
            "x": config["exchange"],
            "m": config["market"],
            "ts": window if "window" in config else range
        }).sort("ts", 1).limit(10))

    logging.warning(len(trades))
    # logging.error (trades)
    # for t in trades:
    # print ("t.ts: ", int(t["ts"].timestamp()*1000))

    # Extract salient fields from each trade
    trades = [[
        int(t["ts"].timestamp() * 1000), t["id"], t["r"], t["q"], t["buy"]
    ] for t in trades]


def save_tuning(tuning: dict, ):
    # Save tuning data
    if config["output"] is None:
        output_name = ":".join([
            "auto",
            str(config["envId"]), config["exchange"], config["market"]
        ])
        output_name = {"name": output_name}
    else:
        output_name = {"name": config["output"]}

    config_db["tuning"].update_one(output_name,
                                   {'$set': {
                                       **output_name,
                                       **tuning
                                   }},
                                   upsert=True)


if __name__ == '__main__':

    logging.basicConfig(format='[%(levelname)-5s] %(message)s',
                        level=logging.WARNING,
                        datefmt='')

    logging.debug(f'sys.argv: {sys.argv}')

    assert os.environ['MONGODB'], 'MONGODB Not Defined'
    mongodb = MongoClient(os.environ['MONGODB'])
    config_db = mongodb["configuration"]

    load_config()
    logging.error(config)

    load_trades()
    logging.debug(trades)

    price_depths = price_depths()
    logging.debug(price_depths)

    depths = depths()
    logging.debug(depths)

    # os._exit(0)

    values = get_values(price_depths, depths)

    tuning = {"x": price_depths, "y": depths, "values": values}

    save_tuning(tuning)

    print("That's All Folks")
    """
    trades=[
        [12345, 1234, 30, 504, True],
        [12345, 1233, 31, 1003, True],
        [12345, 1235, 30, 1003, True],
        [98765, 1235, 31, 1003, True],
        [98765, 1235, 31, 1003, False],
    ]
    """
