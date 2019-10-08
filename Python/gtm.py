import numpy as np
from operator import sub
import os
from pymongo import MongoClient
from bson.objectid import ObjectId
import sys
import logging
from datetime import datetime, timedelta



BUY = 1


def calc_price_depths(
    bucket: list,
    price_depths: list,
    sorted_trades: list,
):

    ref_rate = bucket[0][2]

    l = []
    for trade in bucket:

        if trade[4] == BUY:
            l.append(1.0 / (ref_rate / trade[2]))
        else:
            l.append(ref_rate / trade[2])

    price_depths.append(l)


def price_depths_and_volumes(
    trades: list,
):

    def process_bucket():

        bucket.sort(key=lambda x: x[2])

        calc_price_depths(bucket, price_depths, sorted_trades)

        volumes.append(list(np.cumsum([t[3] for t in bucket])))

        sorted_trades.extend(bucket)

    # Preliminary sort by ts, id, and buy
    trades.sort(key=lambda x: (x[0], x[1], x[4]))

    sorted_trades = []
    price_depths = []
    volumes = []
    bucket = []
    ref_ts = trades[0][0]
    ref_buy = trades[0][4]

    try:
        for trade in trades:

            # New bucket when ts changes
            if trade[0] != ref_ts or trade[4] != ref_buy:

                process_bucket()

                # Start again with an empty bucket
                bucket, ref_ts, ref_buy = [], trade[0], trade[4]

            bucket.append(trade)

    finally:

        # Handle the last bucket
        process_bucket()

    return price_depths, volumes


def remaining_size_depths(
  remaining_volumes: list,
  depths: list,
):

    remainders = []

    for depth in depths:

      volume = [max(x) for x in remaining_volumes]

      remainder < - volume - depth
      remainder = list(map(sub, volume, depth))

      # Ensure that if the remainder is less than 0 we only have 0
      remainders.append([(x if x > 0 else 0) for x in remainder])

    return remainders

def remaining_trade_size_quadrant(
  trades: list,
  price_depths: list,
  depths: list,
  IL: float):

    price_depths, remaining_volumes=price_depths_and_volumes(trades)

    print("price_depths\n", price_depths)
    print("remaining_volumes\n", remaining_volumes)

    remaining_depth=remaining_size_depths(
        remaining_volumes,
        depths)

    total_volume=sum(max(x) for x in remaining_volume)

    # create empty matrix to be filled
    output=np.empty(shape = (len(depths), len(price_depths)),
                    dtype = np.float64)

    for i in range (len(depths)):
    
      for j in range (len(price_depths)):
      
        # use the remaining depth and price depth rows to calculate quadrant data
        output[i,j] = quadrant(remaining_depth[i], remaining_pdepth[j]) / total_volumefsubract
  
    return output

def quadrant (remaining_depth: list, remaining_pdepth: list):
  return sum(list(map(min, zip(*[remaining_depth,remaining_pdepth]))))

  
def remaining_price_depth(
  pdepth: list,
  remain: list,
  price_depths: list,
):

  remaining_pdepth=[]

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

if __name__ == '__main__':

    logging.basicConfig(
        format='[%(levelname)-5s] %(message)s',
        level=logging.DEBUG,
        datefmt='')

    logging.debug(f'simulate args: {sys.argv}')
    config_id = sys.argv[1]

    assert os.environ['MONGODB'], 'MONGODB Not Defined'
    remote_mongo_client = MongoClient(os.environ['MONGODB'])
    config_db = remote_mongo_client["configuration"]
    config_collection = config_db["generate.tuning"]

    config = config_collection.find_one ({"name": sys.argv[1]})

    logging.error (config)

    history_db = remote_mongo_client["history"]

    if "startWindow" in config:

        trades = list(
                    history_db.trades.find(
                        filter={
                            "e": config["envId"],
                            "x": config["exchange"],
                            "m": config["market"],
                            "ts": {
                                "$gte": datetime.now() - timedelta(milliseconds=config["startWindow"])
                            },
                        }).sort("ts", 1)
                )

    elif (
        "startTime" in config and 
        "endTime" in config and 
        config["startTime"] < config["endTime"]
    ):
        print ("range")
        trades = list(
                    history_db.trades.find(
                        filter={
                            "e": config["envId"],
                            "x": config["exchange"],
                            "m": config["market"],
                            "ts": {
                                "$gte": config["startTime"],
                                "$lt": config["endTime"]
                            },
                        }).sort("ts", 1).limit(5)
                )

    logging.error (len(trades))
    logging.error (trades)
    for t in trades:
        print ("t.ts: ", int(t["ts"].timestamp()*1000))

    new_trades = []
    for t in trades:
        new_trades.append ([int(t["ts"].timestamp()*1000), t["id"], t["r"], t["q"], t["buy"]])


    print (new_trades)

    os._exit(0)


    trades=[
        [12345, 1234, 30, 504, True],
        [12345, 1233, 31, 1003, True],
        [12345, 1235, 30, 1003, True],
        [98765, 1235, 31, 1003, True],
        [98765, 1235, 31, 1003, False],
    ]

    priced_depths = [1, 2, 3, 4]
    depths = [5, 6, 7, 8]

    output_matrix = remaining_trade_size_quadrant (trades, price_depths, depths, 2.0)


    print("That's All Folks")
