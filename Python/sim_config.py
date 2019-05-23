import os
from pymongo import MongoClient
import numpy as np
from numpy import ndarray
import logging
from schema import Schema, And, Use, Optional, SchemaError
import numpy as np

try:
    profile
except NameError:
    profile = lambda x: x
    
sim_id: str = None
orderbook_id: str = None
rate_precision: int = None
quantity_precision: int = None
partition_config: dict = {}
pdf_x: ndarray = None
pdf_y: ndarray = None


def check(conf_schema, conf):

    try:
        conf_schema.validate(conf)
        return True
    except SchemaError as err:
        logging.warn(err)
        return False


partition_schema = Schema({
    '_id': And(Use(str)),
    'quantityLimit': And(Use(float)),
    'inventoryLimit': And(Use(float)),
    'feeRate': And(Use(float)),
    'actualFeeRate': And(Use(float)),
    'allowOrderConflicts': And(Use(bool)),
    'tick': And(Use(float)),
    'pdf': And(Use(list)),
}, ignore_extra_keys=True)


def init(config):

    global rate_precision
 
    try:

        partition_config = config

        if __debug__:
            logging.debug(f'partition_config: {partition_config}')

        if not check(partition_schema, partition_config):
            raise Exception('Invalid Trader Configuration')

        # if pdf_x.size == None:
        #    get_pdf(partition_config["pdf"])

        QL = partition_config["quantityLimit"]
        IL = partition_config["inventoryLimit"]
        rate_precision = - int(np.log10(partition_config["tick"]))

        return None

    except:
        raise


def get_pdf(sim_db, pdf: str):

    global pdf_x
    global pdf_y

    try:

        remote_mongo_client = MongoClient(os.environ['MONGODB'])
        sim_db = remote_mongo_client.sim

        PDFs_collection = sim_db.PDFs

        pdf = PDFs_collection.find_one({"name": pdf})

        pdf_x = np.array(pdf["x"])
        assert len(pdf_x) > 0

        pdf_y = np.array(pdf["y"])
        assert len(pdf_y) > 0

        assert pdf_x.shape == pdf_y.shape

        pdf_x = np.power(10, pdf_x)

        return None

    except:
        raise
