#!/usr/bin/env python

import os
from pymongo import MongoClient
import numpy as np
import matplotlib.pyplot as plt
from numpy import ndarray

sim_trade_collection = None
sim_id = None
partition_id = None
orderbook_id = None
quantity_precision = None
trader_config: dict = None
pdf_x: ndarray
pdf_y: ndarray


def init():

    try:

        remote_mongo_client = MongoClient(os.environ['MONGODB'])
        if remote_mongo_client == None:
            raise Exception('Unable to Connect to Database')

        # Prep for db access
        config_db = remote_mongo_client.configuration
        if config_db == None:
            raise Exception('Unable to Open "configuration" Database')

        PDFs_collection = config_db.PDFs

        # Load and check the pdf
        pdf = PDFs_collection.find_one({"name": "pdf1"})

        pdf_x = np.array(pdf["x"])
        assert len(pdf_x) > 0

        pdf_y = np.array(pdf["y"])
        assert len(pdf_y) > 0

        assert pdf_x.shape == pdf_y.shape

        pdf_x = np.power(10, pdf_x)
        # pdf_y = np.power(10, pdf_y)

        print('pdf_x.shape: ' + str (pdf_x.shape))
        print('pdf_x:\n' + str(pdf_x))

        print('pdf_y.shape: ' + str (pdf_y.shape))
        print('pdf_y:\n' + str(pdf_y))

        fig, ax = plt.subplots(1)

        # plot the data
        ax.plot(pdf_x, pdf_y)
        plt.show()

        return None

    except AssertionError:
        raise

    except Exception as err:
        raise


if __name__ == '__main__':

    try:
        init()

    except AssertionError as err:
        print ('Assertion Error: ' + str(err))
        raise

    except Exception as err:
        print(err)

    finally:
        quit()
