import os
import numpy as np
import logging
from schema import Schema, And, Use, Optional, SchemaError
import numpy as np


sim_id: str = None
orderbook_id: str = None
# rate_precision: int = None
# quantity_precision: int = None
# partition_config: dict = {}
# sim_db = None
# sim_configuration_db = None


# def check(conf_schema, conf):

#     try:
#         conf_schema.validate(conf)
#         return True
#     except SchemaError as err:
#         logging.warn(err)
#         return False


# partition_schema = Schema(
#     {
#         "_id": And(Use(str)),
#         "QL": And(Use(float)),
#         "IL": And(Use(float)),
#         "feeRate": And(Use(float)),
#         "actualFeeRate": And(Use(float)),
#         "tick": And(Use(float)),
#     },
#     ignore_extra_keys=True,
# )


# def init(config):

#     global rate_precision

#     try:

#         partition_config = config

#         if __debug__:
#             logging.debug(f"partition_config: {partition_config}")

#         if not check(partition_schema, partition_config):
#             raise Exception("Invalid Partition Configuration")

#         rate_precision = -int(np.log10(partition_config["tick"]))

#         return None

#     except:
#         raise
