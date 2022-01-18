import math
from datetime import datetime
from typing import Optional

from bson.objectid import ObjectId
from pydantic import BaseModel


class Trader(BaseModel):
    name: str
    optimized: bool = False


class TimeFrame(BaseModel):
    startTime: datetime
    endTime: datetime


class SimulationConfig(BaseModel):
    runId: ObjectId
    simVersion: Optional[str]
    configName: str
    ts: datetime
    status: str
    envId: int
    minNotional: float
    trim: Optional[bool]
    saveRedis: bool = False
    exchange: str
    market: str
    actualFeeRate: float
    allowOrderConflicts: bool
    depth: float
    pdf: str
    feeRate: float
    tick: float
    partitions: int
    QL: float
    IL: float
    timeFrame: TimeFrame
    trader: Trader
    precision: int

    class Config:
        arbitrary_types_allowed = True


class PartitionConfig(BaseModel):
    runId: ObjectId
    simVersion: Optional[int]
    configName: str
    simId: ObjectId
    partition: int
    ts: datetime
    status: str
    envId: int
    minNotional: float
    trim: Optional[bool]
    saveRedis: bool = False
    exchange: str
    actualFeeRate: float
    pdf: str
    feeRate: float
    QL: float
    IL: float
    allowOrderConflicts: bool
    depth: float
    precision: int
    trader: str
    market: str
    startTime: datetime
    endTime: datetime

    class Config:
        arbitrary_types_allowed = True
