from enum import Enum

class MatchResult(Enum):
    NO_TRADES = 'No Trades'
    BLOCKED = 'Blocked'
    MIN_NOTIONAL_FAILURE = 'Min Notional Failure'
    UNMATCHABLE = 'Unmatchable'
    MATCHED = 'Matched'