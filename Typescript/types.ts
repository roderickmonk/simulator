
export enum MatchResult {
    NO_TRADES = 'No Trades',
    BLOCKED = 'Blocked',
    MIN_NOTIONAL_FAILURE = 'Min Notional Failure',
    UNMATCHABLE = 'Unmatchable',
    MATCHED = 'Matched',
}

export interface Trade {
    r: number;
    q: number;
}

export type Trades = Array<Trade>;

export type SimConfig = {
    sim_id: string;
    orderbook_id: string;
    rate_precision: number;
    quantity_precision: number;
    partition_config: any;
    pdf_x: Array<number>;
    pdf_y: Array<number>;

}