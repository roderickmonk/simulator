"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const assert = require("assert");
const types_1 = require("./types");
class MatchingEngine {
    constructor(assets, QL, IL, actualFeeRate, min_notional, tradesCollection) {
        this.assets = assets;
        this.QL = QL;
        this.IL = IL;
        this.actualFeeRate = actualFeeRate;
        this.min_notional = min_notional;
        this.tradesCollection = tradesCollection;
        this.buy_blocked_count = 0;
        this.buy_no_trades_count = 0;
        this.buy_notion_failure_count = 0;
        this.buy_match_count = 0;
        this.buy_unmatchable_count = 0;
        this.sell_blocked_count = 0;
        this.sell_match_count = 0;
        this.sell_no_trades_count = 0;
        this.sell_notion_failure_count = 0;
        this.sell_unmatchable_count = 0;
        this.sim_trades_idx = 0;
        this.match = (buy_rate, sell_rate, buy_trades, sell_trades) => {
            assert(buy_rate > 0 && sell_rate > 0);
            const [funds, inventory] = this.assets;
            const start_assets = this.assets;
            const buy_result = this.buy(start_assets, buy_rate, sell_rate, sell_trades);
            const sell_result = this.sell(start_assets, sell_rate, buy_trades);
            return [buy_result, sell_result];
        };
        this.buy = (start_assets, buy_rate, sell_rate, sell_trades) => {
            if (sell_trades.length() == 0) {
                this.buy_no_trades_count += 1;
                return types_1.MatchResult.NO_TRADES;
            }
            const [funds, inventory] = start_assets;
            const ceiling = this.IL - inventory * sell_rate;
            let quantity = Math.min(this.QL, funds, ceiling);
            if (ceiling <= 0 || funds <= 0) {
                this.buy_blocked_count += 1;
                return types_1.MatchResult.BLOCKED;
            }
            let matched = false;
            let match = 0;
            for (const trade of sell_trades) {
                if (trade.r <= buy_rate) {
                    if (quantity <= 0) {
                        if (matched) {
                            this.buy_match_count += 1;
                            return types_1.MatchResult.MATCHED;
                        }
                        else {
                            this.buy_unmatchable_count += 1;
                            return types_1.MatchResult.UNMATCHABLE;
                        }
                    }
                    const base = Math.min(quantity, trade.q * buy_rate);
                    const quote = base / buy_rate;
                    const fee = quote * this.actualFeeRate;
                    const notion = quote * buy_rate;
                    if (notion < this.min_notional) {
                        if (matched) {
                            this.buy_match_count += 1;
                            return types_1.MatchResult.MATCHED;
                        }
                        else {
                            this.buy_notion_failure_count += 1;
                            return types_1.MatchResult.MIN_NOTIONAL_FAILURE;
                        }
                    }
                    matched = true;
                    quantity -= base;
                    this.assets[0] += -base;
                    this.assets[1] += quote - fee;
                    this.sim_trades_idx += 1;
                    match += 1;
                }
            }
            if (matched) {
                this.buy_match_count += 1;
                return types_1.MatchResult.MATCHED;
            }
            else {
                this.buy_unmatchable_count += 1;
                return types_1.MatchResult.UNMATCHABLE;
            }
        };
        this.sell = (start_assets, sell_rate, buy_trades) => {
            const [, inventory] = start_assets;
            if (buy_trades.length() == 0) {
                this.sell_no_trades_count += 1;
                return types_1.MatchResult.NO_TRADES;
            }
            let quantity = Math.min(this.QL, inventory * sell_rate);
            if (quantity <= 0) {
                this.sell_blocked_count += 1;
                return types_1.MatchResult.BLOCKED;
            }
            let matched = false;
            let match = 0;
            for (const trade of buy_trades) {
                if (trade.r >= sell_rate) {
                    if (quantity <= 0) {
                        if (matched) {
                            this.sell_match_count += 1;
                            return types_1.MatchResult.MATCHED;
                        }
                        else {
                            this.sell_unmatchable_count += 1;
                            return types_1.MatchResult.UNMATCHABLE;
                        }
                    }
                    const base = Math.min(quantity, trade['q'] * sell_rate);
                    const quote = base / sell_rate;
                    const fee = base * this.actualFeeRate;
                    const notion = quote * sell_rate;
                    if (notion < this.min_notional) {
                        if (matched) {
                            this.sell_match_count += 1;
                            return types_1.MatchResult.MATCHED;
                        }
                        else {
                            this.sell_notion_failure_count += 1;
                            return types_1.MatchResult.MIN_NOTIONAL_FAILURE;
                        }
                    }
                    matched = true;
                    quantity -= base;
                    this.assets[0] += base - fee;
                    this.assets[1] += -quote;
                    this.sim_trades_idx += 1;
                    match += 1;
                }
            }
            if (matched) {
                this.sell_match_count += 1;
                return types_1.MatchResult.MATCHED;
            }
            else {
                this.sell_unmatchable_count += 1;
                return types_1.MatchResult.UNMATCHABLE;
            }
        };
        assert(QL > 0);
    }
}
exports.MatchingEngine = MatchingEngine;
