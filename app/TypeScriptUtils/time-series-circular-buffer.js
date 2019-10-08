"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
const moment_1 = __importDefault(require("moment"));
const assert_1 = __importDefault(require("assert"));
const process_1 = require("../Process/process");
class TimeSeriesCircularBuffer extends Map {
    constructor(maxAge) {
        super();
        this.maxAge = maxAge;
        this.enq = (ts, volume) => {
            this.set(ts, volume);
        };
        this.volume = () => {
            try {
                return Array
                    .from(this.values())
                    .reduce((sum, current) => {
                    if (typeof (current) === 'number') {
                        return sum + current;
                    }
                    else {
                        assert_1.default(false, `Software Failure`);
                        return 0;
                    }
                }, 0);
            }
            catch (err) {
                throw err;
            }
        };
        this.purge = () => {
            try {
                for (const ts of this.keys()) {
                    if (ts < +moment_1.default() - this.maxAge) {
                        this.delete(ts);
                    }
                }
            }
            finally {
                process_1.Process.active ? setTimeout(this.purge, 2000) : lodash_1.default.noop;
            }
        };
        setTimeout(this.purge, 2000);
    }
}
exports.TimeSeriesCircularBuffer = TimeSeriesCircularBuffer;
