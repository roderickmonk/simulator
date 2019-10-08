"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
const now = require('performance-now');
const CircularBuffer = require('circular-buffer');
class TimeAverager {
    constructor(size = 100) {
        this.avg = (value = undefined) => {
            if (lodash_1.default.isUndefined(value)) {
                this.timeBuffer.enq(now() - this.now);
                this.now = now();
            }
            else {
                this.timeBuffer.enq(value);
            }
            return ((this.timeBuffer
                .toarray()
                .reduce((a, b) => a + b, 0)) / this.timeBuffer.size());
        };
        this.enq = (value) => {
            this.timeBuffer.enq(value);
        };
        this.size = () => this.timeBuffer.size();
        this.capacity = () => this.timeBuffer.capacity();
        this.clear = () => {
            while (this.timeBuffer.size() > 0)
                this.timeBuffer.pop();
        };
        this.get = () => {
            return ((this.timeBuffer
                .toarray()
                .reduce((a, b) => a + b, 0)) / this.timeBuffer.size());
        };
        this.timeBuffer = new CircularBuffer(size);
        this.now = now();
    }
}
exports.TimeAverager = TimeAverager;
