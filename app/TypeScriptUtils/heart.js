"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const logger_1 = require("./logger");
class Heart {
    constructor(name) {
        this.name = name;
        this.heartbeat = false;
        this.active = false;
        this.stop = () => {
            clearInterval(this.timer);
        };
        this.beat = () => {
            this.heartbeat = true;
        };
        this.timeout = () => __awaiter(this, void 0, void 0, function* () {
            try {
                if (this.active && !this.heartbeat) {
                    logger_1.logWarn(`(${this.name}) Market Inactive`);
                }
                else {
                    this.heartbeat = false;
                }
            }
            catch (err) {
                throw err;
            }
        });
        this.active = true;
        this.timer = setInterval(this.timeout, 120000);
    }
}
exports.Heart = Heart;
