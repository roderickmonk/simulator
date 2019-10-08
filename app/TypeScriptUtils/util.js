"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const lodash_1 = __importDefault(require("lodash"));
exports.sortByObjectKeys = (obj) => {
    const sortedObj = {};
    Object.keys(obj)
        .sort((a, b) => lodash_1.default.toLower(a) >= lodash_1.default.toLower(b) ? 1 : -1)
        .forEach((key) => {
        return lodash_1.default.isObject(obj[key]) ?
            sortedObj[key] = exports.sortByObjectKeys(obj[key]) :
            sortedObj[key] = obj[key];
    });
    return sortedObj;
};
