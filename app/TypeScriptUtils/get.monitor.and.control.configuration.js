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
const Barrel_1 = require("../Barrel");
const envalid = require("envalid");
const { str } = envalid;
exports.getMonitorAndControlConfiguration = () => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const env = envalid.cleanEnv(process.env, {
            MONGODB: str(),
        });
        const mongoDbClient = yield Barrel_1.MongoClient.connect(env.MONGODB, {
            useNewUrlParser: true,
            useUnifiedTopology: true,
        });
        const db = mongoDbClient.db("configuration");
        const configCollection = db.collection("monitor.and.control");
        return yield configCollection.findOne({});
    }
    catch (err) {
        return Promise.reject(err);
    }
});
