// tslint:disable

import * as _ from 'lodash';
import * as assert from "assert";
import * as Ajv from "ajv";
const ajv = new Ajv;

const simSchema = {

    "properties": {
        "name": {
            "type": "string",
        },
        "envId": {
            "type": "number",
        },
        "minNotional": {
            "type": "number",
            "default": 0.00000001
        },
        "trim": {
            "type": "boolean",
        },
        "parallelSimulations": {
            "type": "number",
        },
        "saveRedis": {
            "type": "boolean",
            "default": false,
        },
        "onlyOrderbooksWithTrades": {
            "type": "boolean",
            "default": true,
        },
    },
    "required": [
        "name",
        "envId",
        "minNotional",
        "trim",
        "parallelSimulations",
        "saveRedis",
        "onlyOrderbooksWithTrades",
        "multiplyConfig",
    ]
}

export type MultiplyConfig = Array<Array<object> | object>;

export interface SimConfiguration {

    readonly name: string;
    readonly envId: number;
    readonly trim: boolean;
    readonly minNotional: number;
    readonly parallelSimulations: number;
    readonly onlyOrderbooksWithTrades: boolean;
    readonly saveRedis: boolean;
    readonly multiplyConfig: MultiplyConfig;
    multiplyConfigParams: string;
}

export const configValidator = (simConfig: SimConfiguration):

    Promise<boolean> => {

    try {

        // Validate the configuration
        const ajv = new Ajv({
            allErrors: true,
            useDefaults: true,
            verbose: false,
        });

        const validate = ajv.compile(simSchema);
        const valid = validate(simConfig);

        if (!valid || validate.errors) {

            const msg = `Invalid Configuration: ` +
                `${JSON.stringify(validate.errors, null, 4)}`;
            return Promise.reject(new Error(msg));
        }

        return Promise.resolve(true);

    } catch (err) {
        return Promise.reject(err);
    }
}

