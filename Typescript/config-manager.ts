// tslint:disable

import * as _ from 'lodash';
import * as assert from "assert";
import * as Ajv from "ajv";
// const Ajv = require('ajv');
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
        "simConfig": {
            "type": "array",
            "items": {
                "type": "object",
            }
        },
    },
    "required": [
        "name",
        "envId",
        "minNotional",
        "trim",
        "parallelSimulations",
        "simConfig",
    ]
}

export interface ISimConfiguration {

    readonly name: string;
    readonly envId: number;
    readonly trim: boolean;
    readonly minNotional: number;
    readonly parallelSimulations: number;
    readonly simConfig: any;
}

export const configValidator = (simConfig: ISimConfiguration):

    boolean => {

    try {

        // Validate the configuration
        const ajv = new Ajv({
            allErrors: true,
            useDefaults: true,
            verbose: true
        });

        const validate = ajv.compile(simSchema);
        const valid = validate(simConfig);

        if (!valid || validate.errors) {

            console.log(`Invalid Configuration: ` +
                `${JSON.stringify(validate.errors, null, 4)}`);
            return false;

        } else {
            return true;
        }

    } catch (err) {
        throw err;
    }
}

