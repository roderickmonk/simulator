#!/usr/bin/env node

const assert = require('assert');
import * as _ from "lodash";
const chalk = require('chalk');
import { debug } from "./debug";
import { simParams } from "./sim-params"

import {
    Collection,
    Db,
    MongoClient,
    MongoError,
    ObjectID,
    ObjectId,
} from "mongodb";

import {
    configValidator,
    SimConfiguration,
} from "./config-manager";

const GenerateSchema = require('generate-schema')

export class ConfigGenerator {

    private propertyLength = new Map();
    private propertyLevel = new Map();
    private propertyData = new Map();
    private propertySchema = new Map();

    public config: SimConfiguration | null = null;

    constructor(
        private configName: string,
        private simDb: Db) { }

    public validateSimConfig = (simConfig: Array<Array<object> | object>) => {

        // Capture Schema Output
        let schema: any = GenerateSchema.json('Product', simConfig).items.oneOf;

        console.log({ schema });

        schema.shift(); // Skip first one

        let properties: string[] = [];
        for (const level of schema) {

            debug('level:\n', JSON.stringify(level, null, 4));

            for (const prop of Object.keys(level.properties)) {

                properties.push(prop);

                this.propertySchema.set(prop, level.properties[prop]);
            }
        }

        for (const [prop, entry] of this.propertySchema.entries()) {

            assert(entry.type === 'array', `Parameter "${prop}" Data Not Array`);

            if (simParams.hasOwnProperty(prop)) {

                // This test only applies if it is a known parameter
                assert(
                    //@ts-ignore
                    simParams[prop].items.type === entry.items.type,
                    `Parameter "${prop}" Wrong Type`
                );
            }

            // Ensure required params 
            Object.keys(simParams).forEach(property => {

                assert(
                    this.propertySchema.has(property),
                    `Required Parameter "${property}" Not Found`
                );
            })
        }

        assert(
            (new Set(properties)).size === properties.length,
            'Duplicate Parameters Detected',
        );

        for (const [i, config] of simConfig.entries()) {

            const levelConfig = i === 0 ?
                //@ts-ignore
                config["0"] :
                config;

            debug({ levelConfig });

            for (const prop in levelConfig) {

                debug(chalk.red(`${prop}[${levelConfig[prop]}]`));

                assert(
                    levelConfig[prop].length > 0,
                    'Empty Configuration Parameter Array'
                );
                this.propertyLength.set(prop, levelConfig[prop].length);
                this.propertyData.set(prop, levelConfig[prop]);

                this.propertyLevel.set(prop, i);
            }
        }

        debug(this.propertyData);
        debug(this.propertyLevel);
        debug(this.propertyLength);

        const levels = simConfig.length;
        debug({ levels });

        for (let i = 0; i < simConfig.length; ++i) {

            const levelProperties = Array.from(this.propertyLevel)
                .filter(level => level[1] === i)
                .map(level => level[0]);

            debug({ levelProperties });

            const levelPropertiesSameLength =
                levelProperties
                    .map(prop => this.propertyLength.get(prop))
                    .every((val, i, arr) => val === arr[0]);


            assert(
                levelPropertiesSameLength,
                'Mismatched Parameter Array Lengths'
            );
        }

        debug('levels: ', schema.length);

    }

    public * generate(level: number): any {

        const levelProperties: Array<string> = Array.from(this.propertyLevel)
            .filter(l => l[1] === level)
            .map(l => l[0]);

        if (levelProperties.length === 0) {

            yield {};

        } else {

            const levelDepth = this.propertyData.get(levelProperties[0]).length;

            for (let i = 0; i < levelDepth; ++i) {

                const returnObj = {};

                for (const prop of levelProperties) {

                    //@ts-ignore
                    returnObj[prop] = this.propertyData.get(prop)[i];
                }

                const genLoop = this.generate(level + 1);

                while (true) {

                    const next = genLoop.next();

                    if (next.done) break;

                    yield { ...returnObj, ...next.value };
                }
            }
        }
    }

    public getGenerator = async (): Promise<any> => {

        try {

            // ToDo: the following is required but strange
            process.on('unhandledRejection', _.noop);

            this.config = await this.simDb
                .collection('configurations')
                .findOne({ name: this.configName });

            assert(this.config, 'Unknown Configuration');

            const validConfig = await configValidator(this.config!);

            if (validConfig) {
                this.validateSimConfig(this.config!.simConfig);
                return this.generate(0);

            } else {
                return Promise.reject(new Error('Invalid Configuration'));
            }


        } catch (err) {
            return Promise.reject(err);
        }

    }
}
