import yargs from 'yargs/yargs';
// eslint-disable-next-line import/no-extraneous-dependencies
import cubejs, { Query, CubejsApi } from '@cubejs-client/core';
// eslint-disable-next-line import/no-extraneous-dependencies
import { afterAll, beforeAll, expect, jest } from '@jest/globals';
import { BirdBox, startBirdBoxFromCli, startBirdBoxFromContainer } from '../src';

// eslint-disable-next-line import/no-extraneous-dependencies
require('jest-specific-snapshot');

const DB_TYPES = ['athena', 'bigquery'];
type DbType = typeof DB_TYPES[number];

const SERVER_MODES = ['cli', 'docker', 'local'];
type ServerMode = typeof SERVER_MODES[number];

interface Args {
  type: DbType
  envFile: string
  mode: ServerMode
}

const args: Args = yargs(process.argv.slice(2))
  .exitProcess(false)
  .options(
    {
      type: {
        choices: DB_TYPES,
        demandOption: true,
        describe: 'db type',
      },
      envFile: {
        alias: 'env-file',
        demandOption: true,
        describe: 'path to .env file with db config & auth env variables',
        type: 'string',
      },
      mode: {
        choices: SERVER_MODES,
        default: 'docker',
        describe: 'how to stand up the server',
      }
    }
  )
  .argv as Args;

const name = `${args.type}`;

describe(name, () => {
  jest.setTimeout(60 * 5 * 1000);

  let birdbox: BirdBox;
  let httpClient: CubejsApi;

  beforeAll(async () => {
    try {
      switch (args.mode) {
        case 'cli':
        case 'local': {
          birdbox = await startBirdBoxFromCli(
            {
              cubejsConfig: 'single/cube.js',
              dbType: args.type,
              useCubejsServerBinary: args.mode === 'local',
              envFile: args.envFile,
              extraEnv: {
                CUBEJS_SCHEDULED_REFRESH_DEFAULT: 'false',
                CUBEJS_EXTERNAL_DEFAULT: 'true',
              }
            }
          );
          break;
        }

        case 'docker': {
          birdbox = await startBirdBoxFromContainer(
            {
              name,
              envFile: args.envFile
            }
          );
          break;
        }

        default: {
          throw new Error(`Bad serverMode ${args.mode}`);
        }
      }

      httpClient = cubejs(async () => 'test', {
        apiUrl: birdbox.configuration.apiUrl,
      });
    } catch (e) {
      console.log(e);
      throw e;
    }
  });

  afterAll(async () => {
    await birdbox.stop();
  });

  it('Driver.query', async () => {
    const response = await httpClient.load(
      {
        measures: ['Orders.totalAmount'],
        dimensions: ['Orders.status'],
      }
    );
    // ../.. to move out of dist/test directory
    // @ts-ignore
    expect(response.rawData()).toMatchSpecificSnapshot(
      `../../test/__snapshots__/${name}.query.snapshot`
    );
  });

  describe('filters', () => {
    type QueryTestOptions = {
      name: string;
      ws?: true,
    };

    const containsAsserts: [options: QueryTestOptions, query: Query][] = [
      [
        {
          name: '#1 Orders.status.contains: ["e"]',
        },
        {
          measures: [
            'Orders.count'
          ],
          filters: [
            {
              member: 'Orders.status',
              operator: 'contains',
              values: ['e'],
            },
          ],
        },
      ], [
        {
          name: '#2 Orders.status.contains: ["es"]',
        },
        {
          measures: [
            'Orders.count'
          ],
          filters: [
            {
              member: 'Orders.status',
              operator: 'contains',
              values: ['es'],
            },
          ],
        },
      ], [
        {
          name: '#3 Orders.status.contains: ["es", "w"]',
        },
        {
          measures: [
            'Orders.count'
          ],
          filters: [
            {
              member: 'Orders.status',
              operator: 'contains',
              values: ['es', 'w'],
            },
          ],
        },
      ], [
        {
          name: '#3 Orders.status.contains: ["a"]',
        },
        {
          measures: [
            'Orders.count'
          ],
          filters: [
            {
              member: 'Orders.status',
              operator: 'contains',
              values: ['a'],
            },
          ],
        },
      ],
    ];
    const startsWithAsserts: [options: QueryTestOptions, query: Query][] = [
      [
        {
          name: '#1 Orders.status.startsWith: ["a"]',
        },
        {
          measures: [
            'Orders.count'
          ],
          filters: [
            {
              member: 'Orders.status',
              operator: 'startsWith',
              values: ['a'],
            },
          ],
        },
      ], [
        {
          name: '#2 Orders.status.startsWith: ["n"]',
        },
        {
          measures: [
            'Orders.count'
          ],
          filters: [
            {
              member: 'Orders.status',
              operator: 'startsWith',
              values: ['n'],
            },
          ],
        },
      ], [
        {
          name: '#3 Orders.status.startsWith: ["p"]',
        },
        {
          measures: [
            'Orders.count'
          ],
          filters: [
            {
              member: 'Orders.status',
              operator: 'startsWith',
              values: ['p'],
            },
          ],
        },
      ], [
        {
          name: '#4 Orders.status.startsWith: ["sh"]',
        },
        {
          measures: [
            'Orders.count'
          ],
          filters: [
            {
              member: 'Orders.status',
              operator: 'startsWith',
              values: ['sh'],
            },
          ],
        },
      ], [
        {
          name: '#5 Orders.status.startsWith: ["n", "p", "s"]',
        },
        {
          measures: [
            'Orders.count'
          ],
          filters: [
            {
              member: 'Orders.status',
              operator: 'startsWith',
              values: ['n', 'p', 's'],
            },
          ],
        },
      ],
    ];
    const endsWithAsserts: [options: QueryTestOptions, query: Query][] = [
      [
        {
          name: '#1 Orders.status.endsWith: ["a"]',
        },
        {
          measures: [
            'Orders.count'
          ],
          filters: [
            {
              member: 'Orders.status',
              operator: 'endsWith',
              values: ['a'],
            },
          ],
        },
      ], [
        {
          name: '#2 Orders.status.endsWith: ["w"]',
        },
        {
          measures: [
            'Orders.count'
          ],
          filters: [
            {
              member: 'Orders.status',
              operator: 'endsWith',
              values: ['w'],
            },
          ],
        },
      ], [
        {
          name: '#3 Orders.status.endsWith: ["sed"]',
        },
        {
          measures: [
            'Orders.count'
          ],
          filters: [
            {
              member: 'Orders.status',
              operator: 'endsWith',
              values: ['sed'],
            },
          ],
        },
      ], [
        {
          name: '#4 Orders.status.endsWith: ["ped"]',
        },
        {
          measures: [
            'Orders.count'
          ],
          filters: [
            {
              member: 'Orders.status',
              operator: 'endsWith',
              values: ['ped'],
            },
          ],
        },
      ], [
        {
          name: '#4 Orders.status.endsWith: ["w", "sed", "ped"]',
        },
        {
          measures: [
            'Orders.count'
          ],
          filters: [
            {
              member: 'Orders.status',
              operator: 'endsWith',
              values: ['w', 'sed', 'ped'],
            },
          ],
        },
      ],
    ];

    describe('contains', () => {
      // eslint-disable-next-line no-restricted-syntax
      for (const [options, query] of containsAsserts) {
        // eslint-disable-next-line no-loop-func
        it(`${options.name}`, async () => {
          const response = await httpClient.load(query);
          // @ts-ignore
          expect(response.rawData()).toMatchSpecificSnapshot(
            `../../test/__snapshots__/${name}.query.snapshot`
          );
        });
      }
    });

    describe('startsWith', () => {
      // eslint-disable-next-line no-restricted-syntax
      for (const [options, query] of startsWithAsserts) {
        // eslint-disable-next-line no-loop-func
        it(`${options.name}`, async () => {
          const response = await httpClient.load(query);
          // @ts-ignore
          expect(response.rawData()).toMatchSpecificSnapshot(
            `../../test/__snapshots__/${name}.query.snapshot`
          );
        });
      }
    });

    describe('endsWith', () => {
      // eslint-disable-next-line no-restricted-syntax
      for (const [options, query] of endsWithAsserts) {
        // eslint-disable-next-line no-loop-func
        it(`${options.name}`, async () => {
          const response = await httpClient.load(query);
          // @ts-ignore
          expect(response.rawData()).toMatchSpecificSnapshot(
            `../../test/__snapshots__/${name}.query.snapshot`
          );
        });
      }
    });
  });
});
