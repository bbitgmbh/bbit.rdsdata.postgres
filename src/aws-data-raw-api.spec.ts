import { AwsDataRawApi } from './aws-data-raw-api';

const dbUrl = process.env.AURORA_TEST_DB_URL;
if (!process.env.CI) {
  console.log('dbUrl', dbUrl);
}

// file.only

describe('config tests', () => {
  test('get config test', async () => {
    const client = new AwsDataRawApi(dbUrl);
    expect(client.region.length).toBeGreaterThan(0);

    const rawConfig = await client.postgresNativeClientConfig();
    if (!process.env.CI) {
      console.log(rawConfig);
    }

    expect(rawConfig.user.length).toBeGreaterThan(0);

    const dataApiConfig = client.postgresDataApiClientConfig();
    if (!process.env.CI) {
      console.log(dataApiConfig);
    }
    expect(dataApiConfig.database.length).toBeGreaterThan(0);
  });

  test('db state', async () => {
    const client = new AwsDataRawApi(dbUrl);

    let isRunning: boolean;
    try {
      isRunning = await client.checkDbState();
    } catch (err) {
      err; //?
      expect(err.code).toBe('db-cluster-is-starting');
      return;
    }

    expect(isRunning).toBe(true);

    if (!process.env.CI) {
      console.log(client.getClusterInfo());
    }
  });
});
