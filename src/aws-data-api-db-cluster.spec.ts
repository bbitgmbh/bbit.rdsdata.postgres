import { AwsDataApiDbCluster } from './aws-data-api-db-cluster';

const dbUrl = process.env.AURORA_TEST_DB_URL;

// file.only

console.log(dbUrl);

describe('config tests', () => {
  test('get cionfig test', async () => {
    const client = new AwsDataApiDbCluster(dbUrl);
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
    const client = new AwsDataApiDbCluster(dbUrl);

    try {
      const isRunning = await client.checkDbState({ triggerDatabaseStartup: true });
      expect(isRunning).toBe(true);
    } catch (err) {
      expect(err.code).toBe('db-cluster-is-starting');
      return;
    }

    if (!process.env.CI) {
      console.log(client.getClusterInfo());
    }
  });
});
