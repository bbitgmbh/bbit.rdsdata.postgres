import { AwsDataApiDbCluster } from './aws-data-api-db-cluster';

const dbUrl = process.env.AURORA_TEST_DB_URL;

console.log(dbUrl);

describe('config tests', () => {
  test('get raw connection string', async () => {
    const client = new AwsDataApiDbCluster(dbUrl);

    const rawConfig = await client.postgresNativeClientConfig();
    console.log(rawConfig);

    expect(rawConfig.user.length).toBeGreaterThan(0);
  });
});
