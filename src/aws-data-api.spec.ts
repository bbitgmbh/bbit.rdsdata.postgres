import { AwsDataApi } from './aws-data-api';

const dbUrl = process.env.AURORA_TEST_DB_URL;
if (!process.env.CI) {
  console.log('dbUrl', dbUrl);
}

// file.only

describe('config tests', () => {
  test('date queries', async () => {
    const client = new AwsDataApi(dbUrl, { formatOptions: { datetimeConverstion: 'convertToIsoString' } });

    const res = await client.query('SELECT NOW() as message'); // ?
    console.log('message res plain', res);
    expect(res.records[0].message.length).toBeGreaterThan(0);

    const res2 = await client.query('SELECT CAST(NOW() AS DATE) as message'); // ?
    console.log('message res', res2);
    expect(res2.records[0].message.length).toBeGreaterThan(0);
  });
});
