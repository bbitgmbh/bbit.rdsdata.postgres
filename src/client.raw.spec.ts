import { Client } from './client';

const dbUrl = process.env.AURORA_TEST_DB_URL;

console.log(dbUrl);

describe('Simulate raw postgres client', () => {
  test('load and index', async () => {
    const client = new Client(dbUrl);

    console.log(client.getConfig());

    await client.connect();
    const res = await client.query('SELECT NOW() as message');
    console.log(res.rows[0].message);
    await client.end();
  });
});
