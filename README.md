# bbit.rdsdata.postgres

***WARNING - WORK IN PROGRESS***

The goal of this project is to provide a node-postgres compatible client that connects to the AWS Aurora Postgres database over the AWS RDS Data HTTP API. This way we can connect node-postgres supporting libraries like ORMs to an AWS RDS database without the need for a proper VPC setup. It just uses HTTP with AWS IAM authentication, as we know it from AWS-SDK, S3, DynamoDB, etc.

## Features

* node-postgres compatible client with
  * Support for named parameters with double point sign
  * Support for positional parameters with dollar sign
  * Support for transactions
  * Support for postgres array datatype
* TypeScript support
* General RDS Data API Client with
  * automatic SQL and parameters preparation to AWS format
  * automatic response parsing from AWS format
  * promisified interfaces
  * configurable maximum concurrent sql statements, defaults to 1 per client instance

## How to use - Examples

NOTE: see test files under src/*.spec.ts for more examples.

### Install with peer dependencies

```
npm i @bbitgmbh/bbit.rdsdata.postgres aws-sdk luxon --save
```


### as a node-postgres replacement

```typescript
import { Client } from '@bbitgmbh/bbit.rdsdata.postgres';

// we introduced a special connection string url for this wrapper to be compatible with existing libraries:
const client = new Client(`awsrds://${encodeURIComponent(databaseName)}:${encodeURIComponent(awsSecretName)}@${awsRegion}.${awsAccount}.aws/${encodeURIComponent(awsRdsClustername)}`);

await client.connect();
const res = await client.query('select table_name from information_schema.tables where table_name = :name ', { name: 'pg_tables' });

/*
res = {
  rows: [{ table_name: 'pg_tables' }]
}
*/

await client.end();

```

### with Sequelize ORM

```typescript
import pg = require('@bbitgmbh/bbit.rdsdata.postgres');

const connectionParams = (new pg.Client(`awsrds://${encodeURIComponent(databaseName)}:${encodeURIComponent(awsSecretName)}@${awsRegion}.${awsAccount}.aws/${encodeURIComponent(awsRdsClustername)}`)).dataApiRetrievePostgresDataApiClientConfig();

/*
connectionParams = {
  user: 'aws:eu-central-1',
  password: 'arn:aws:secretsmanager:eu-central-1:xxxxx:secret:rds-db-credentials/cluster-xxxxxx/postgres-xxxxx',
  host: 'arn:aws:rds:eu-central-1:xxxxxx:cluster:xxxxxx',
  port: 443,
  database: 'xxxxxx'
}
*/

const sequelize = new Sequelize({
      ...(connectionParams as any),
      dialect: 'postgres',
      dialectModule: pg,
    });

User.init(
  {
    id: {
      type: DataTypes.INTEGER.UNSIGNED,
      autoIncrement: true,
      primaryKey: true,
    },
    name: {
      type: new DataTypes.STRING(128),
      allowNull: false,
    },
    preferredName: {
      type: new DataTypes.STRING(128),
      allowNull: true,
    },
  },
  {
    tableName: 'users',
    sequelize, // passing the `sequelize` instance is required
  },
);

await sequelize.sync();

const newUser = await User.create({
  name: 'Johnny',
  preferredName: 'John',
});

const foundUser = await User.findOne({ where: { name: 'Johnny' } });

console.log(foundUser.name); // Johnny

await sequelize.close();

```

## Background - why I did this
When we started to go serverless with API Gateway and AWS Lambda, we soon recognized that RDS Database connection handling is hard. There are many great blog posts on the internet about, to summarize those:

1. In an AWS Lambda, you shouldn't use connection pools and you should open/close the database connection on every event to prevent timeout issues and to prevent crashing the database server with too many concurrent connections.
2. To be able to connect fast, you wanna use AWS RDS Proxy. RDS Proxy also helps prevent issues with maximal open concurrent connections to the database, so that lambda can scale without having to worry about that. But this also has a price tag
3. To be able to connect at all, your AWS Lambda needs to be in a proper configured VPC. When your Lambda needs Internet-Access or needs to access an AWS resource where you didn't set up a VPC Endpoint, you need at least one NAT-Gateway, which also has a price tag.

If you wanna go around all those challenges, there is the AWS RDS Data API, which lets you execute SQL statements over HTTP with the usual AWS IAM authentication. But this introduces other challenges:

1. the request/response format does not match with the ones from node-postgres. When you use an ORM like sequelize, this is not usable.
2. transactions do have a dedicated API, where you start a transaction, get a transaction id, run queries with this transaction id, and then either commit or rollback
3. when a DDL statement terminates before it is finished running, it can result in errors and possibly corrupted data structures. To continue running a statement after a call time out, we need to specify the "continue-after-timeout" option.

This project tries to solve those challenges by providing the missing piece of software to combine classic node-postgres with RDS Data HTTP API.

## Limitations and Issues
![Main](https://github.com/bbitgmbh/bbit.rdsdata.postgres/workflows/Main/badge.svg)
[![codecov](https://codecov.io/gh/bbitgmbh/bbit.rdsdata.postgres/branch/master/graph/badge.svg)](https://codecov.io/gh/bbitgmbh/bbit.rdsdata.postgres)

We are in the process to find and fix them. If you find an issue, please provide detailed info. Pull requests are very welcome.

### Database name may not contain URL sensitive chars like double point
While postgres is supporting chars like double point, it looks like AWS RDS Data API does not properly escape those. Same issue exists when trying to connect by AWS Console.

### Asynchronous notification will not work
Due to the request/response nature of the http protocol asynchronous database notifications can not be transmitted back to the client.

### Postgres special datatypes like Name, Geometry, UUID, etc. are not supported
Workaround: Cast them in your sql statement to something else, for instance a varchar(255). Example:

```
-- following statements selects all tables with their column names
-- field attname of table pg_class as has name datatype, therefore we cast this with cast(a.attname as varchar(512)) to a string

SELECT i.relname AS tablename, array_agg(cast(a.attname as varchar(512))) AS column_names 
	FROM pg_class t, pg_class i, pg_index ix, pg_attribute a WHERE t.oid = ix.indrelid AND i.oid = ix.indexrelid AND a.attrelid = t.oid AND t.relkind = 'r'
GROUP BY i.relname ORDER BY i.relname
```


## Performance
some manual comparisons showed that executing statements over Data API has little overhead (middle two digit millisecs).
### Tips
* Reuse HTTP-Connections with keep-alive
  * Either set environment variable AWS_NODEJS_CONNECTION_REUSE_ENABLED = 1
  * or inject your preprepared HTTP client in AWS-SDK, for details see: https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/node-reusing-connections.html

## How to setup AWS RDS Data API
See https://github.com/jeremydaly/data-api-client#enabling-data-api

## Acknowledgments

* I rewrote and extended the great work of https://github.com/jeremydaly/data-api-client in typescript and with focus for postgresql compatibilty.

## License
MIT
