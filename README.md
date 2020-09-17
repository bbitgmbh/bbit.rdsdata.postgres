# bbit.postgresql.rds

WARNING - WORK IN PROGRESS

The goal of this project is to provide a node-postgres compatible client that connects to the AWS Aurora Postgres database over the AWS RDS Data HTTP API. This way we can connect node-postgres supporting libraries like ORMs to an AWS RDS database without the need for a proper VPC setup. It just uses HTTP with AWS IAM authentication, as we know it from AWS-SDK, S3, DynamoDB, etc.

## Features

* node-postgres compatible client with
  * Support for named parameters
  * Support for positional parameters
  * Support for transactions
* TypeScript support (I rewrote and extended the great work of https://github.com/jeremydaly/data-api-client in typescript)
* General RDS Data API Client with
  * automatic SQL and parameters preparation to AWS format
  * automatic response parsing from AWS format
  * promisified interfaces

## How to use - Examples

### as a node-postgres replacement

```
import { Client } from 'bbit.rdsdata.postgres';

// we introduced a special connection string url for this wrapper to be compatible with existing libraries:
const client = new Client(`awsrds://${databaseName}:${awsSecretName}@${awsRegion}.${awsAccount}.aws/${awsRdsClustername}`);

await client.connect();
const res = await client.query('select table_name from information_schema.tables where table_name = :name ', { name: 'pg_tables' });

/*
res = {
  rows: [{ table_name: 'pg_tables' }]
}
*/

await client.end();

```

### with Sequlize ORM

```
import * as pg from 'bbit.rdsdata.postgres';

const connectionParams = (new pg.Client(`awsrds://${databaseName}:${awsSecretName}@${awsRegion}.${awsAccount}.aws/${awsRdsClustername}`)).dataApiRetrievePostgresDataApiClientConfig();

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
2. To be able to connect/disconnect fast, you wanna use AWS RDS Proxy. RDS Proxy also helps prevent issues with maximal open concurrent connections to the database, so that lambda can scale without having to worry about that. But this also has a price tag
3. To be able to connect/disconnect, your AWS Lambda needs to be in a proper configured VPC. When your Lambda needs Internet-Access or needs to access an AWS resource where you didn't set up a VPC Endpoint, you need at least one NAT-Gateway, which also has a price tag.

If you wanna go around all those challenges, there is the AWS RDS Data API, which lets you execute SQL statements over HTTP with their usual AWS IAM authentication. But this introduces other challenges:

1. the request/response format does not match with the ones from node-postgres. When you use an ORM like sequelize, this is not usable.
2. transactions do have a dedicated API, where you start a transaction, get a transaction id, run queries with this transaction id, and then either commit or rollback
3. when a DDL statement terminates before it is finished running, it can result in errors and possibly corrupted data structures. To continue running a statement after a call time out, we need to specify the "continue-after-timeout" option.

This project tries to solve those challenges to get around the other ones.

## Limitations
We are in the process to find and fix them

## Performance
* Reuse HTTP-Connections with keep-alive
  * Either set environment variable AWS_NODEJS_CONNECTION_REUSE_ENABLED = 1
  * or inject your preprepared HTTP client in AWS-SDK, for details see: https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/node-reusing-connections.html


## How to setup AWS RDS Data API
See https://github.com/jeremydaly/data-api-client#enabling-data-api

## License
MIT
