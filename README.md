# bbit.postgresql.rds

WARNING - WORK IN PROGRESS

The idea of this project is to provide a node-postgres compatible postgresql client which connects to the AWS Aurora database over the AWS HTTP Data API. This way you can connect any ORM supporting node-postgres to your AWS RDS instance without having to deal with a proper VPC setup and native database connections. Just connect from everywhere and let the AWS SDK handle the authentication, like it works for S3, DynamoDB, etc.

## Limitations
see https://github.com/jeremydaly/data-api-client#enabling-data-api


## Performance
ToDo

## How to setup AWS Data API
https://github.com/jeremydaly/data-api-client#enabling-data-api

