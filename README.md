# bbit.postgresql.rds

WARNING - WORK IN PROGRESS

The idea of this project is to provide a node-postgres compatible postgresql client which connects to the AWS Aurora database over the AWS HTTP Data API. This way you can connect any ORM supporting node-postgres to your AWS RDS instance without having to deal with a proper VPC setup and native database connections. Just connect from everywhere and let the AWS SDK handle the authentication, like it works for S3, DynamoDB, etc.

## Limitations
see https://github.com/jeremydaly/data-api-client#enabling-data-api


## Performance
Benchmarks are ToDo, but here is what we observed manually:

- on first query there we observed an overhead of 200-300ms (probalby startup of container which transforms your http request into native database request)
- on following query overhead is around 10-50ms, depending on result size

## How to setup AWS Data API
https://github.com/jeremydaly/data-api-client#enabling-data-api

