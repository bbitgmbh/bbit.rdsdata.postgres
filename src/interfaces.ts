import * as AWS from 'aws-sdk';
import { AwsDataApiDbCluster } from './aws-data-api-db-cluster';

export interface IAwsDataApiConfig extends IAwsDataApiQueryParams {
  cluster: AwsDataApiDbCluster;
  engine?: 'postgres' | 'mysql';
  transactionId?: string;
}

export interface IAwsDataApiQueryParams {
  schema?: string;
  queryTimeout?: number;
  hydrateColumnNames?: boolean;
  maxConcurrentQueries?: number;
  formatOptions?: {
    deserializeDate?: boolean;
    treatAsLocalDate?: boolean;
    stringifyArrays?: boolean;
  };
  convertSnakeToCamel?: boolean;
}

export interface IAwsDataApiQueryResult {
  transactionId: string;
  transactionStatus?: string;
  columnMetadata?: AWS.RDSDataService.Metadata;
  numberOfRecordsUpdated?: number;
  records?: any[];
  insertId?: number;
}
