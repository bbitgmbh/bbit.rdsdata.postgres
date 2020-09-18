import * as AWS from 'aws-sdk';

export interface IAwsDataApiConfig extends IAwsDataApiQueryParams {
  secretArn: string;
  resourceArn: string;
  engine?: 'postgres' | 'mysql';
  transactionId?: string;
  options?: AWS.RDSDataService.ClientConfiguration;
  client?: AWS.RDSDataService;
}

export interface IAwsDataApiQueryParams {
  database?: string;
  schema?: string;
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
