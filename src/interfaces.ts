import * as AWS from 'aws-sdk';

export interface IAwsDataRawApiConfig {
  schema?: string;
  rdsOptions?: AWS.RDSDataService.ClientConfiguration;
  client?: AWS.RDSDataService;
  queryTimeoutInMS?: number;
}

export interface IAwsDataApiQueryParams extends IAwsDataRawApiConfig {
  hydrateColumnNames?: boolean;
  maxConcurrentQueries?: number;
  formatOptions?: {
    deserializeDate?: boolean;
    treatAsLocalDate?: boolean;
    stringifyArrays?: boolean;
  };
  convertSnakeToCamel?: boolean;
}

export interface IAwsDataApiConfig extends IAwsDataApiQueryParams {
  transactionId?: string;
}

export interface IAwsDataApiQueryResult {
  transactionId: string;
  transactionStatus?: string;
  columnMetadata?: AWS.RDSDataService.Metadata;
  numberOfRecordsUpdated?: number;
  records?: any[];
  insertId?: number;
}
