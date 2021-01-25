import * as AWS from 'aws-sdk';

export interface IAwsDataRawApiConfig {
  schema?: string;
  rdsOptions?: AWS.RDSDataService.ClientConfiguration;
  client?: AWS.RDSDataService;
  queryTimeoutInMS?: number;
  awaitStartup?: boolean;
  maxConcurrency?: number;
  sqlMonkeyPatchers?: { [key: string]: (sql: string) => string };
}

export interface IAwsDataApiQueryParams extends IAwsDataRawApiConfig {
  hydrateColumnNames?: boolean;
  maxConcurrentQueries?: number;
  formatOptions?: {
    datetimeConverstion?: 'keepSQLFormat' | 'convertToJsDate' | 'convertToIsoString';
    treatAsTimeZone?: string; // local, utc, Europe/Zurich, etc.
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
