import { ClientConfig, QueryArrayConfig, QueryArrayResult, QueryConfig, QueryResult, QueryResultRow, Submittable } from 'pg';
import { SecretsManager } from 'aws-sdk';
import * as dataApiClient from 'data-api-client';
import { isFunction, isString } from 'lodash';
import { EventEmitter } from 'events';

export class Connection extends EventEmitter {}

export class Client extends EventEmitter {
  private _client: any = null;
  private _secretArn: string;
  private _resourceArn: string;
  private _databaseName: string;
  private _region: string;

  public connection: Connection = new Connection();

  constructor(config?: string | ClientConfig) {
    super();
    if (!config) {
      return;
    }

    if (isString(config)) {
      // awsrds://{database}:{mysecret}@{region}.{account}.aws/{clustername}
      const url = new URL(config);
      if (url.protocol !== 'awsrds:') {
        throw new Error('unknown protocol ' + url.protocol);
      }
      const [region, account] = url.hostname.split('.');
      const clusterName = url.pathname.replace(/^\//, '');
      const secret = decodeURIComponent(url.password);

      this._region = region;
      this._databaseName = url.username;
      this._secretArn = `arn:aws:secretsmanager:${region}:${account}:secret:${secret}`;
      this._resourceArn = `arn:aws:rds:${region}:${account}:cluster:${clusterName}`;
    } else {
      const [, , service, region] = config.host.split(':');

      if (service !== 'rds') {
        throw new Error('host must be an AWS RDS arn');
      }

      this._region = region;
      this._databaseName = config.database;
      this._secretArn = config.password;
      this._resourceArn = config.host;
    }
  }

  dataApiGetAWSConfig(): { secretArn: string; resourceArn: string; database: string; options: { region: string } } {
    return {
      secretArn: this._secretArn,
      resourceArn: this._resourceArn,
      database: this._databaseName,
      options: {
        region: this._region,
      },
    };
  }

  dataApiRetrievePostgresDataApiClientConfig(): ClientConfig {
    return {
      user: 'aws:' + this._region,
      password: this._secretArn,
      host: this._resourceArn,
      port: 443,
      database: this._databaseName,
    } as any;
  }

  async dataApiRetrievePostgresNativeClientConfig(): Promise<ClientConfig> {
    // arn:aws:secretsmanager:eu-central-1:XXXXX:secret:rds-db-credentials/cluster-XXXXX/postgres-xxxx
    const [, , service, region, , type] = (this._secretArn || '').split(':');

    if (service !== 'secretsmanager') {
      throw new Error('secret arn must be a secretsmanager ARN');
    }

    if (type !== 'secret') {
      throw new Error('secret arn type must be secret');
    }

    const secretsClient = new SecretsManager({ region });

    const data = await secretsClient.getSecretValue({ SecretId: this._secretArn }).promise();

    const secretString = 'SecretString' in data ? data.SecretString : Buffer.from(data.SecretBinary as string, 'base64').toString('ascii');

    const values = JSON.parse(secretString);

    return {
      user: values.username,
      password: values.password,
      host: values.host,
      port: values.port,
      database: this._databaseName,
      awsDbInstanceIdentifier: values.dbInstanceIdentifier,
      awsEngine: values.engine,
      awsResourceId: values.resourceId,
    } as any;
  }

  connect(callback?: (err: Error) => void): Promise<void> {
    const promise = async (): Promise<void> => {
      this._client = dataApiClient.default({
        ...this.dataApiGetAWSConfig(),
      });
    };

    if (callback) {
      promise().then(
        () => callback(null),
        (err) => callback(err),
      );
    }

    return promise();
  }

  query(
    query: QueryArrayConfig<any> | QueryConfig<any> | string | Submittable,
    valuesOrCallback?: any,
    callback?: (err: Error, result: any) => void,
  ): Promise<QueryArrayResult<any> | QueryResultRow> {
    if (isFunction(valuesOrCallback)) {
      callback = valuesOrCallback;
      valuesOrCallback = undefined;
    }

    const promise = async (): Promise<any> => {
      switch (true) {
        case isString(query):
          const result = await this._client.query(query);
          console.log('query string', query, valuesOrCallback, result.records);
          return {
            rowCount: result.records?.length,
            rows: result.records || [],
          } as QueryResult<any>;

        default:
          console.error('query not implemented', query, valuesOrCallback);
          throw new Error('unknown query type');
      }
    };

    if (callback) {
      promise().then(
        (result) => callback(null, result),
        (err) => callback(err, null),
      );
    }

    return promise();
  }
  // tslint:enable:no-unnecessary-generics

  // copyFrom(queryText: string): stream.Writable;
  // copyTo(queryText: string): stream.Readable;

  pauseDrain(): void {
    throw new Error('not implemented');
  }
  resumeDrain(): void {
    throw new Error('not implemented');
  }

  escapeIdentifier(str: string): string {
    console.log('escapeIdentifier', str);
    throw new Error('not implemented');
  }

  escapeLiteral(str: string): string {
    console.log('escapeLiteral', str);
    throw new Error('not implemented');
  }

  /*
  on(event: 'drain' | 'error' | 'notice' | 'notification' | 'end', listener: (param?: Error | Notification) => void): this {
    console.log('on ', event, listener);
    return this;
  }

  once(event: 'drain' | 'error' | 'notice' | 'notification' | 'end', listener: (param?: Error | Notification) => void): this {
    console.log('once ', event, listener);
    return this;
  }
  */

  end(callback?: (err: Error) => void): Promise<void> {
    const promise = async (): Promise<void> => {
      this._client = null;
    };

    if (callback) {
      promise().then(
        () => callback(null),
        (err) => callback(err),
      );
    }

    return promise();
  }
}
