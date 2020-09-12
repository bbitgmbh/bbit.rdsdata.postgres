import { ClientConfig, QueryArrayConfig, QueryArrayResult, QueryConfig, QueryResult, QueryResultRow, Submittable } from 'pg';
import * as AWS from 'aws-sdk';
import * as dataApiClient from 'data-api-client';
import { isFunction, isString } from 'lodash';

export class Client {
  private _client: any = null;
  private _secretArn: string;
  private _clusterArn: string;
  private _databaseName: string;
  private _region: string;

  constructor(config?: string | ClientConfig) {
    console.log('config', config);

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
      this._clusterArn = `arn:aws:rds:${region}:${account}:cluster:${clusterName}`;
    } else {
      const [region, account] = config.host.split('.');

      this._region = region;
      this._databaseName = config.user;
      this._secretArn = `arn:aws:secretsmanager:${region}:${account}:secret:${config.password}`;
      this._clusterArn = `arn:aws:rds:${region}:${account}:cluster:${config.database}`;
    }
  }

  getConfig(): { secretArn: string; resourceArn: string; database: string; options: { region: string } } {
    return {
      secretArn: this._secretArn,
      resourceArn: this._clusterArn,
      database: this._databaseName,
      options: {
        region: this._region,
      },
    };
  }

  connect(callback?: (err: Error) => void): Promise<void> {
    const promise = async (): Promise<void> => {
      this._client = dataApiClient.default({
        ...this.getConfig(),
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
      console.log('query', query, valuesOrCallback);

      switch (true) {
        case isString(query):
          const result = await this._client.query(query);
          return {
            rowCount: result.records.length,
            rows: result.records,
          } as QueryResult<any>;

        default:
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

  on(event: 'drain' | 'error' | 'notice' | 'notification' | 'end', listener: (param?: Error | Notification) => void): this {
    console.log('on ', event, listener);
    return this;
  }

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
