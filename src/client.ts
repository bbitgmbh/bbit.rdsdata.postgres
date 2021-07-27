import { ClientConfig, QueryArrayConfig, QueryArrayResult, QueryConfig, QueryResult, QueryResultRow, Submittable } from 'pg';
import { EventEmitter } from 'events';
import { AwsDataApi } from './aws-data-api';
import { AwsDataApiUtils } from './utils';

export class Connection extends EventEmitter {}

export class Client extends EventEmitter {
  public readonly dataApiClient: AwsDataApi = null;

  public connection: Connection = new Connection();

  constructor(config?: string | ClientConfig) {
    super();

    this.dataApiClient = new AwsDataApi(config, {
      formatOptions: {
        stringifyArrays: true,
        datetimeConverstion: 'keepSQLFormat',
      },
      sqlMonkeyPatchers: {
        sequelizeListDbWithColumns: (sql) => {
          if (/array_agg\(a.attname\) AS column_names.*from.*pg_attribute a/i.test(sql)) {
            // prevents https://github.com/bbitgmbh/bbit.rdsdata.postgres/issues/5
            console.info('monkey patching sequelize list db with columns sql', sql);
            return sql.replace(/array_agg\(a.attname\) AS column_names/gi, 'array_agg(cast(a.attname as varchar(512))) AS column_names');
          }

          return sql;
        },
      },
    });
  }

  connect(callback?: (err: Error) => void): Promise<void> {
    const promise = async (): Promise<void> => {
      await this.dataApiClient.raw.checkDbState({ awaitStartup: true });
    };

    if (callback) {
      return promise().then(
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
    if (AwsDataApiUtils.isFunction(valuesOrCallback)) {
      callback = valuesOrCallback;
      valuesOrCallback = undefined;
    }

    const promise = async (): Promise<any> => {
      switch (true) {
        case AwsDataApiUtils.isString(query):
          // console.log('query string', query, valuesOrCallback);
          const result = await this.dataApiClient.query(query as string, valuesOrCallback);
          // console.log('query string', query, valuesOrCallback, result.records);
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
      return promise().then(
        (result) => {
          callback(null, result);
          return result;
        },
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

  /* ToDo: alert when someone subscribes to those events
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
      await this.dataApiClient.raw.awaitCompleteOfAllPendingStatements();
    };

    if (callback) {
      return promise().then(
        () => callback(null),
        (err) => callback(err),
      );
    }

    return promise();
  }
}
