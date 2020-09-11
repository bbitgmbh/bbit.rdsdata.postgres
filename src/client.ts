import {
  ClientConfig,
  QueryArrayConfig,
  QueryArrayResult,
  QueryConfig,
  QueryResult,
  QueryResultRow,
  Submittable,
} from "pg";

import * as dataApiClient from "data-api-client";
import { isFunction, isString } from "lodash";

export class Client {
  private _client: any = null;
  constructor(config?: string | ClientConfig) {}

  connect(callback?: (err: Error) => void): Promise<void> {
    const promise = async (): Promise<void> => {
      this._client = dataApiClient({
        secretArn:
          "arn:aws:secretsmanager:us-east-1:XXXXXXXXXXXX:secret:mySecret",
        resourceArn:
          "arn:aws:rds:us-east-1:XXXXXXXXXXXX:cluster:my-cluster-name",
        database: "myDatabase", // default database
      });
    };

    if (callback) {
      promise().then(
        () => callback(null),
        (err) => callback(err)
      );
    }

    return promise();
  }

  query(
    query: QueryArrayConfig<any> | QueryConfig<any> | string | Submittable,
    valuesOrCallback: any,
    callback: (err: Error, result: any) => void
  ): Promise<any> {
    if (isFunction(valuesOrCallback)) {
      callback = valuesOrCallback;
      valuesOrCallback = undefined;
    }

    const promise = async (): Promise<any> => {

        switch(true) {
            case isString(query):
                let result = await this._client.query(query);
                return {
                    rowCount: result.records.length,
                    rows: result.records
                } as QueryResult<any>;

            default:
                throw new Error('unknown query type');
        }

    };

    if (callback) {
      promise().then(
        (result) => callback(null, result),
        (err) => callback(err, null)
      );
    }

    return promise();
  }
  // tslint:enable:no-unnecessary-generics

  // copyFrom(queryText: string): stream.Writable;
  // copyTo(queryText: string): stream.Readable;

  pauseDrain(): void {
    throw new Error("not implemented");
  }
  resumeDrain(): void {
    throw new Error("not implemented");
  }

  escapeIdentifier(str: string): string {
    throw new Error("not implemented");
  }

  escapeLiteral(str: string): string {
    throw new Error("not implemented");
  }

  on(
    event: "drain" | "error" | "notice" | "notification" | "end",
    listener: (param?: Error | Notification) => void
  ): this {
    return this;
  }

  end(callback?: (err: Error) => void): Promise<void> {
    const promise = async (): Promise<void> => {
      this._client = null;
    };

    if (callback) {
      promise().then(
        () => callback(null),
        (err) => callback(err)
      );
    }

    return promise();
  }
}
