import * as AWS from 'aws-sdk';
import {
  DbName,
  Id,
  ResultSetOptions,
  SqlParameterSets,
  SqlParametersList,
  SqlRecords,
  SqlStatement,
} from 'aws-sdk/clients/rdsdataservice';
import * as sqlString from 'sqlstring';
import { IAwsDataApiConfig, IAwsDataApiQueryParams, IAwsDataApiQueryResult } from './interfaces';
import { Utils } from './utils';

// Supported value types in the Data API
const supportedTypes = ['arrayValue', 'blobValue', 'booleanValue', 'doubleValue', 'isNull', 'longValue', 'stringValue', 'structValue'];

export class AwsDataApi {
  // Simple error function
  static error(...err) {
    throw Error(...err);
  }

  // Normize parameters so that they are all in standard format
  static normalizeParams(paramsToNormalize: (Record<any, any> | { name: string; value: any })[]): { name: string; value: any }[] {
    return paramsToNormalize.reduce<{ name: string; value: any }[]>(
      (acc, p) =>
        Array.isArray(p)
          ? acc.concat(AwsDataApi.normalizeParams(p))
          : Object.keys(p).length === 2 && p.name && p.value
          ? acc.concat([p as any])
          : acc.concat(AwsDataApi.splitParams(p)),
      [],
    );
  }

  // Prepare parameters
  static processParams(sql: string, sqlParams, paramsToProcess, formatOptions, row = 0) {
    return {
      processedParams: paramsToProcess.reduce((acc, p) => {
        if (Array.isArray(p)) {
          const result = AwsDataApi.processParams(sql, sqlParams, p, formatOptions, row);
          if (row === 0) {
            sql = result.escapedSql;
            row++;
          }
          return acc.concat([result.processedParams]);
        } else if (sqlParams[p.name]) {
          if (sqlParams[p.name].type === 'n_ph') {
            acc.push(AwsDataApi.formatParam(p.name, p.value, formatOptions));
          } else if (row === 0) {
            const regex = new RegExp('::' + p.name + '\\b', 'g');
            sql = sql.replace(regex, sqlString.escapeId(p.value));
          }
          return acc;
        } else {
          return acc;
        }
      }, []),
      escapedSql: sql,
    };
  }

  // Converts parameter to the name/value format
  static formatParam(n, v, formatOptions) {
    return AwsDataApi.formatType(n, v, AwsDataApi.getType(v), AwsDataApi.getTypeHint(v), formatOptions);
  }

  // Converts object params into name/value format
  static splitParams<K extends string | number | symbol, T>(p: Record<K, T>): { name: K; value: T }[] {
    return Object.keys(p).reduce((arr, x) => arr.concat({ name: x, value: p[x] }), []);
  }

  // Get all the sql parameters and assign them types
  static prepareSqlAndParams(
    sql: string,
    values: any,
    queryParams: IAwsDataApiQueryParams,
  ): { sql: string; parameters?: SqlParametersList; includeResultMetadata: boolean } {
    if (/\$(\d+)/.test(sql)) {
      // we have positional parameters like $1, convert them to named ones

      const namedParams = {};

      sql = sql.replace(/\$(\d+)/gi, (_, p1) => {
        namedParams['posparam' + p1] = values[parseInt(p1, 10) - 1];

        // ToDo: move this elsewhere and only check strings
        const dateCheck = Date.parse(namedParams['posparam' + p1]);
        if (dateCheck !== NaN && dateCheck > 0) {
          namedParams['posparam' + p1] = new Date(dateCheck);
        }

        return ':posparam' + p1;
      });

      values = [namedParams];
    }

    if (values === undefined) {
      values = [];
    }

    if (Utils.isObject(values) && !Array.isArray(values)) {
      values = [values];
    }

    if (!Array.isArray(values)) {
      AwsDataApi.error('Values must be an object or array');
    }

    // Parse and normalize parameters
    const parameters = AwsDataApi.normalizeParams(values);

    const parameterLabelAndTypes = (sql.match(/:{1,2}[\w\d]+/g) || [])
      .map((p) => {
        // TODO: future support for placeholder parsing?
        // return p === '??' ? { type: 'id' } // identifier
        //   : p === '?' ? { type: 'ph', label: '__d'+i  } // placeholder
        return p.startsWith('::')
          ? { type: 'n_id', label: p.substr(2) } // named id
          : { type: 'n_ph', label: p.substr(1) }; // named placeholder
      })
      .reduce((acc, x) => {
        return Object.assign(acc, {
          [x.label]: {
            type: x.type,
          },
        });
      }, {});

    // Process parameters and escape necessary SQL
    const { processedParams, escapedSql } = AwsDataApi.processParams(sql, parameterLabelAndTypes, parameters, queryParams.formatOptions);

    const returnVal: { sql: string; parameters?: SqlParametersList; includeResultMetadata: boolean } = {
      sql: escapedSql,
      includeResultMetadata: true,
    };
    if (processedParams && processedParams.length > 0) {
      returnVal.parameters = processedParams;
    }

    if (!queryParams.hydrateColumnNames) {
      returnVal.includeResultMetadata = false;
    }

    return returnVal;
  }

  // Gets the value type and returns the correct value field name
  // TODO: Support more types as the are released
  static getType(val: any): string {
    return typeof val === 'string'
      ? 'stringValue'
      : typeof val === 'boolean'
      ? 'booleanValue'
      : typeof val === 'number' && parseInt(val as any) === val
      ? 'longValue'
      : typeof val === 'number' && parseFloat(val as any) === val
      ? 'doubleValue'
      : val === null
      ? 'isNull'
      : Utils.isDate(val)
      ? 'stringValue'
      : Buffer.isBuffer(val)
      ? 'blobValue'
      : // : Array.isArray(val) ? 'arrayValue' This doesn't work yet
      // TODO: there is a 'structValue' now for postgres
      typeof val === 'object' && Object.keys(val).length === 1 && supportedTypes.includes(Object.keys(val)[0])
      ? null
      : undefined;
  }

  // Hint to specify the underlying object type for data type mapping
  static getTypeHint(val) {
    return Utils.isDate(val) ? 'TIMESTAMP' : undefined;
  }

  // Creates a standard Data API parameter using the supplied inputs
  static formatType(name, value, type, typeHint, formatOptions) {
    return Object.assign(
      typeHint != null ? { name, typeHint } : { name },
      type === null
        ? { value }
        : {
            value: {
              [type ? type : AwsDataApi.error(`'${name}' is an invalid type`)]:
                type === 'isNull'
                  ? true
                  : Utils.isDate(value)
                  ? AwsDataApi.formatToTimeStamp(value, formatOptions && formatOptions.treatAsLocalDate)
                  : value,
            },
          },
    );
  }

  // Formats the (UTC) date to the AWS accepted YYYY-MM-DD HH:MM:SS[.FFF] format
  // See https://docs.aws.amazon.com/rdsdataservice/latest/APIReference/API_SqlParameter.html
  static formatToTimeStamp(date, treatAsLocalDate) {
    const pad = (val, num = 2) => '0'.repeat(num - (val + '').length) + val;

    const year = treatAsLocalDate ? date.getFullYear() : date.getUTCFullYear();
    const month = (treatAsLocalDate ? date.getMonth() : date.getUTCMonth()) + 1; // Convert to human month
    const day = treatAsLocalDate ? date.getDate() : date.getUTCDate();

    const hours = treatAsLocalDate ? date.getHours() : date.getUTCHours();
    const minutes = treatAsLocalDate ? date.getMinutes() : date.getUTCMinutes();
    const seconds = treatAsLocalDate ? date.getSeconds() : date.getUTCSeconds();
    const ms = treatAsLocalDate ? date.getMilliseconds() : date.getUTCMilliseconds();

    const fraction = ms <= 0 ? '' : `.${pad(ms, 3)}`;

    return `${year}-${pad(month)}-${pad(day)} ${pad(hours)}:${pad(minutes)}:${pad(seconds)}${fraction}`;
  }

  static formatRecords(recs: SqlRecords, columns: AWS.RDSDataService.Metadata, params: IAwsDataApiQueryParams) {
    if (params.convertSnakeToCamel) {
      columns.filter((c) => c.label.includes('_')).forEach((c) => (c.label = Utils.snakeToCamel(c.label)));
    }

    const fieldMap: { label: string; typeName: string; fieldKey: string }[] =
      recs && recs[0]
        ? recs[0].map<{ label: string; typeName: string; fieldKey: string }>((x, i) => ({
            label: columns && columns.length ? columns[i].label : 'col' + i,
            typeName: columns && columns.length ? columns[i].typeName : undefined,
            fieldKey: Object.keys(x).filter((type) => type !== 'isNull' && !!x[type])[0],
          }))
        : [];

    // Map over all the records (rows)
    return recs
      ? recs.map((rec) => {
          // Reduce each field in the record (row)
          return rec.reduce((acc, field, i) => {
            // If the field is null, always return null
            if (field.isNull === true) {
              return params.hydrateColumnNames // object if hydrate, else array
                ? Object.assign(acc, { [fieldMap[i].label]: null })
                : acc.concat(null);
            }

            const value = AwsDataApi.deserializeRecordValue(field[fieldMap[i].fieldKey], fieldMap[i], params);

            return params.hydrateColumnNames // object if hydrate, else array
              ? Object.assign(acc, { [fieldMap[i].label]: value })
              : acc.concat(value);
          }, (params.hydrateColumnNames ? {} : []) as any); // init object if hydrate, else init array
        })
      : [];
  }

  // Format record value based on its value, the database column's typeName and the formatting options
  static deserializeRecordValue(value: any, field: { label: string; typeName: string; fieldKey: string }, params: IAwsDataApiQueryParams) {
    if (field.fieldKey === 'arrayValue') {
      const arrayFieldName = Object.keys(value).filter((type) => type !== 'isNull' && !!value[type])[0];

      const arrValue = value[arrayFieldName].map((e: any) =>
        AwsDataApi.deserializeRecordValue(e, { ...field, fieldKey: arrayFieldName }, params),
      );
      if (params?.formatOptions?.stringifyArrays) {
        return JSON.stringify(arrValue);
      }

      return arrValue;
    }

    return params?.formatOptions?.deserializeDate && ['DATE', 'DATETIME', 'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE'].includes(field.typeName)
      ? AwsDataApi.formatFromTimeStamp(
          value,
          (params.formatOptions && params.formatOptions.treatAsLocalDate) || field.typeName === 'TIMESTAMP WITH TIME ZONE',
        )
      : value;
  }

  // Converts the string value to a Date object.
  // If standard TIMESTAMP format (YYYY-MM-DD[ HH:MM:SS[.FFF]]) without TZ + treatAsLocalDate=false then assume UTC Date
  // In all other cases convert value to datetime as-is (also values with TZ info)
  static formatFromTimeStamp(value: string, treatAsLocalDate: boolean): Date {
    return !treatAsLocalDate && /^\d{4}-\d{2}-\d{2}(\s\d{2}:\d{2}:\d{2}(\.\d{3})?)?$/.test(value) ? new Date(value + 'Z') : new Date(value);
  }

  // Format updateResults and extract insertIds
  static formatUpdateResults(res) {
    return res.map((x) => {
      return x.generatedFields && x.generatedFields.length > 0 ? { insertId: x.generatedFields[0].longValue } : {};
    });
  }

  private _config: IAwsDataApiConfig;
  private _rds: AWS.RDSDataService;
  private _serializingQueue: {
    sql: string;
    values?: any;
    queryParams?: IAwsDataApiQueryParams;
    running: boolean;
    resolve: (value: IAwsDataApiQueryResult) => void;
    reject: (err: Error) => void;
  }[] = [];

  constructor(params: IAwsDataApiConfig) {
    if (!Utils.isString(params.secretArn)) {
      AwsDataApi.error("'secretArn' string value required");
    }

    if (!Utils.isString(params.resourceArn)) {
      AwsDataApi.error("'resourceArn' string value required");
    }

    if (params.database !== undefined && !Utils.isString(params.database)) {
      AwsDataApi.error("'database' string value required");
    }

    if (typeof params.hydrateColumnNames !== 'boolean') {
      params.hydrateColumnNames = true;
    }

    if (!Utils.isObject(params.formatOptions)) {
      params.formatOptions = {} as any;
    }

    this._config = Utils.mergeConfig({ hydrateColumnNames: true }, params);

    if (params.options !== undefined && !Utils.isObject(params.options)) {
      throw new Error('param-options-must-be-an-object');
    }

    this._rds = new AWS.RDSDataService(params.options);
  }

  query(sql: string, values?: any, queryParams?: IAwsDataApiQueryParams): Promise<IAwsDataApiQueryResult> {
    return this._internalQuery(sql, values, queryParams);
    /* return new Promise((resolve, reject) => {
      this._serializingQueue.push({
        sql,
        values,
        queryParams,
        running: false,
        resolve,
        reject,
      });

      if (this._serializingQueue.length === 1) {
        this._dequeSQL();
      }
    }); */
  }

  private async _dequeSQL() {
    if (this._serializingQueue.length === 0) return;

    const input = this._serializingQueue[0];
    input.running = true;

    return this._internalQuery(input.sql, input.values, input.queryParams).then(
      (result: any) => {
        input.running = false;
        this._serializingQueue.shift();

        input.resolve(result);
        this._dequeSQL();
      },
      (error: Error) => {
        input.running = false;
        this._serializingQueue.shift();

        input.reject(error);
      },
    );
  }

  private async _internalQuery(inputsql: string, values?: any, queryParams?: IAwsDataApiQueryParams): Promise<IAwsDataApiQueryResult> {
    // ToDo: validate formatOptions
    const cleanedParams = Object.assign(
      Utils.pick(this._config, ['hydrateColumnNames', 'formatOptions', 'database', 'convertSnakeToCamel']),
      queryParams || {},
    );

    // Transactional overwrites
    switch (true) {
      case inputsql.trim().substr(0, 'BEGIN'.length).toUpperCase() === 'BEGIN':
        const beginRes = await this.beginTransaction();
        this._config.transactionId = beginRes.transactionId;
        return { transactionId: beginRes.transactionId };

      case inputsql.trim().substr(0, 'COMMIT'.length).toUpperCase() === 'COMMIT':
        const commitRes = {
          transactionId: this._config.transactionId,
          transactionStatus: (await this.commitTransaction({ transactionId: this._config.transactionId })).transactionStatus,
        };
        this._config.transactionId = null;
        return commitRes;

      case inputsql.trim().substr(0, 'ROLLBACK'.length).toUpperCase() === 'ROLLBACK':
        const rollbackRes = {
          transactionId: this._config.transactionId,
          transactionStatus: (await this.rollbackTransaction({ transactionId: this._config.transactionId })).transactionStatus,
        };
        this._config.transactionId = null;
        return rollbackRes;
    }

    const preparedSQL = AwsDataApi.prepareSqlAndParams(inputsql, values, cleanedParams);

    // ToDo continueAfterTimeout if its a DDL statement

    // Create/format the parameters
    const params = {
      ...Utils.pick(cleanedParams, ['schema', 'database']),
      ...preparedSQL,
      ...(this._config.transactionId ? { transactionId: this._config.transactionId } : {}),
    };

    try {
      const result = await this.executeStatement(params);
      console.log('query params', JSON.stringify(params, null, 3), ' --> ', result.records);
      return Object.assign(
        { columnMetadata: result.columnMetadata, transactionId: this._config.transactionId },
        result.numberOfRecordsUpdated !== undefined && !result.records ? { numberOfRecordsUpdated: result.numberOfRecordsUpdated } : {},
        result.records
          ? {
              records: AwsDataApi.formatRecords(result.records, result.columnMetadata, cleanedParams),
            }
          : {},
        // updateResults ? { updateResults: AwsDataApi.formatUpdateResults(updateResults) } : {},
        result.generatedFields && result.generatedFields.length > 0 ? { insertId: result.generatedFields[0].longValue } : {},
      );
    } catch (e) {
      console.error('query params', JSON.stringify(params, null, 3), e);
      throw e;
    }
  }

  /* ToDo async batch(..._args) {
    // Flatten array if nested arrays (fixes #30)
    const args = Array.isArray(_args[0]) ? Utils.flatten(_args) : _args;

    // Parse and process sql
    const sql = AwsDataApi.parseSQL(args);
    const sqlParams = AwsDataApi.getSqlParams(sql);

    // Parse hydration setting
    const hydrateColumnNames = AwsDataApi.parseHydrate(this._config, args);

    // Parse data format settings
    const formatOptions = AwsDataApi.parseFormatOptions(this._config, args);

    // Parse and normalize parameters
    const parameters = AwsDataApi.normalizeParams(AwsDataApi.parseParams(args));

    // Process parameters and escape necessary SQL
    const { processedParams, escapedSql } = AwsDataApi.processParams(sql, sqlParams, parameters, formatOptions);

    // Determine if this is a batch request
    const isBatch = processedParams.length > 0 && Array.isArray(processedParams[0]) ? true : false;

    // Create/format the parameters
    const params = Object.assign(
      AwsDataApi.prepareParams(this._config, args),
      {
        database: AwsDataApi.parseDatabase(this._config, args), // add database
        sql: escapedSql, // add escaped sql statement
      },
      // Only include parameters if they exist
      processedParams.length > 0
        ? // Batch statements require parameterSets instead of parameters
          { [isBatch ? 'parameterSets' : 'parameters']: processedParams }
        : {},
      // Force meta data if set and not a batch
      hydrateColumnNames && !isBatch ? { includeResultMetadata: true } : {},
      // If a transactionId is passed, overwrite any manual input
      this._config.transactionId ? { transactionId: this._config.transactionId } : {},
    ); // end params

    const result = await this._rds.batchExecuteStatement(params).promise();

    return { updateResults: AwsDataApi.formatUpdateResults(result.updateResults) };

    // return AwsDataApi.formatResults(result, hydrateColumnNames, args[0].includeResultMetadata === true ? true : false, formatOptions);
  } */

  async transaction<T>(params: { schema?: string }, lambda: (client: AwsDataApi) => Promise<T>): Promise<T> {
    const transactionalClient = new AwsDataApi({ ...this._config, transactionId: null });

    await transactionalClient.query('BEGIN');

    let res;
    try {
      res = await lambda(transactionalClient);
      await transactionalClient.query('COMMIT');
    } catch (e) {
      await transactionalClient.query('ROLLBACK');
      throw e;
    }

    return res;
  }

  batchExecuteStatement(args: {
    /**
     * The parameter set for the batch operation. The SQL statement is executed as many times as the number of parameter sets provided. To execute a SQL statement with no parameters, use one of the following options:   Specify one or more empty parameter sets.   Use the ExecuteStatement operation instead of the BatchExecuteStatement operation.    Array parameters are not supported.
     */
    parameterSets?: SqlParameterSets;

    /**
     * The name of the database schema.
     */
    schema?: DbName;

    /**
     * The SQL statement to run.
     */
    sql: SqlStatement;
    /**
     * The identifier of a transaction that was started by using the BeginTransaction operation. Specify the transaction ID of the transaction that you want to include the SQL statement in. If the SQL statement is not part of a transaction, don't set this parameter.
     */
    transactionId?: Id;
  }) {
    return this._rds
      .batchExecuteStatement(Utils.mergeConfig(Utils.pick(this._config, ['resourceArn', 'secretArn', 'database', 'schema']), args))
      .promise();
  }

  beginTransaction(args?: {
    /**
     * The name of the database schema.
     */
    schema?: DbName;
  }) {
    return this._rds
      .beginTransaction(Utils.mergeConfig(Utils.pick(this._config, ['resourceArn', 'secretArn', 'database', 'schema']), args || {}))
      .promise();
  }

  executeStatement(args: {
    /**
     * A value that indicates whether to continue running the statement after the call times out. By default, the statement stops running when the call times out.  For DDL statements, we recommend continuing to run the statement after the call times out. When a DDL statement terminates before it is finished running, it can result in errors and possibly corrupted data structures.
     */
    continueAfterTimeout?: boolean;
    /**
     * A value that indicates whether to include metadata in the results.
     */
    includeResultMetadata?: boolean;
    /**
     * The parameters for the SQL statement.  Array parameters are not supported.
     */
    parameters?: SqlParametersList;
    /**
     * Options that control how the result set is returned.
     */
    resultSetOptions?: ResultSetOptions;
    /**
     * The name of the database schema.
     */
    schema?: DbName;
    /**
     * The SQL statement to run.
     */
    sql: SqlStatement;
    /**
     * The identifier of a transaction that was started by using the BeginTransaction operation. Specify the transaction ID of the transaction that you want to include the SQL statement in. If the SQL statement is not part of a transaction, don't set this parameter.
     */
    transactionId?: Id;
  }) {
    return this._rds
      .executeStatement(Utils.mergeConfig(Utils.pick(this._config, ['resourceArn', 'secretArn', 'database', 'schema']), args))
      .promise();
  }

  commitTransaction(args: {
    /**
     * The identifier of the transaction to end and commit.
     */
    transactionId: Id;
  }) {
    return this._rds.commitTransaction(Utils.mergeConfig(Utils.pick(this._config, ['resourceArn', 'secretArn']), args)).promise();
  }

  rollbackTransaction(args: {
    /**
     * The identifier of the transaction to roll back.
     */
    transactionId: Id;
  }) {
    return this._rds.rollbackTransaction(Utils.mergeConfig(Utils.pick(this._config, ['resourceArn', 'secretArn']), args)).promise();
  }
}
