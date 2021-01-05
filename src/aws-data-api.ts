import * as AWS from 'aws-sdk';
import { SqlParametersList, SqlRecords } from 'aws-sdk/clients/rdsdataservice';
import { ClientConfig } from 'pg';
import * as sqlString from 'sqlstring';
import { AwsDataRawApi } from './aws-data-raw-api';
import { IAwsDataApiConfig, IAwsDataApiQueryParams, IAwsDataApiQueryResult } from './interfaces';
import { AwsDataApiUtils } from './utils';

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

        // ToDo: find out if this check needs to be also done for named parametes, not only positional ones
        if (
          AwsDataApiUtils.isString(namedParams['posparam' + p1]) &&
          /^\d{4}[-_]\d{2}[-_]\d{0,2}\s\d{2}:\d{0,2}:\d{0,2}/.test(namedParams['posparam' + p1])
        ) {
          const dateCheck = Date.parse(namedParams['posparam' + p1]);
          if (dateCheck !== NaN) {
            namedParams['posparam' + p1] = new Date(dateCheck);
          }
        }

        return ':posparam' + p1;
      });

      values = [namedParams];
    }

    if (values === undefined) {
      values = [];
    }

    if (AwsDataApiUtils.isObject(values) && !Array.isArray(values)) {
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
      : AwsDataApiUtils.isDate(val)
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
    return AwsDataApiUtils.isDate(val) ? 'TIMESTAMP' : undefined;
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
                  : AwsDataApiUtils.isDate(value)
                  ? AwsDataApi.formatToTimeStamp(value, formatOptions && formatOptions.treatAsLocalDate)
                  : value,
            },
          },
    );
  }

  // Formats the (UTC) date to the AWS accepted YYYY-MM-DD HH:MM:SS[.FFF] format
  // See https://docs.aws.amazon.com/rdsdataservice/latest/APIReference/API_SqlParameter.html
  static formatToTimeStamp(date: Date, treatAsLocalDate: boolean) {
    const pad = (val: number, num = 2) => '0'.repeat(num - (val + '').length) + val;

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
      columns.filter((c) => c.label.includes('_')).forEach((c) => (c.label = AwsDataApiUtils.snakeToCamel(c.label)));
    }

    const fieldMap: { label: string; typeName: string; fieldKey: string }[] =
      recs && recs[0]
        ? recs[0].map<{ label: string; typeName: string; fieldKey: string }>((x, i) => ({
            label: columns && columns.length ? columns[i].label : 'col' + i,
            typeName: columns && columns.length ? columns[i].typeName : undefined,
            fieldKey: Object.keys(x).filter((type) => type !== 'isNull' && x[type] !== undefined && x[type] !== null)[0],
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
  private _serializingQueue: {
    sql: string;
    values?: any;
    queryParams?: IAwsDataApiQueryParams;
    running: boolean;
    resolve: (value: IAwsDataApiQueryResult) => void;
    reject: (err: Error) => void;
  }[] = [];
  public raw: AwsDataRawApi;

  constructor(public readonly connectionConfig: string | ClientConfig, additionalConfig?: IAwsDataApiConfig) {
    if (!additionalConfig) {
      additionalConfig = {};
    }

    this.raw = new AwsDataRawApi(connectionConfig, additionalConfig);

    if (typeof additionalConfig.hydrateColumnNames !== 'boolean') {
      additionalConfig.hydrateColumnNames = true;
    }

    if (!AwsDataApiUtils.isObject(additionalConfig.formatOptions)) {
      additionalConfig.formatOptions = {} as any;
    }

    this._config = AwsDataApiUtils.mergeConfig({ hydrateColumnNames: true }, additionalConfig);
  }

  clearQueue() {
    this._serializingQueue = [];
  }

  query(sql: string, values?: any, queryParams?: IAwsDataApiQueryParams): Promise<IAwsDataApiQueryResult> {
    // return this._internalQuery(sql, values, queryParams);
    return new Promise((resolve, reject) => {
      this._serializingQueue.push({
        sql,
        values,
        queryParams,
        running: false,
        resolve,
        reject,
      });

      if (this._serializingQueue.length <= (this._config.maxConcurrentQueries || 1)) {
        this._dequeSQL();
      }
    });
  }

  private _dequeSQL() {
    if (this._serializingQueue.length === 0) return;

    const input = this._serializingQueue[0];
    input.running = true;

    return this._internalQuery(input.sql, input.values, input.queryParams).then(
      (result: any) => {
        input.running = false;
        this._serializingQueue.shift();

        input.resolve(result);
        this._dequeSQL();
        return Promise.resolve();
      },
      (error: Error) => {
        input.running = false;
        this._serializingQueue.shift();

        input.reject(error);
        this._dequeSQL();
        return Promise.resolve();
      },
    );
  }

  private async _internalQuery(inputsql: string, values?: any, queryParams?: IAwsDataApiQueryParams): Promise<IAwsDataApiQueryResult> {
    // ToDo: validate formatOptions
    const cleanedParams = Object.assign(
      { database: this.raw.databaseName, schema: this.raw.schema },
      AwsDataApiUtils.pick(this._config, ['hydrateColumnNames', 'formatOptions', 'schema', 'convertSnakeToCamel']),
      queryParams || {},
    );

    let isDDLStatement = false;
    // Transactional overwrites
    switch (true) {
      case inputsql.trim().substr(0, 'BEGIN'.length).toUpperCase() === 'BEGIN':
        const beginRes = await this.raw.beginTransaction();
        this._config.transactionId = beginRes.transactionId;
        return { transactionId: beginRes.transactionId };

      case inputsql.trim().substr(0, 'COMMIT'.length).toUpperCase() === 'COMMIT':
        const commitRes = {
          transactionId: this._config.transactionId,
          transactionStatus: (await this.raw.commitTransaction({ transactionId: this._config.transactionId })).transactionStatus,
        };
        this._config.transactionId = null;
        return commitRes;

      case inputsql.trim().substr(0, 'ROLLBACK'.length).toUpperCase() === 'ROLLBACK':
        const rollbackRes = {
          transactionId: this._config.transactionId,
          transactionStatus: (await this.raw.rollbackTransaction({ transactionId: this._config.transactionId })).transactionStatus,
        };
        this._config.transactionId = null;
        return rollbackRes;

      case inputsql.trim().substr(0, 'CREATE'.length).toUpperCase() === 'CREATE':
      case inputsql.trim().substr(0, 'DROP'.length).toUpperCase() === 'DROP':
      case inputsql.trim().substr(0, 'ALTER'.length).toUpperCase() === 'ALTER':
        isDDLStatement = true;
        break;
    }

    const preparedSQL = AwsDataApi.prepareSqlAndParams(inputsql, values, cleanedParams);

    // Create/format the parameters
    const params = {
      ...AwsDataApiUtils.pick(cleanedParams, ['schema', 'database']),
      ...{ continueAfterTimeout: isDDLStatement },
      ...preparedSQL,
      ...(this._config.transactionId ? { transactionId: this._config.transactionId } : {}),
    };

    try {
      const result = await this.raw.executeStatement(params, { queryTimeoutInMS: queryParams?.queryTimeoutInMS });

      // console.log('query params', JSON.stringify(params, null, 3), ' --> ', result.records);
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
      console.error('on executeStatement ', JSON.stringify(params, null, 3), e);
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

  async transaction<T>(lambda: (client: AwsDataApi) => Promise<T>): Promise<T> {
    const transactionalClient = new AwsDataApi(this.connectionConfig, { ...this._config, transactionId: null });

    await transactionalClient.query('BEGIN');

    let res: T;
    try {
      res = await lambda(transactionalClient);
      await transactionalClient.query('COMMIT');
    } catch (e) {
      await transactionalClient.query('ROLLBACK');
      throw e;
    }

    return res;
  }
}
