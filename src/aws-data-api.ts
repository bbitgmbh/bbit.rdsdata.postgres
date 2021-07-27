import { ClientConfig } from 'pg';
import { AwsDataFormat } from './aws-data-format';
import { AwsDataRawApi } from './aws-data-raw-api';
import { IAwsDataApiConfig, IAwsDataApiQueryParams, IAwsDataApiQueryResult } from './interfaces';
import { AwsDataApiUtils } from './utils';

export class AwsDataApi {
  private _config: IAwsDataApiConfig;
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

  async query(inputSQL: string, values?: any, queryParams?: IAwsDataApiQueryParams): Promise<IAwsDataApiQueryResult> {
    // ToDo: validate formatOptions
    const cleanedParams = Object.assign(
      { database: this.raw.database, schema: this.raw.schema },
      AwsDataApiUtils.pick(this._config, ['hydrateColumnNames', 'formatOptions', 'schema', 'convertSnakeToCamel']),
      queryParams || {},
    );

    let isDDLStatement = false;
    // Transactional overwrites
    switch (true) {
      case inputSQL.trim().substr(0, 'BEGIN'.length).toUpperCase() === 'BEGIN':
      case inputSQL.trim().substr(0, 'START TRANSACTION'.length).toUpperCase() === 'START TRANSACTION':
        const beginRes = await this.raw.beginTransaction(AwsDataApiUtils.pick(cleanedParams, ['schema', 'database']));
        this._config.transactionId = beginRes.transactionId;
        return { transactionId: beginRes.transactionId };

      case inputSQL.trim().substr(0, 'DISCARD ALL'.length).toUpperCase() === 'DISCARD ALL':
      case inputSQL.trim().substr(0, 'COMMIT'.length).toUpperCase() === 'COMMIT':
      case inputSQL.trim().substr(0, 'ROLLBACK'.length).toUpperCase() === 'ROLLBACK':
        const currentTransactionId = this._config.transactionId;

        if (currentTransactionId) {
          if (inputSQL.trim().substr(0, 'DISCARD ALL'.length).toUpperCase() === 'DISCARD ALL') {
            inputSQL = 'ROLLBACK';
          }
          const isCommit = inputSQL.trim().substr(0, 'COMMIT'.length).toUpperCase() === 'COMMIT';

          const params = {
            transactionId: currentTransactionId,
          };

          let commitRes: any = {};
          try {
            commitRes = await (isCommit ? this.raw.commitTransaction(params) : this.raw.rollbackTransaction(params));
          } catch (err) {
            console.error('TRANSACTION ERROR', err);
            this._config.transactionId = null;
            commitRes.transactionStatus = 'ERROR: ' + err;
          } finally {
            this._config.transactionId = null;
          }

          return {
            transactionId: currentTransactionId,
            transactionStatus: commitRes.transactionStatus,
          };
        }

      case inputSQL.trim().substr(0, 'CREATE'.length).toUpperCase() === 'CREATE':
      case inputSQL.trim().substr(0, 'DROP'.length).toUpperCase() === 'DROP':
      case inputSQL.trim().substr(0, 'ALTER'.length).toUpperCase() === 'ALTER':
        isDDLStatement = true;
        break;
    }

    const preparedSQL = AwsDataFormat.prepareSqlAndParams(inputSQL, values, cleanedParams);

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
              records: AwsDataFormat.formatRecords(result.records, result.columnMetadata, cleanedParams),
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
