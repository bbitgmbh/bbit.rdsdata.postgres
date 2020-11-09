import { SecretsManager, RDSDataService, RDS } from 'aws-sdk';
import { ClientConfig } from 'pg';
import { AwsDataApiUtils, UnixEpochTimestamp } from './utils';
import { DbName, Id, ResultSetOptions, SqlParameterSets, SqlParametersList, SqlStatement } from 'aws-sdk/clients/rdsdataservice';
import { DBCluster } from 'aws-sdk/clients/rds';
import { IAwsDataRawApiConfig } from './interfaces';

export class AwsDataRawApi {
  public readonly secretArn: string;
  public readonly resourceArn: string;
  public readonly clusterIdentifier: string;
  public readonly databaseName: string;
  public readonly region: string;
  public readonly schema: string;

  private readonly _dbState: { isRunning: boolean; lastCheck: UnixEpochTimestamp } = { isRunning: false, lastCheck: null };
  private _rds: RDSDataService;
  private _clusterInfo: DBCluster;
  private _defaultQueryTimeoutInMS: number;

  public static MIN_AURORA_CLUSTER_UPTIME_SECONDS = 5 * 60;

  static getDbUrl(clusterId: string, secretArn: string, dbName: string, params?: { querytimeout?: number; schema?: string }) {
    // awsrds://{databaseName}:{awsSecretName}@{awsRegion}.{awsAccount}.aws/{awsRdsClustername}?param=value
    const [, , , region, account, , secretName] = secretArn.split(':');

    const paramsKeyValue = params
      ? Object.keys(params).reduce((acc, key) => acc.concat(`${encodeURIComponent(key)}=${encodeURIComponent(params[key])}`), [])
      : [];

    return `awsrds://${dbName}:${encodeURIComponent(secretName)}@${region}.${account}.aws/${encodeURIComponent(clusterId)}${
      paramsKeyValue?.length > 0 ? '?' + paramsKeyValue.join('&') : ''
    }`;
  }

  static splitSecretArn(secretArn: string) {
    if (!secretArn) {
      return null;
    }

    if (!AwsDataApiUtils.isString(secretArn)) {
      throw new Error("'secretArn' string value required");
    }

    if (!secretArn.startsWith('arn:')) {
      throw new Error('secret arn must start with arn:');
    }

    // arn:aws:secretsmanager:eu-central-1:XXXXX:secret:rds-db-credentials/cluster-XXXXX/postgres-xxxx
    const [, , service, region, account, type, secretName] = secretArn.split(':');

    if (service !== 'secretsmanager') {
      throw new Error('secret arn must be a secretsmanager ARN');
    }

    if (type !== 'secret') {
      throw new Error('secret arn type must be secret');
    }

    return {
      service,
      region,
      type,
      account,
      secretName,
    };
  }

  static getDbUrlFromConfig(config: ClientConfig) {
    const secret = AwsDataRawApi.splitSecretArn(config.password);
    if (!secret) {
      throw new Error('invalid secret-arn');
    }

    let params = config.user?.startsWith('awsDataApi:') ? '?' + config.user.replace(/^awsDataApi:/i, '') : '';

    if (config.query_timeout) {
      params += '&querytimeout=' + config.query_timeout;
    }

    return `awsrds://${config.database}:${encodeURIComponent(secret.secretName)}@${secret.region}.${
      secret.account
    }.aws/${encodeURIComponent(config.host)}${params}`;
  }

  constructor(config: string | ClientConfig, additionalConfig?: IAwsDataRawApiConfig) {
    if (!config) {
      throw new Error('config must be provided');
    }

    this.schema = additionalConfig?.defaultSchema;
    this._defaultQueryTimeoutInMS = additionalConfig?.defaultQueryTimeoutInMS;

    const awsrdsUrl = AwsDataApiUtils.isString(config) ? config : AwsDataRawApi.getDbUrlFromConfig(config);

    // awsrds://{databaseName}:{awsSecretName}@{awsRegion}.{awsAccount}.aws/{awsRdsClustername}?param=value
    const url = new URL(awsrdsUrl);
    if (url.protocol !== 'awsrds:') {
      throw new Error('unknown protocol ' + url.protocol + ' . must be awsrds://');
    }
    const [region, account] = url.hostname.split('.');
    const secret = decodeURIComponent(url.password);

    if (!region || region.length === 0) {
      throw new Error('AwsDataApi: region must be defined');
    }

    if (!account || account.length === 0) {
      throw new Error('AwsDataApi: region must be defined');
    }

    this.region = region;
    this.clusterIdentifier = decodeURIComponent(url.pathname.replace(/^\//, ''));
    this.databaseName = decodeURIComponent(url.username);
    this.secretArn = `arn:aws:secretsmanager:${region}:${account}:secret:${secret}`;
    this.resourceArn = `arn:aws:rds:${region}:${account}:cluster:${this.clusterIdentifier}`;

    if (url.searchParams) {
      for (const [key, value] of url.searchParams) {
        switch (key.toLowerCase()) {
          case 'querytimeout':
            this._defaultQueryTimeoutInMS = Number(value);
            break;
          case 'schema':
            this.schema = value;
            break;
        }
      }
    }

    if (!AwsDataApiUtils.isString(this.resourceArn)) {
      throw new Error("'resourceArn' string value required");
    }

    if (this.databaseName !== undefined && !AwsDataApiUtils.isString(this.databaseName)) {
      throw new Error("'database' string value required");
    }

    // temporary error since AWS seems to have trouble with those
    if (/[:@]/gi.test(this.databaseName)) {
      throw new Error("'database' name may not contain url special chars");
    }

    if (additionalConfig?.client) {
      this._rds = additionalConfig.client;
    } else {
      this._rds = new RDSDataService(AwsDataApiUtils.mergeConfig({ region: this.region }, additionalConfig?.rdsOptions || {}));
    }
  }

  setDefaultQueryTimeout(timeoutInMS: number): void {
    this._defaultQueryTimeoutInMS = timeoutInMS;
  }

  async checkDbState(params?: { startupTimeoutInMS?: number }): Promise<boolean> {
    if (
      !this._dbState.isRunning ||
      !this._dbState.lastCheck ||
      Math.abs(this._dbState.lastCheck - AwsDataApiUtils.getUnixEpochTimestamp()) > AwsDataRawApi.MIN_AURORA_CLUSTER_UPTIME_SECONDS
    ) {
      await this.executeStatement(
        {
          continueAfterTimeout: false,
          sql: 'SELECT NOW() as currenttime',
        },
        { queryTimeoutInMS: params?.startupTimeoutInMS || 1000, skipDbStateCheck: true },
      );
    }

    return this._dbState.isRunning;
  }

  async getClusterInfo(params?: { skipCache?: boolean }): Promise<DBCluster> {
    if (!this._clusterInfo || !params?.skipCache) {
      const rds = new RDS({ region: this.region });
      const clusterRes = await rds
        .describeDBClusters({
          DBClusterIdentifier: this.clusterIdentifier,
        })
        .promise();
      this._clusterInfo = clusterRes.DBClusters[0];

      // this line here does not work yet as expected
      // this._dbState.isRunning = this._clusterInfo.Status === 'available';
      // this._dbState.lastCheck = AwsDataApiUtils.getUnixEpochTimestamp();
    }

    return this._clusterInfo;
  }

  postgresDataApiClientConfig(): ClientConfig {
    const params: Record<string, any> = {
      region: this.region,
    };

    if (this.schema) {
      params.schema = this.schema;
    }

    if (this._defaultQueryTimeoutInMS) {
      params.timeout = this._defaultQueryTimeoutInMS;
    }

    return {
      user:
        'awsDataApi:' +
        Object.keys(params)
          .reduce((acc, key) => acc.concat(`${encodeURIComponent(key)}=${encodeURIComponent(params[key])}`), [])
          .join('&'),
      password: this.secretArn,
      host: this.clusterIdentifier,
      port: 443,
      database: this.databaseName,
    } as any;
  }

  async postgresNativeClientConfig(): Promise<ClientConfig> {
    // arn:aws:secretsmanager:eu-central-1:XXXXX:secret:rds-db-credentials/cluster-XXXXX/postgres-xxxx
    const [, , service, region, , type] = (this.secretArn || '').split(':');

    if (service !== 'secretsmanager') {
      throw new Error('secret arn must be a secretsmanager ARN');
    }

    if (type !== 'secret') {
      throw new Error('secret arn type must be secret');
    }

    const secretsClient = new SecretsManager({ region });

    const data = await secretsClient.getSecretValue({ SecretId: this.secretArn }).promise();

    const secretString = 'SecretString' in data ? data.SecretString : Buffer.from(data.SecretBinary as string, 'base64').toString('ascii');

    const values = JSON.parse(secretString);

    return {
      user: values.username,
      password: values.password,
      host: values.host,
      port: values.port,
      database: this.databaseName,
      awsDbInstanceIdentifier: values.dbInstanceIdentifier,
      awsEngine: values.engine,
      awsResourceId: values.resourceId,
    } as any;
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
      .batchExecuteStatement(
        AwsDataApiUtils.mergeConfig(AwsDataApiUtils.pick(this, ['resourceArn', 'secretArn', 'database', 'schema']), args),
      )
      .promise();
  }

  beginTransaction(args?: {
    /**
     * The name of the database schema.
     */
    schema?: DbName;
  }) {
    return this._rds
      .beginTransaction(
        AwsDataApiUtils.mergeConfig(AwsDataApiUtils.pick(this, ['resourceArn', 'secretArn', 'database', 'schema']), args || {}),
      )
      .promise();
  }

  async executeStatement(
    args: {
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
    },
    additionalParams?: {
      queryTimeoutInMS?: number;
      skipDbStateCheck?: boolean;
    },
  ): Promise<RDSDataService.ExecuteStatementResponse> {
    if (!additionalParams?.skipDbStateCheck) {
      await this.checkDbState();
    }

    return new Promise((resolve, reject) => {
      const sqlReq = this._rds.executeStatement(
        AwsDataApiUtils.mergeConfig(AwsDataApiUtils.pick(this, ['resourceArn', 'secretArn', 'database', 'schema']), args),
      );

      const timeoutInMS = additionalParams?.queryTimeoutInMS || this._defaultQueryTimeoutInMS;
      let isAborted = false;
      let timeoutRef =
        timeoutInMS > 0
          ? setTimeout(() => {
              isAborted = true;
              timeoutRef = null;
              sqlReq.abort();
              reject(
                new Error(
                  (this._dbState.isRunning ? 'sql-statement-timeout' : 'db-cluster-is-starting') + ' ' + JSON.stringify({ timeoutInMS }),
                ),
              );
            }, timeoutInMS)
          : null;

      sqlReq.send((err, data) => {
        if (timeoutRef) {
          clearTimeout(timeoutRef);
        }

        if (isAborted) {
          return;
        }

        if (err) {
          return reject(err);
        }

        this._dbState.isRunning = true;
        this._dbState.lastCheck = AwsDataApiUtils.getUnixEpochTimestamp();

        return resolve(data);
      });
    });
  }

  commitTransaction(args: {
    /**
     * The identifier of the transaction to end and commit.
     */
    transactionId: Id;
  }) {
    return this._rds
      .commitTransaction(AwsDataApiUtils.mergeConfig(AwsDataApiUtils.pick(this, ['resourceArn', 'secretArn']), args))
      .promise();
  }

  rollbackTransaction(args: {
    /**
     * The identifier of the transaction to roll back.
     */
    transactionId: Id;
  }) {
    return this._rds
      .rollbackTransaction(AwsDataApiUtils.mergeConfig(AwsDataApiUtils.pick(this, ['resourceArn', 'secretArn']), args))
      .promise();
  }
}
