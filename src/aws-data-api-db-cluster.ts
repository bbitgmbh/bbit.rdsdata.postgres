import { SecretsManager, RDSDataService } from 'aws-sdk';
import { ClientConfig } from 'pg';
import { Utils } from './utils';
import { DbName, Id, ResultSetOptions, SqlParameterSets, SqlParametersList, SqlStatement } from 'aws-sdk/clients/rdsdataservice';

export class AwsDataApiDbCluster {
  public readonly secretArn: string;
  public readonly resourceArn: string;
  public readonly clusterIdentifier: string;
  public readonly databaseName: string;
  public readonly region: string;
  public readonly schema: string;

  private _rds: RDSDataService;

  constructor(
    config?: string | ClientConfig,
    additionalConfig?: { defaultSchema?: string; rdsOptions?: AWS.RDSDataService.ClientConfiguration; client?: any },
  ) {
    if (!config) {
      return;
    }

    this.schema = additionalConfig?.defaultSchema;

    if (Utils.isString(config)) {
      // awsrds://{databaseName}:{awsSecretName}@{awsRegion}.{awsAccount}.aws/{awsRdsClustername}
      const url = new URL(config);
      if (url.protocol !== 'awsrds:') {
        throw new Error('unknown protocol ' + url.protocol);
      }
      const [region, account] = url.hostname.split('.');
      const secret = decodeURIComponent(url.password);

      this.region = region;
      this.clusterIdentifier = decodeURIComponent(url.pathname.replace(/^\//, ''));
      this.databaseName = decodeURIComponent(url.username);
      this.secretArn = `arn:aws:secretsmanager:${region}:${account}:secret:${secret}`;
      this.resourceArn = `arn:aws:rds:${region}:${account}:cluster:${this.clusterIdentifier}`;
    } else {
      const [, , service, region] = config.host.split(':');

      if (service !== 'rds') {
        throw new Error('host must be an AWS RDS arn');
      }

      this.region = region;
      this.databaseName = config.database;
      this.secretArn = config.password;
      this.resourceArn = config.host;
    }

    if (!Utils.isString(this.secretArn)) {
      throw new Error("'secretArn' string value required");
    }

    if (!Utils.isString(this.resourceArn)) {
      throw new Error("'resourceArn' string value required");
    }

    if (this.databaseName !== undefined && !Utils.isString(this.databaseName)) {
      throw new Error("'database' string value required");
    }

    // temporary warning since AWS seems to have trouble with those
    if (/[:@]/gi.test(this.databaseName)) {
      console.warn('database name may not contain url special chars');
    }

    if (additionalConfig?.client) {
      this._rds = new additionalConfig.client(Utils.mergeConfig({ region: this.region }, additionalConfig?.rdsOptions || {}));
    } else {
      this._rds = new RDSDataService(Utils.mergeConfig({ region: this.region }, additionalConfig?.rdsOptions || {}));
    }
  }

  postgresDataApiClientConfig(): ClientConfig {
    return {
      user: 'aws:' + this.region,
      password: this.secretArn,
      host: this.resourceArn,
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
      .batchExecuteStatement(Utils.mergeConfig(Utils.pick(this, ['resourceArn', 'secretArn', 'database', 'schema']), args))
      .promise();
  }

  beginTransaction(args?: {
    /**
     * The name of the database schema.
     */
    schema?: DbName;
  }) {
    return this._rds
      .beginTransaction(Utils.mergeConfig(Utils.pick(this, ['resourceArn', 'secretArn', 'database', 'schema']), args || {}))
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
      .executeStatement(Utils.mergeConfig(Utils.pick(this, ['resourceArn', 'secretArn', 'database', 'schema']), args))
      .promise();
  }

  commitTransaction(args: {
    /**
     * The identifier of the transaction to end and commit.
     */
    transactionId: Id;
  }) {
    return this._rds.commitTransaction(Utils.mergeConfig(Utils.pick(this, ['resourceArn', 'secretArn']), args)).promise();
  }

  rollbackTransaction(args: {
    /**
     * The identifier of the transaction to roll back.
     */
    transactionId: Id;
  }) {
    return this._rds.rollbackTransaction(Utils.mergeConfig(Utils.pick(this, ['resourceArn', 'secretArn']), args)).promise();
  }
}
