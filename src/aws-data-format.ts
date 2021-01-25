import * as AWS from 'aws-sdk';
import { SqlParametersList, SqlRecords, Field } from 'aws-sdk/clients/rdsdataservice';
import * as sqlString from 'sqlstring';
import { AwsDataError } from './aws-data-error';
import { IAwsDataApiQueryParams } from './interfaces';
import { AwsDataApiUtils } from './utils';
import { DateTime } from 'luxon';

// Supported value types in the Data API
const supportedTypes = ['arrayValue', 'blobValue', 'booleanValue', 'doubleValue', 'isNull', 'longValue', 'stringValue', 'structValue'];

export class AwsDataFormat {
  // Normize parameters so that they are all in standard format
  static normalizeParams(paramsToNormalize: (Record<any, any> | { name: string; value: any })[]): { name: string; value: any }[] {
    return paramsToNormalize.reduce<{ name: string; value: any }[]>(
      (acc, p) =>
        Array.isArray(p)
          ? acc.concat(AwsDataFormat.normalizeParams(p))
          : Object.keys(p).length === 2 && p.name && p.value
          ? acc.concat([p as any])
          : acc.concat(AwsDataFormat.splitParams(p)),
      [],
    );
  }

  // Prepare parameters
  static processParams(sql: string, sqlParams, paramsToProcess, formatOptions: IAwsDataApiQueryParams['formatOptions'], row = 0) {
    return {
      processedParams: paramsToProcess.reduce((acc, p) => {
        if (Array.isArray(p)) {
          const result = AwsDataFormat.processParams(sql, sqlParams, p, formatOptions, row);
          if (row === 0) {
            sql = result.escapedSql;
            row++;
          }
          return acc.concat([result.processedParams]);
        } else if (sqlParams[p.name]) {
          if (sqlParams[p.name].type === 'n_ph') {
            acc.push(AwsDataFormat.formatParam(p.name, p.value, formatOptions));
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
  static formatParam(n, v, formatOptions: IAwsDataApiQueryParams['formatOptions']) {
    return AwsDataFormat.formatType(n, v, AwsDataFormat.getType(v), AwsDataFormat.getTypeHint(v), formatOptions);
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
      throw new AwsDataError('invalid-input', { reason: 'Values must be an object or array' });
    }

    // Parse and normalize parameters
    const parameters = AwsDataFormat.normalizeParams(values);

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
    const { processedParams, escapedSql } = AwsDataFormat.processParams(sql, parameterLabelAndTypes, parameters, queryParams.formatOptions);

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
  static getTypeHint(val: unknown) {
    return AwsDataApiUtils.isDate(val) ? 'TIMESTAMP' : undefined;
  }

  // Creates a standard Data API parameter using the supplied inputs
  static formatType(name, value, type, typeHint, formatOptions: IAwsDataApiQueryParams['formatOptions']) {
    return Object.assign(
      typeHint != null ? { name, typeHint } : { name },
      type === null
        ? { value }
        : {
            value: {
              [type ? type : AwsDataError.throw('invalid-type', { reason: `'${name}' is an invalid type` })]:
                type === 'isNull'
                  ? true
                  : AwsDataApiUtils.isDate(value)
                  ? AwsDataFormat.formatToTimeStamp(value, formatOptions.treatAsTimeZone)
                  : value,
            },
          },
    );
  }

  // Formats the (UTC) date to the AWS accepted YYYY-MM-DD HH:MM:SS[.FFF] format
  // See https://docs.aws.amazon.com/rdsdataservice/latest/APIReference/API_SqlParameter.html
  static formatToTimeStamp(date: Date, treatAsTimeZone: string) {
    // ToDo: does not work with sequelize: DateTime.fromJSDate(value).setZone(formatOptions.treatAsTimeZone || 'utc').toSQL({ includeZone: false })
    const pad = (val: number, num = 2) => '0'.repeat(num - (val + '').length) + val;

    const year = treatAsTimeZone === 'local' ? date.getFullYear() : date.getUTCFullYear();
    const month = (treatAsTimeZone === 'local' ? date.getMonth() : date.getUTCMonth()) + 1; // Convert to human month
    const day = treatAsTimeZone === 'local' ? date.getDate() : date.getUTCDate();

    const hours = treatAsTimeZone === 'local' ? date.getHours() : date.getUTCHours();
    const minutes = treatAsTimeZone === 'local' ? date.getMinutes() : date.getUTCMinutes();
    const seconds = treatAsTimeZone === 'local' ? date.getSeconds() : date.getUTCSeconds();
    const ms = treatAsTimeZone === 'local' ? date.getMilliseconds() : date.getUTCMilliseconds();

    const fraction = ms <= 0 ? '' : `.${pad(ms, 3)}`;

    return `${year}-${pad(month)}-${pad(day)} ${pad(hours)}:${pad(minutes)}:${pad(seconds)}${fraction}`;
  }

  static formatRecords(recs: SqlRecords, columns: AWS.RDSDataService.Metadata, params: IAwsDataApiQueryParams) {
    if (params.convertSnakeToCamel) {
      columns.filter((c) => c.label.includes('_')).forEach((c) => (c.label = AwsDataApiUtils.snakeToCamel(c.label)));
    }

    const fieldMap: { label: string; typeName: string }[] =
      recs && recs[0]
        ? recs[0].map<{ label: string; typeName: string }>((_x, i) => ({
            label: columns && columns.length ? columns[i].label : 'col' + i,
            typeName: columns && columns.length ? columns[i].typeName : undefined,
            // fieldKey: Object.keys(x).filter((type) => type !== 'isNull' && x[type] !== undefined && x[type] !== null)[0],
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

            const value = AwsDataFormat.deserializeRecordField(field, fieldMap[i], params);

            return params.hydrateColumnNames // object if hydrate, else array
              ? Object.assign(acc, { [fieldMap[i].label]: value })
              : acc.concat(value);
          }, (params.hydrateColumnNames ? {} : []) as any); // init object if hydrate, else init array
        })
      : [];
  }

  // Format record value based on its value, the database column's typeName and the formatting options
  static deserializeRecordField(value: Field, field: { label: string; typeName: string }, params: IAwsDataApiQueryParams) {
    if (value.arrayValue) {
      const arrayFieldKey = Object.keys(value.arrayValue).find(
        (type) => type !== 'isNull' && value.arrayValue[type] !== undefined && value.arrayValue[type] !== null,
      );

      const arrValue = value.arrayValue[arrayFieldKey].map((e: any) => AwsDataFormat.deserializeRecordValue(e, field, params));
      if (params?.formatOptions?.stringifyArrays) {
        return JSON.stringify(arrValue);
      }

      return arrValue;
    }

    const fieldKey = Object.keys(value).find((type) => type !== 'isNull' && value[type] !== undefined && value[type] !== null);

    return AwsDataFormat.deserializeRecordValue(value[fieldKey], field, params);
  }

  static deserializeRecordValue(value: any, field: { label: string; typeName: string }, params: IAwsDataApiQueryParams) {
    const isDateField = ['date', 'datetime', 'timestamp', 'timestamptz'].includes(field.typeName.toLowerCase());

    if (isDateField) {
      switch (params?.formatOptions?.datetimeConverstion || 'convertToIsoString') {
        case 'convertToJsDate':
          return DateTime.fromSQL(value, { zone: params?.formatOptions?.treatAsTimeZone || 'utc' }).toJSDate();

        case 'convertToIsoString':
          return DateTime.fromSQL(value, { zone: params?.formatOptions?.treatAsTimeZone || 'utc' }).toISO();

        case 'keepSQLFormat':
          return value;
      }
    }

    return value;
  }

  // Format updateResults and extract insertIds
  static formatUpdateResults(res) {
    return res.map((x) => {
      return x.generatedFields && x.generatedFields.length > 0 ? { insertId: x.generatedFields[0].longValue } : {};
    });
  }
}
