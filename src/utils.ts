export class Utils {
  // Utility function for removing certain keys from an object
  static omit<T>(obj: T, values: string[]) {
    return Object.keys(obj).reduce((acc, x) => (values.includes(x) ? acc : Object.assign(acc, { [x]: obj[x] })), {} as T);
  }

  // Utility function for picking certain keys from an object
  static pick<T>(obj: T, values: string[]): T {
    return Object.keys(obj).reduce((acc, x) => (values.includes(x) ? Object.assign(acc, { [x]: obj[x] }) : acc), {} as T);
  }

  // Utility function for flattening arrays
  static flatten<T>(arr: T[][]): T[] {
    return arr.reduce((acc, x) => acc.concat(x), []);
  }

  static isDate(val: any): val is Date {
    return val instanceof Date;
  }

  static isString(val: any): val is string {
    return typeof val === 'string';
  }

  static isFunction(val: any): boolean {
    return typeof val === 'function';
  }

  static isObject(val: any): boolean {
    return typeof val === 'object';
  }

  static mergeConfig<T, U>(initialConfig: T, args: U): T & U {
    return Object.assign(initialConfig, args);
  }

  static snakeToCamel(value: string) {
    return value.replace(/([-_][a-z])/g, (group) => group.toUpperCase().replace('-', '').replace('_', ''));
  }

  static async promiseWithTimeout<T>(promise: Promise<T>, timeoutInMS: number, errorCode = 'database-is-starting'): Promise<T> {
    let timeoutId: any;
    let outerResolve: any;
    const timeoutPromise: Promise<any> = new Promise<T>((resolve, reject) => {
      outerResolve = resolve;
      timeoutId = setTimeout(() => {
        reject(new Error(errorCode));
      }, timeoutInMS);
    });
    const v = await Promise.race([promise, timeoutPromise]);
    clearTimeout(timeoutId);
    outerResolve();
    return v;
  }
}
