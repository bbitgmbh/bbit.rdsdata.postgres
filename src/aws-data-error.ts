const simpleSlugify = (str: string): string => {
  return (str + '')
    .trim()
    .replace(/[^a-z0-9-]/gi, '-')
    .replace(/-+/g, '-')
    .replace(/^-|-$/g, '')
    .toLowerCase();
};

export class AwsDataError extends Error {
  public code: string;
  public retryable: boolean;
  public params: { [key: string]: any };

  constructor(code: string, params?: { [key: string]: any; retryable?: boolean }) {
    // 'Error' breaks prototype chain here
    super(simpleSlugify(code));

    // restore prototype chain
    const actualProto = new.target.prototype;

    /* istanbul ignore else: platform specific fallback */
    if (Object.setPrototypeOf) {
      Object.setPrototypeOf(this, actualProto);
    } else {
      (this as any).__proto__ = actualProto;
    }

    this.code = simpleSlugify(code);
    this.params = params;
    if (params && params.hasOwnProperty('retryable')) {
      this.retryable = params.retryable;
    }

    this.message = AwsDataError.stringify({ code: this.code, params });
  }

  static stringify({ code, params }: { code: string; params: { [key: string]: any; retryable?: boolean } }): string {
    return code + (params ? ' ' + JSON.stringify(params) : '');
  }

  static parse(str: string): { code: string; params: { [key: string]: any; retryable?: boolean } } {
    const parts = (str + '').split(' ');

    let params = {};
    if (parts[1] && parts[1].trim().startsWith('{')) {
      try {
        params = JSON.parse(parts[1].trim());
      } catch (err) {
        console.error('Error on parsing Error-String ' + str, err);
      }
    }

    return { code: parts[0], params };
  }

  static from(str: string): AwsDataError {
    const parsed = AwsDataError.parse(str);
    return new AwsDataError(parsed.code, parsed.params);
  }

  static throw(code: string, params?: { [key: string]: any; retryable?: boolean }) {
    throw new AwsDataError(code, params);
  }
}
