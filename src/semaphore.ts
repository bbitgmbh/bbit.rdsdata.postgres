type BbitSemaphoreReleaserFunc = () => void;

type BbitSemaphoreWorkerFunc<T> = (value: number) => Promise<T> | T;

export class Semaphore {
  private _queue: Array<(lease: [number, BbitSemaphoreReleaserFunc]) => void> = [];
  private _currentReleaser: BbitSemaphoreReleaserFunc | undefined;
  private _freeConcurrencyPlaces: number;
  private _config: { maxConcurrency: number; timeoutInMS: number; timeoutError: Error };

  constructor(config?: { maxConcurrency?: number; timeoutInMS?: number; timeoutError?: Error }) {
    this._config = { maxConcurrency: 1, timeoutInMS: 0, timeoutError: new Error('semaphore-timeout'), ...(config || {}) };

    if (!(this._config.maxConcurrency > 0)) {
      this._config.maxConcurrency = 1;
    }

    this._freeConcurrencyPlaces = this._config.maxConcurrency;
  }

  private _acquire(): Promise<[number, BbitSemaphoreReleaserFunc]> {
    const locked = this.isLocked();
    const ticket = new Promise<[number, BbitSemaphoreReleaserFunc]>((r) => this._queue.push(r));

    if (!locked) this._dispatch();

    return ticket;
  }

  async awaitFree(): Promise<boolean> {
    await this.runExclusive(() => true);
    return !this.isLocked();
  }

  acquire(): Promise<[number, BbitSemaphoreReleaserFunc]> {
    if (this._config.timeoutInMS > 0) {
      return new Promise(async (resolve, reject) => {
        let isTimeout = false;

        setTimeout(() => {
          isTimeout = true;
          reject(this._config.timeoutError);
        }, this._config.timeoutInMS);

        const ticket = await this._acquire();

        if (isTimeout) {
          const release = ticket[1];

          release();
        } else {
          resolve(ticket);
        }
      });
    } else {
      return this._acquire();
    }
  }

  async runExclusive<T>(callback: BbitSemaphoreWorkerFunc<T>): Promise<T> {
    const [value, release] = await this.acquire();

    try {
      return await callback(value);
    } finally {
      release();
    }
  }

  isLocked(): boolean {
    return this._freeConcurrencyPlaces <= 0;
  }

  release(): void {
    if (this._config.maxConcurrency > 1) {
      throw new Error('this method is unavailable on semaphores with concurrency > 1; use the scoped release returned by acquire instead');
    }

    if (this._currentReleaser) {
      this._currentReleaser();
      this._currentReleaser = undefined;
    }
  }

  private _dispatch(): void {
    const nextConsumer = this._queue.shift();

    if (!nextConsumer) return;

    let released = false;
    this._currentReleaser = () => {
      if (released) return;

      released = true;
      this._freeConcurrencyPlaces++;

      this._dispatch();
    };

    nextConsumer([this._freeConcurrencyPlaces--, this._currentReleaser]);
  }
}
