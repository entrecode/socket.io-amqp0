export declare function mapIter<T, U>(iterable: Iterable<T>, proj: (item: T) => U): Iterable<U>;
export declare function filterIter<T>(iterable: Iterable<T>, pred: (item: T) => boolean): Iterable<T>;
export declare function randomString(length?: number): string;
export declare function delay(ms: number): Promise<void>;
