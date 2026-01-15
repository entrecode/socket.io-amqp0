export function* mapIter(iterable, proj) {
    for (const x of iterable) {
        yield proj(x);
    }
}
export function* filterIter(iterable, pred) {
    for (const x of iterable) {
        if (pred(x))
            yield x;
    }
}
export function randomString(length = 8) {
    let text = '';
    const possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    for (let i = 0; i < length; i++) {
        text += possible.charAt(Math.floor(Math.random() * possible.length));
    }
    return text;
}
export function delay(ms) {
    return new Promise((res, rej) => setTimeout(res, ms));
}
//# sourceMappingURL=util.js.map