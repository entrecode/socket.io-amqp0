var _AmqpAdapter_consumeChannel$, _AmqpAdapter_publishChannel$;
import { __classPrivateFieldGet } from "tslib";
/* eslint-disable no-console */
import { Adapter } from 'socket.io-adapter';
import debugFactory from 'debug';
import { hostname } from 'os';
import { randomString, mapIter, filterIter } from './util.js';
import { promisify } from 'util';
import { ReplaySubject, filter, firstValueFrom } from 'rxjs';
const nullSet = new Set([null]);
Object.freeze(nullSet);
const emptySet = new Set([]);
Object.freeze(emptySet);
const defaultRoomName = 'broadcast';
const defaultExchangeName = 'socket.io';
export const createAdapter = function ({ name, ...opts }) {
    const shim = class AmqpAdapterWrapper extends AmqpAdapter {
        constructor(nsp) {
            super(nsp, opts, name ?? 'default');
        }
    };
    //    shim.name = AmqpAdapter.name;
    return shim;
};
export class AmqpAdapter extends Adapter {
    constructor(nsp, options, name) {
        super(nsp);
        this.nsp = nsp;
        this.options = options;
        this.rooms = new Map();
        this.sids = new Map();
        this.roomListeners = new Map();
        this.closed = false;
        _AmqpAdapter_consumeChannel$.set(this, new ReplaySubject(1));
        _AmqpAdapter_publishChannel$.set(this, new ReplaySubject(1));
        this.readyConsumeChannel$ = __classPrivateFieldGet(this, _AmqpAdapter_consumeChannel$, "f").pipe(filter(Boolean));
        this.readyPublishChannel$ = __classPrivateFieldGet(this, _AmqpAdapter_publishChannel$, "f").pipe(filter(Boolean));
        this.localRouting = new Set();
        this.debug = debugFactory(`socket.io-amqp:${name}`);
        this.instanceName = options.instanceName ?? hostname();
        this.exchangeName = options.exchangeName ?? defaultExchangeName;
        this.queuePrefix = options.queuePrefix ?? defaultExchangeName;
        options.shutdownCallbackCallback?.(async () => {
            this.debug('called shutdownCallback');
            this.closed = true;
            await Promise.all(mapIter(this.roomListeners.values(), (unsub) => unsub()));
        });
        this.init(); // hack until issue in socket.io is resolved
    }
    serverCount() {
        return Promise.resolve(10);
    }
    async broadcastWithAck(packet, opts, clientCountCallback, ack) {
        await this.broadcast(packet, opts);
        // todo: shim to handle broadcast with ack until I have time to implement it for real
        clientCountCallback(1);
        ack();
    }
    async handleConnection(conn) {
        conn.on('close', async () => {
            if (this.closed)
                return;
            this.debug('not closed, reopening');
            const conn = await this.options.amqpConnection();
            this.handleConnection(conn);
        });
        conn.on('error', (err) => {
            this.debug('Got connection error', err);
        });
        try {
            const [consumeChannel, publishChannel] = await Promise.all([
                conn.createChannel(),
                conn.createConfirmChannel(),
            ]);
            __classPrivateFieldGet(this, _AmqpAdapter_consumeChannel$, "f").next(consumeChannel);
            __classPrivateFieldGet(this, _AmqpAdapter_publishChannel$, "f").next(publishChannel);
            const promises = [];
            for (const [room, shutdown] of this.roomListeners) {
                promises.push(shutdown());
                promises.push(this.setupRoom(room));
            }
            await Promise.all(promises);
        }
        catch (err) {
            if (this.closed)
                throw err;
            __classPrivateFieldGet(this, _AmqpAdapter_publishChannel$, "f").next(undefined);
            __classPrivateFieldGet(this, _AmqpAdapter_consumeChannel$, "f").next(undefined);
            this.debug('Error in handleConnection', err);
            this.handleConnection(conn);
        }
    }
    async init() {
        this.debug('start init w/ exchange name', this.exchangeName);
        // console.log('ohai', this.exchangeName);
        const connection = await this.options.amqpConnection();
        await this.handleConnection(connection);
        // set up the default broadcast
        await this.setupRoom(null);
        this.debug('end init');
        this.options.readyCallback?.();
    }
    async close() {
        this.debug('Closing in Adapter', this.exchangeName);
        this.closed = true;
        await Promise.all(mapIter(this.roomListeners.values(), (unsub) => unsub()));
    }
    async setupRoom(room) {
        if (this.closed)
            return;
        const queueName = await this.createRoomExchangeAndQueue(room);
        const unsub = await this.createRoomListener(room, queueName);
        this.roomListeners.set(room, unsub);
    }
    async createQueueForRoom(room) {
        const queueName = `${this.queuePrefix}#${this.instanceName}${room ? `#${room}` : ''}`;
        const consumeChannel = await firstValueFrom(this.readyConsumeChannel$);
        await consumeChannel.assertQueue(queueName, {
            autoDelete: true,
            durable: true,
            arguments: {
                'x-expires': 1000 * 60,
            },
        });
        return queueName;
    }
    async createRoomExchangeAndQueue(room) {
        const consumeChannelPromise = firstValueFrom(this.readyConsumeChannel$);
        const publishChannel = await firstValueFrom(this.readyPublishChannel$);
        const [, queueName, consumeChannel] = await Promise.all([
            publishChannel.assertExchange(this.exchangeName, 'direct', {
                autoDelete: true,
                durable: false,
            }),
            this.createQueueForRoom(room),
            consumeChannelPromise,
        ]);
        this.debug('gonna bind', this.exchangeName, room ?? defaultRoomName);
        await consumeChannel.bindQueue(queueName, this.exchangeName, room ?? defaultRoomName);
        this.debug('did bind', this.exchangeName, room ?? defaultRoomName);
        return queueName;
    }
    async handleIncomingMessage(envelope, room) {
        const packet = envelope.packet;
        if (room && !this.rooms.has(room))
            return;
        super.broadcast(packet, { except: new Set(envelope.except), rooms: room ? new Set([room]) : emptySet });
    }
    async createRoomListener(room, queueName) {
        this.debug('Starting room listener for', room);
        let consumerTag = randomString();
        const consumeChannel = await firstValueFrom(this.readyConsumeChannel$);
        const consumeReply = await consumeChannel.consume(queueName, async (msg) => {
            if (!msg)
                return;
            const payload = JSON.parse(msg.content.toString('utf8'));
            await this.handleIncomingMessage(payload, room);
            consumeChannel.ack(msg, false);
        }, {
            noAck: false, // require manual ack
            consumerTag,
        });
        consumerTag = consumeReply.consumerTag;
        return async () => {
            this.debug('Canceling room listener for', room, `(${this.exchangeName})`);
            await consumeChannel.cancel(consumerTag);
        };
    }
    async addAll(id, rooms) {
        // eslint-disable-next-line prefer-rest-params
        this.debug('addAll', ...arguments);
        const newRooms = new Set();
        for (const room of rooms) {
            if (room === id) {
                if (this.options.sidRoomRouting === "banned" /* SidRoomRouting.banned */)
                    continue;
                if (this.options.sidRoomRouting === "local" /* SidRoomRouting.local */) {
                    this.localRouting.add(room);
                    continue;
                }
            }
            if (!this.sids.has(id)) {
                this.sids.set(id, new Set());
            }
            this.sids.get(id).add(room);
            if (!this.rooms.has(room)) {
                this.rooms.set(room, new Set());
                newRooms.add(room);
            }
            this.rooms.get(room).add(id);
        }
        await Promise.all([
            ...mapIter(newRooms, async (room) => {
                const queueName = await this.createRoomExchangeAndQueue(room);
                const unsub = await this.createRoomListener(room, queueName);
                this.roomListeners.set(room, unsub);
            }),
        ]);
    }
    del(id, room) {
        if (this.sids.has(id)) {
            this.sids.get(id).delete(room);
        }
        if (this.rooms.has(room)) {
            this.rooms.get(room).delete(id);
            if (this.rooms.get(room).size === 0) {
                this.rooms.delete(room);
                this.debug('called del on room:', room);
                // tear down the room listener
                this.roomListeners.get(room)?.();
                this.roomListeners.delete(room);
            }
        }
    }
    delAll(id) {
        this.localRouting.delete(id);
        if (!this.sids.has(id)) {
            return;
        }
        for (const room of this.sids.get(id)) {
            this.del(id, room); // todo: probably wrap this via promises
        }
        this.sids.delete(id);
    }
    async publishToRooms(rooms, envelope) {
        this.debug('Publishing message for rooms', rooms, envelope);
        const routeKeys = rooms.map((room) => room ?? defaultRoomName);
        const buffer = Buffer.from(JSON.stringify(envelope));
        const publishChannel = await firstValueFrom(this.readyPublishChannel$);
        await promisify(publishChannel.publish).bind(publishChannel)(this.exchangeName, routeKeys[0], buffer, {
            ...(routeKeys.length > 1 ? { CC: routeKeys.slice(1) } : {}),
        });
    }
    async broadcast(packet, opts) {
        this.debug('broadcast', packet, opts);
        if (opts.flags?.local) {
            return super.broadcast(packet, opts);
        }
        const envelope = {
            packet,
            except: opts.except && [...opts.except],
        };
        const rooms = opts.rooms && opts.rooms.size ? opts.rooms : nullSet;
        const nonlocalRooms = [...filterIter(rooms, (room) => !this.localRouting.has(room))];
        await Promise.all([
            ...mapIter(filterIter(rooms, (room) => this.localRouting.has(room)), async (room) => {
                await this.broadcast(packet, {
                    ...opts,
                    rooms: new Set([room]),
                    flags: { ...opts.flags, local: true },
                });
            }),
            this.publishToRooms(nonlocalRooms, envelope),
        ]);
    }
    sockets(rooms, callback) {
        const sids = new Set();
        if (rooms.size) {
            for (const room of rooms) {
                if (!this.rooms.has(room))
                    continue;
                for (const id of this.rooms.get(room)) {
                    if (id in this.nsp.sockets) {
                        sids.add(id);
                    }
                }
            }
        }
        else {
            for (const [id] of this.sids) {
                if (id in this.nsp.sockets)
                    sids.add(id);
            }
        }
        callback?.(sids);
        return Promise.resolve(sids);
    }
    socketRooms(id) {
        return this.sids.get(id);
    }
    serverSideEmit(packet) {
        throw new Error('this adapter does not support the serverSideEmit() functionality');
    }
}
_AmqpAdapter_consumeChannel$ = new WeakMap(), _AmqpAdapter_publishChannel$ = new WeakMap();
//# sourceMappingURL=index.js.map