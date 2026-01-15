import { BroadcastOptions, Room, SocketId, Adapter } from 'socket.io-adapter';
import { Namespace } from 'socket.io';
import { Connection } from 'amqplib';
export declare const enum SidRoomRouting {
    normal = "normal",
    local = "local",
    banned = "banned"
}
export interface AmqpAdapterOptions {
    amqpConnection: () => Promise<Connection> | Connection;
    sidRoomRouting?: SidRoomRouting;
    instanceName?: string;
    exchangeName?: string;
    queuePrefix?: string;
    shutdownCallbackCallback?: (callback: () => Promise<void>) => void;
    readyCallback?: () => void;
}
export declare const createAdapter: ({ name, ...opts }: AmqpAdapterOptions & {
    name?: string;
}) => typeof Adapter;
export declare class AmqpAdapter extends Adapter {
    #private;
    readonly nsp: Namespace;
    private options;
    private debug;
    readonly rooms: Map<Room, Set<SocketId>>;
    readonly sids: Map<SocketId, Set<Room>>;
    readonly instanceName: string;
    readonly exchangeName: string;
    readonly queuePrefix: string;
    private roomListeners;
    private closed;
    private readyConsumeChannel$;
    private readyPublishChannel$;
    constructor(nsp: Namespace, options: AmqpAdapterOptions, name: string);
    serverCount(): Promise<number>;
    broadcastWithAck(packet: any, opts: BroadcastOptions, clientCountCallback: (clientCount: number) => void, ack: (...args: any[]) => void): Promise<void>;
    handleConnection(conn: Connection): Promise<void>;
    init(): Promise<void>;
    close(): Promise<void>;
    private setupRoom;
    private localRouting;
    private createQueueForRoom;
    private createRoomExchangeAndQueue;
    private handleIncomingMessage;
    private createRoomListener;
    addAll(id: string, rooms: Set<string>): Promise<void>;
    del(id: string, room: string): void;
    delAll(id: string): void;
    private publishToRooms;
    broadcast(packet: any, opts: BroadcastOptions): Promise<void>;
    sockets(rooms: Set<Room>, callback?: (sockets: Set<SocketId>) => void): Promise<Set<SocketId>>;
    socketRooms(id: string): Set<Room> | undefined;
    serverSideEmit(packet: any[]): void;
}
