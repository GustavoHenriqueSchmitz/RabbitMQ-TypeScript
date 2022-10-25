import { WBA } from '../Webbuffet';
import * as amqp from 'amqplib';
import Bluebird = require('bluebird');
import * as cluster from 'cluster';

export interface IRedisOptions {
    host: string;
    port: number;
    user?: number;
    password?: string;
}

export class RabbitController {
    private static _instance: RabbitController;
    public connection: amqp.Connection;
    private host: string;
    private port: number;
    private user: string;
    private pass: string;
    private active: boolean;
    private defaultTimeout: number;

    public static get Instance(): RabbitController {
        return this._instance || (this._instance = new this());
    }

    private constructor() {
        const config = WBA.config();
        this.connection = null;
        this.defaultTimeout = config.rabbit_timeout;
        this.port = config.rabbit_port;
        this.user = config.rabbit_user;
        this.pass = config.rabbit_pass;
        switch (config.environment) {
            case 'prod':
                this.host = config.rabbit_address;
                this.active = config.rabbit_active;
                break;
            case 'qa':
                this.host = config.rabbit_address_qa;
                this.active = config.rabbit_active_qa;
                break;
            default:
                this.host = config.rabbit_address_dev;
                this.active = config.rabbit_active_dev;
        }
    }
    host: string;

    public async createConnection(): Promise<WBA.Webbuffet> {
        let operationPromise: Bluebird<amqp.Connection> = null;
        try {
            if (this.active) {
                if (this.user) {
                    operationPromise = amqp.connect(`amqp://${this.user}:${this.pass}@${this.host}:${this.port}`);
                    this.connection = await operationPromise.timeout(this.defaultTimeout);
                } else {
                    operationPromise = amqp.connect(`amqp://${this.host}:${this.port}`);
                    this.connection = await operationPromise.timeout(this.defaultTimeout);
                }
                this.connection.on('close', () => {
                    WBA.logError('RabbitMQ Connection is lost');
                    this.connection = null;
                });

                WBA.log(`RabbitMQ connection created: ${this.host}:${this.port}`);
            } else {
                WBA.logDebug(`RabbitMQ is inactive to open connection`);
            }
            return WBA.success(true);
        } catch (ex) {
            const config = WBA.config();
            if (config.environment !== 'qa' && config.environment !== 'prod') {
                return WBA.success(ex);
            }
            return WBA.exception(ex);
        }
    }

    public async createChannel(retry: boolean = false, prefetch: number = null): Promise<WBA.Webbuffet> {
        let operationPromise: Bluebird<amqp.Channel> = null;
        try {
            if (this.active && this.connection) {
                operationPromise = this.connection.createChannel();
                const channel: amqp.Channel = await operationPromise.timeout(this.defaultTimeout);
                if (prefetch)
                    await channel.prefetch(prefetch);
                WBA.log(`RabbitMQ channel created: ${channel}`);
                return WBA.success(channel);
            }

            if (this.active && !this.connection && !retry) {
                await this.createConnection();
                return await this.createChannel(true, prefetch);
            }

            if (this.active) {
                return WBA.error(WBA.Status.FAILED, 'Failed to create channel with RabbitMQ');
            }

            WBA.logDebug(`RabbitMQ is inactive to create a channel`);

            return WBA.success(false);
        } catch (ex) {
            return WBA.exception(ex);
        }
    }

    public async sendMessagesToQueue(queue: string, messages: any[], assertQueueOptions?: amqp.Options.AssertQueue, messageProperties?: amqp.Options.Publish, retry: boolean = false, prefetch: number = null): Promise<WBA.Webbuffet> {
        let operationPromise: Promise<WBA.Webbuffet> = null;
        let rc: WBA.Webbuffet = null;
        let channel: amqp.Channel = null;
        let assertQueue: amqp.Replies.AssertQueue = null;
        try {
            if (this.active && this.connection) {
                operationPromise = this.createChannel(false, prefetch);
                rc = await operationPromise.timeout(this.defaultTimeout);
                if (rc.$status != WBA.Status.SUCCESS) {
                    throw rc;
                }
                channel = rc.$data;
                if (channel) {
                    assertQueue = await channel.assertQueue(queue, assertQueueOptions);
                    if (assertQueue) {
                        let count = 0;
                        let sizeOfMessages = 0;
                        for (let message of messages) {
                            message = JSON.stringify(message);
                            sizeOfMessages += message.length;
                            const sent: boolean = await channel.sendToQueue(queue, Buffer.from(message), messageProperties);
                            if (!sent) {
                                WBA.logError(`Message not sent: ${message}`);
                            }
                            count++;
                            if (count >= 1000 || sizeOfMessages > 1048576) {
                                await this.waitForPending(channel);
                                await channel.close();
                                operationPromise = this.createChannel(false, prefetch);
                                rc = await operationPromise.timeout(this.defaultTimeout);
                                if (rc.$status != WBA.Status.SUCCESS) {
                                    throw rc;
                                }
                                channel = rc.$data;
                                if (channel) {
                                    assertQueue = await channel.assertQueue(queue, assertQueueOptions);
                                    if (!assertQueue) {
                                        throw Error(`Fail on assert queue`);
                                    }
                                }
                                count = 0;
                                sizeOfMessages = 0;
                            }
                        }
                    } else {
                        throw Error(`Fail on assert queue`);
                    }
                } else {
                    WBA.logError('RabbitMQ channel not found');
                }
            } else if (this.active && !this.connection && !retry) {
                await this.createConnection();
                return await this.sendMessagesToQueue(queue, messages, assertQueueOptions, messageProperties, true);
            } else if (this.active) {
                return WBA.error(WBA.Status.FAILED, 'Fail to send messages to queue');
            } else {
                WBA.logError('RabbitMQ is inactive for send messages');
            }
            return WBA.success(true);
        } catch (ex) {
            return WBA.exception(ex);
        } finally {
            await this.waitForPending(channel);
            await channel.close();
        }
    }

    public async createConsumer(name: string, queue: string, callback: (msg: amqp.ConsumeMessage | null) => void, assertQueueOptions?: amqp.Options.AssertQueue, consumeOptions?: amqp.Options.Consume, prefetch: number = null, startMultipleConsumers: boolean = true): Promise<WBA.Webbuffet> {
        let operationPromise: Promise<WBA.Webbuffet> = null;
        let rc: WBA.Webbuffet = null;
        let channel: amqp.Channel = null;
        try {
            const config = WBA.config();
            if (!cluster.isMaster || config.environment === 'dev') {
                if (this.active && this.connection) {
                    let startConsumer = true;
                    operationPromise = this.createChannel(false, prefetch);
                    rc = await operationPromise.timeout(this.defaultTimeout);
                    if (rc.$status != WBA.Status.SUCCESS) {
                        throw rc;
                    }
                    channel = rc.$data;
                    if (channel) {
                        const assertQueue: amqp.Replies.AssertQueue = await channel.assertQueue(queue, assertQueueOptions);
                        if (assertQueue) {
                            if ( !startMultipleConsumers && assertQueue.consumerCount > 0 ) {
                                startConsumer = false;
                            }
                            if ( startConsumer ) {
                                WBA.log(`[*] RabbitMQ Consumer: ${name} on ${process && process.env && process.env.name ? process.env.name : 'Master'} Waiting for messages in ${queue}.`);
                                channel.consume(queue, callback, consumeOptions);
                            }
                            return WBA.success(channel);
                        }
                        throw Error(`Fail on assert queue`);
                    }
                    WBA.logError('RabbitMQ channel not found');
                    return WBA.success(channel);
                }
                WBA.logError('RabbitMQ is inactive for create a consumer');
            }
            return WBA.success(null);
        } catch (ex) {
            return WBA.exception(ex);
        }
    }

    private async waitForPending(channel) {
        const intervalTime = 100;
        let timeout = 5000;
        return new Promise(resolve => {
            if (channel.pending.length === 0 && channel.reply === null) {
                resolve();
            } else {
                const interval = setInterval(() => {
                    if (channel.pending.length === 0 && channel.reply === null) {
                        // everything is clean
                        clearInterval(interval);
                        resolve();
                    }
                    timeout -= intervalTime;
                    if (timeout <= 0) {
                        // timed out, but you probably still want to close it, so resolve()
                        clearInterval(interval);
                        resolve();
                    }
                }, intervalTime);
            }
        });
    }
}
