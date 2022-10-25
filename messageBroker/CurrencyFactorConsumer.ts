import { WBA } from "WBA-CORE";
import * as amqp from 'amqplib';
import { RabbitController } from 'WBA-CORE/dist/persistence/RabbitController';
import { IConsumer } from 'WBA-CORE/dist/interface/IRabbit';
import { CurrencyFactor } from "./CurrencyFactor";
import { CurrencyFactorService } from "./CurrencyFactorService";
import { documentErrorSecondStep } from 'WBA-Model/dist/interface/common/IDocumentError';
import { ExceptionUtil } from "WBA-CORE/dist/util/ExceptionUtil";

export class CurrencyFactorConsumer extends CurrencyFactor implements IConsumer {
    public channel: amqp.Channel = null;
    public defaultTimeout: number = null;
    public consumerName: string = null;

    constructor() {
        super();
        this.defaultTimeout = 600000;
        this.consumerName = 'CurrencyFactor';
    }

    public async createConsumer(): Promise<WBA.Webbuffet> {
        try {
            let operationPromise: Promise<WBA.Webbuffet> = null;
            let rc: WBA.Webbuffet = null;
            try {
                operationPromise = RabbitController.Instance.createConsumer(this.consumerName, this.queueName, this.consumeMessage.bind(this), { durable: true,  messageTtl: (60000 * 2) }, { noAck: false }, 1, false);  // TTL 2 minutes
                rc = await operationPromise.timeout(this.defaultTimeout);
                if (rc.$status != WBA.Status.SUCCESS) {
                    throw rc;
                }
                this.channel = rc.$data;

                return WBA.success(true);
            } catch (ex) {
                return WBA.exception(ex);
            }
        } catch (ex) {
            return WBA.exception(ex);
        }
    }

    public async consumeMessage(msg: amqp.ConsumeMessage | null): Promise<void> {
        let operationPromise: Promise<WBA.Webbuffet> = null;
        let rc: WBA.Webbuffet = null;
        const currencyFactorService = new CurrencyFactorService();
        try {
            // Implement process data
            operationPromise = currencyFactorService.processMessage(this.defaultTimeout, msg);
            rc = await operationPromise.timeout(this.defaultTimeout);
            if (rc.$status != WBA.Status.SUCCESS) {
                throw rc;
            }
        } catch (ex) {
            // Document Error
            // Send to error queue
            const dataMsg: string = ExceptionUtil.getExceptionDataMessage(ex);
            const errorMessage: any = { ERROR: dataMsg, MESSAGE: currencyFactorService.decodeMessage(msg), SUBJECT: documentErrorSecondStep.PROCESS_SECOND_STEP};
            operationPromise = RabbitController.Instance.sendMessagesToQueue(`${this.queueName}Error`, [errorMessage]);
            rc = await operationPromise.timeout(3000);
        } finally {
            // Ack Message
            this.channel.ack(msg);
        }

    }
}
