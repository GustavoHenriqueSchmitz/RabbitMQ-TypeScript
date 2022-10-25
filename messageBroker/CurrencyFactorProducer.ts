import { WBA } from "WBA-CORE";
import { RabbitController } from 'WBA-CORE/dist/persistence/RabbitController';
import {  } from "./CurrencyFactor";
import { ICurrencyQueue } from 'WBA-Model/dist/interface/product/CurrencyFactor';
import { CurrencyFactor } from './CurrencyFactor';

export class CurrencyFactorProducer extends CurrencyFactor {

    public async sendMessages(currencies: ICurrencyQueue[]): Promise<WBA.Webbuffet> {
        let operationPromise: Promise<WBA.Webbuffet> = null;
        let rc: WBA.Webbuffet = null;
        try {
            operationPromise = RabbitController.Instance.sendMessagesToQueue(this.queueName, currencies, { messageTtl: (60000 * 2) }); // TTL 2 minutes
            rc = await operationPromise.timeout(3000);
            if (rc.$status != WBA.Status.SUCCESS) {
                throw rc;
            }
            return WBA.success(true);
        } catch (ex) {
            return WBA.exception(ex);
        }
    }
}
