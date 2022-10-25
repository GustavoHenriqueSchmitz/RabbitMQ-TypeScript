import { WBA } from 'WBA-CORE';
import * as amqp from 'amqplib';
import { Service } from '../../Service';
import { CurrencyFactorRepository } from './CurrencyFactorRepository';
import { CurrencyFactorSQLRepository } from '../../../persistence/CurrencyFactorSQLRepository';
import { ICurrencyFactorItem, ICurrencyFactor, ICurrencyOriginDestination, ICurrencyQueue } from 'WBA-Model/dist/interface/product/CurrencyFactor';
import { CacheCurrencyService } from '../../../cache/service/CacheCurrencyService';
import { SQLQuotationController } from 'WBA-Product-Lib/dist/services/sqlConnection/SQLQuotationController';
import { CryptoService } from 'WBA-CORE/dist/service/CryptoService';
import { ProductConfigurationService } from '../../ProductConfigurationService';
import * as ProductConfigurationModel from '../../../model/ProductConfiguration';

export class CurrencyFactorService extends Service<CurrencyFactorRepository> {

    private defaultTimeoutFinds;
    private cryptoService: CryptoService;

    constructor() {
        super(CurrencyFactorRepository);
        this.defaultTimeoutFinds = 10000;
        this.cryptoService = new CryptoService();
    }

    public async processMessage(_timeout: number, msg: amqp.ConsumeMessage): Promise<WBA.Webbuffet> {
        let rc: WBA.Webbuffet = null;
        let operationPromise: Promise<WBA.Webbuffet> = null;
        let messageDecoded: ICurrencyQueue = null;

        try {
            if (!_timeout) throw Error('Missing _timeout parameter in processMessage');
            if (!msg) throw Error('Missing msg parameter in processMessage');

            // Decode message
            messageDecoded = this.decodeMessage(msg);

            if (!messageDecoded) {
                throw new Error('Missing messageDecoded');
            }

            // Clear Document Error
            operationPromise = this.$repository.clearDocumentError(this.defaultTimeoutFinds, messageDecoded);
            rc = await operationPromise.timeout(this.defaultTimeoutFinds);
            if (rc.$status != WBA.Status.SUCCESS) {
                throw new Error('Fail on clearDocumentError on CurrencyFactorService.');
            }

            WBA.log('Start processMessage on CurrencyFactorService: ' + JSON.stringify(messageDecoded.NAME) + ' (Integration ID: ' + JSON.stringify(messageDecoded.INTEGRATION_ID) + ')');
            const currencyCacheService = new CacheCurrencyService();

            // Get currencies information from cache to avoid find all again. (But it can happen if the cache is expired.)
            operationPromise = currencyCacheService.getMonacoCurrencies(_timeout, 'monacoCurrencies');
            rc = await operationPromise.timeout(_timeout);
            if (rc.$status !== WBA.Status.SUCCESS) throw Error(`Fail to getMonacoCurrencies on cache`);

            let monacoCurrencies: ICurrencyQueue[] = rc.$data;

            if (!monacoCurrencies) {
                operationPromise = this.$repository.findAllCurrency(_timeout);
                rc = await operationPromise.timeout(_timeout);
                if (rc.$status !== WBA.Status.SUCCESS) throw Error(`Fail to findAllCurrencies on processMessage.`);
                monacoCurrencies = rc.$data;
                operationPromise = currencyCacheService.setMonacoCurrencies(_timeout, 'monacoCurrencies', monacoCurrencies);
                rc = await operationPromise.timeout(_timeout);
                if (rc.$status !== WBA.Status.SUCCESS) throw Error(`Fail to setMonacoCurrencies on cache`);
            }

            // // Identifying documents by adding a BATCH field
            const batch = await this.cryptoService.generateUniqueTimeId();
            operationPromise = this.$repository.setMonacoCurrenciesFactorBatch(_timeout, messageDecoded.INITIALS, batch);
            rc = await operationPromise.timeout(_timeout);
            if (rc.$status !== WBA.Status.SUCCESS) throw Error(`Fail to setMonacoCurrenciesFactorBatch`);

            const connection = new SQLQuotationController();
            // Find factors            
            await connection.connect();
            const repositorySQL = new CurrencyFactorSQLRepository();
            let days: number = 30;

            // Get the number of days to fetch the history of currency factor
            const productConfigurationService = new ProductConfigurationService();
            operationPromise = productConfigurationService.getProductConfiguration(_timeout);
            rc = await operationPromise.timeout(_timeout);
            if (rc && rc.$data) {
                const productConfiguration: ProductConfigurationModel.ProductConfiguration = rc.$data;
                days = productConfiguration.$CURRENCY_FACTOR_HISTORY_DAYS;
            }

            WBA.log('Updating Currency: ' + messageDecoded.INITIALS + ' (Integration ID: ' + messageDecoded.INTEGRATION_ID + ')');

            operationPromise = repositorySQL.findCurrencyFactors(connection, messageDecoded.INTEGRATION_ID, messageDecoded.INTEGRATION_ID, days);
            rc = await operationPromise.timeout(_timeout);
            if (rc.$status != WBA.Status.SUCCESS) throw new Error('Fail on findCurrencyFactors');

            await connection.close();

            const factorList: ICurrencyFactorItem[] = rc.$data;

            let currencyOrigin: ICurrencyOriginDestination;
            let currencyDestination: ICurrencyOriginDestination;

            // populate object and send to update or insert
            for (const factorItem of factorList) {
                currencyOrigin = monacoCurrencies.find(x => x.INTEGRATION_ID == factorItem.CURRENCY_ORIGIN_INTEGRATION_ID);
                currencyDestination = monacoCurrencies.find(x => x.INTEGRATION_ID == factorItem.CURRENCY_DESTINATION_INTEGRATION_ID);

                if (currencyOrigin && currencyDestination) {
                    const factor: ICurrencyFactor = {
                        _id: null,
                        CURRENCY_ORIGIN: currencyOrigin,
                        CURRENCY_DESTINATION: currencyDestination,
                        INTEGRATION_ID: factorItem.INTEGRATION_ID,
                        TABLE: {
                            ID: factorItem.ID_TABLE.toString(),
                            NAME: factorItem.NAME_TABLE
                        },
                        FACTOR: factorItem.FACTOR,
                        UPDATED_AT: factorItem.UPDATED_AT,
                        UPDATED_LAST: null,
                        BATCH: null
                    };

                    operationPromise = this.$repository.saveCurrencyFactor(_timeout, factor);
                    rc = await operationPromise.timeout(_timeout);
                    if (rc.$status != WBA.Status.SUCCESS) throw new Error('Fail on saveCurrencyFactor: ' + rc.$data);
                    WBA.log(`Finish update Currency Factor (Origin: ${factor.CURRENCY_ORIGIN.INITIALS}, Destination: ${factor.CURRENCY_DESTINATION.INITIALS}, INTEGRATION_ID: ${factor.INTEGRATION_ID}`);
                }
            }

            WBA.log(`Finish all update Currency ${messageDecoded.INITIALS}, (Integration ID: ${messageDecoded.INTEGRATION_ID})`);
            // Delete unupdated documents using BATCH field as parameter
            operationPromise = this.$repository.deleteUnupdatedMonacoCurrenciesFactor(_timeout, messageDecoded.INITIALS, batch);
            rc = await operationPromise.timeout(_timeout);
            if (rc.$status !== WBA.Status.SUCCESS) throw Error(`Fail to deleteUnupdatedMonacoCurrenciesFactor`);

            WBA.log('Finish processMessage on CurrencyFactor: ' + messageDecoded.INITIALS + ' - Integration Id: ' + messageDecoded.INTEGRATION_ID);
            return WBA.success(true);
        } catch (ex) {
            WBA.log('Error on CurrencyFactor in processMessage ID: ' + (messageDecoded ? messageDecoded.INITIALS : '') + ' INTEGRATION_ID: ' + (messageDecoded ? messageDecoded.INTEGRATION_ID : '') + ' ' + JSON.stringify(ex));
            return WBA.exception(ex);
        }
    }

    public decodeMessage(msg: amqp.ConsumeMessage): ICurrencyQueue {
        try {
            if (!msg) throw Error('Missing msg parameter in decodeMessage');
            return <ICurrencyQueue>JSON.parse(msg.content.toString());
        } catch (ex) {
            throw ex;
        }
    }
}
