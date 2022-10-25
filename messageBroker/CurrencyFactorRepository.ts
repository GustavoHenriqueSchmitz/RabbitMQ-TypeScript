import { WBA } from 'WBA-CORE';
import { MongoConnection } from 'WBA-CORE/dist/persistence/MongoController';
import { CollectionConstants } from '../../../util/CollectionConstants';
import * as CurrencyFatorModel from 'WBA-Model/dist/quotation/CurrencyFactor';
import { ICurrencyQueue } from 'WBA-Model/dist/interface/product/CurrencyFactor';
import { ICurrencyFactor } from 'WBA-Model/dist/interface/product/CurrencyFactor';
import { documentErrorSecondStep } from 'WBA-Model/dist/interface/common/IDocumentError';

export class CurrencyFactorRepository extends WBA.RepositoryTransaction {

    constructor(customDBConnection?: MongoConnection) {
        super(customDBConnection);
    }

    public async clearDocumentError(_timeout: number, currency: ICurrencyQueue): Promise<WBA.Webbuffet> {
        try {
            if (!_timeout) return WBA.error(WBA.Status.ERROR_INVALID_PARAMETER, 'Missing timeout parameter in clearDocumentError');

            const filter = { ID: currency.ID, DOCUMENT_ERROR: { $ne: null } };
            const dataUpdate = { $pull: { DOCUMENT_ERROR: { "SUBJECT.ID": documentErrorSecondStep.PROCESS_SECOND_STEP.ID } } };
            const result = await this.collection(CollectionConstants.CURRENCY).updateMany(filter, dataUpdate);

            const filterNull = { ID: currency.ID, DOCUMENT_ERROR: { $size: 0 } };
            await this.collection(CollectionConstants.CURRENCY).updateMany(filterNull, { $set: { DOCUMENT_ERROR: null } });

            return WBA.success(result);
        } catch (ex) {
            return WBA.exception(ex);
        }
    }

    public async saveCurrencyFactor(_timeout: number, data: ICurrencyFactor): Promise<WBA.Webbuffet> {
        try {
            if (!_timeout) return WBA.error(WBA.Status.ERROR_INVALID_PARAMETER, 'Missing timeout parameter in updateCurrencyIntegrationID');
            if (!data) return WBA.error(WBA.Status.ERROR_NULL_PARAMETER, 'Missing data parameter in updateCurrencyIntegrationID');

            let rc: WBA.Webbuffet = null;
            let operationPromise: Promise<WBA.Webbuffet> = null;

            rc = WBA.jsonService().validateAndParse3(CurrencyFatorModel, data);
            if (rc.$status !== WBA.Status.SUCCESS) return WBA.error(WBA.Status.FAILED, `Fail to validate CurrencyFactorModel: ${JSON.stringify(rc.$data)}`);
            const validData: CurrencyFatorModel.CurrencyFactor = rc.$data;
            rc = await this.findOne(CollectionConstants.CURRENCY_FACTOR, { INTEGRATION_ID: validData.$INTEGRATION_ID }, {});
            validData.$UPDATED_AT = (data.UPDATED_AT) ? new Date(data.UPDATED_AT) : null;
            validData.$UPDATED_LAST = new Date();
            const currencyFactor = rc.$data;

            if ( currencyFactor ) {
                const filter = { INTEGRATION_ID: currencyFactor.INTEGRATION_ID };
                const dataUpdate = { $set:  this.updateQueryFactory(validData) };

                rc = await this.findOneAndUpdateByQuery(CollectionConstants.CURRENCY_FACTOR, filter, dataUpdate, _timeout);
                if (rc.$status !== WBA.Status.SUCCESS) return WBA.error(WBA.Status.FAILED, `fail update currency factor by query ${JSON.stringify(rc.$data)}`);
            } else {
                operationPromise = this.insertOne(CollectionConstants.CURRENCY_FACTOR, validData);
                rc = await operationPromise.timeout(_timeout);
                if (rc.$status != WBA.Status.SUCCESS) return rc;
            }
            return WBA.success(true);
        } catch (ex) {
            return WBA.exception(ex);
        }
    }

    public async findAllCurrency(_timeout: number): Promise<WBA.Webbuffet> {
        try {
            if (!_timeout) return WBA.error(WBA.Status.ERROR_INVALID_PARAMETER, 'Missing timeout parameter in findAllCurrency');

            let rc: WBA.Webbuffet = null;
            rc = await this.find(CollectionConstants.CURRENCY, { ACTIVE: true }, { ID: 1, INITIALS: 1, NAME: 1, INTEGRATION_ID: 1, UPDATED_AT: 1 });
            const currencyList = rc.$data;
            return WBA.success(currencyList);
        } catch (ex) {
            return WBA.exception(ex);
        }
    }

    public async setMonacoCurrenciesFactorBatch(_timeout: number, currencyInitials: string, batch: string): Promise<WBA.Webbuffet> {
        try {
            if (!_timeout) return WBA.error(WBA.Status.ERROR_NULL_PARAMETER, `Missing _timeout to setMonacoCurrenciesFactorBatch`);
            if (!currencyInitials) return WBA.error(WBA.Status.ERROR_NULL_PARAMETER, `Missing currencyInitials to setMonacoCurrenciesFactorBatch`);
            if (!batch) return WBA.error(WBA.Status.ERROR_NULL_PARAMETER, `Missing batch to setMonacoCurrenciesFactorBatch`);

            await this.collection(CollectionConstants.CURRENCY_FACTOR).updateMany({ "CURRENCY_ORIGIN.INITIALS": currencyInitials }, { $set: { BATCH: batch } });
            await this.collection(CollectionConstants.CURRENCY_FACTOR).updateMany({ "CURRENCY_DESTINATION.INITIALS": currencyInitials }, { $set: { BATCH: batch } });

            return WBA.success(true);
        } catch (ex) {
            return WBA.exception(ex);
        }
    }

    public async deleteUnupdatedMonacoCurrenciesFactor(_timeout: number, currencyInitials: string, batch: string): Promise<WBA.Webbuffet> {
        try {
            if (!_timeout) return WBA.error(WBA.Status.ERROR_NULL_PARAMETER, `Missing _timeout to deleteUnupdatedMonacoCurrenciesFactor`);
            if (!currencyInitials) return WBA.error(WBA.Status.ERROR_NULL_PARAMETER, `Missing currencyInitials to deleteUnupdatedMonacoCurrenciesFactor`);
            if (!batch) return WBA.error(WBA.Status.ERROR_NULL_PARAMETER, `Missing batch to deleteUnupdatedMonacoCurrenciesFactor`);

            await this.deleteMany(CollectionConstants.CURRENCY_FACTOR, { "CURRENCY_ORIGIN.INITIALS": currencyInitials, "BATCH": { $eq: batch } }, null);
            await this.deleteMany(CollectionConstants.CURRENCY_FACTOR, { "CURRENCY_DESTINATION.INITIALS": currencyInitials, "BATCH": { $eq: batch } }, null);

            return WBA.success(true);
        } catch (ex) {
            return WBA.exception(ex);
        }
    }
}
