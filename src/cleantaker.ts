//importing stuff
import {
    Account,
    Commitment,
    Connection,
    PublicKey,
    Transaction,
    TransactionInstruction,
} from '@solana/web3.js';
import fs from 'fs';
import os from 'os';
import { BN } from 'bn.js';
import {
    BookSide,
    BookSideLayout,
    Cluster,
    Config,
    getMultipleAccounts,
    getPerpMarketByBaseSymbol,
    getUnixTs,
    GroupConfig,
    makeCancelAllPerpOrdersInstruction,
    makePlacePerpOrderInstruction,
    EntropyAccount,
    EntropyAccountLayout,
    EntropyCache,
    EntropyCacheLayout,
    EntropyClient,
    EntropyGroup,
    ONE_BN,
    PerpMarket,
    PerpMarketConfig,
    sleep,
    zeroKey,
    getTokenAccountsByOwnerWithWrappedSol,
} from '@friktion-labs/entropy-client';
import { OpenOrders } from '@project-serum/serum';
import path from 'path';
import {
    loadEntropyAccountWithName,
    loadEntropyAccountWithPubkey,
    makeCheckAndSetSequenceNumberInstruction,
    makeInitSequenceInstruction,
    seqEnforcerProgramId,
} from './utils';
import {
    normalizeBookChanges,
    normalizeTrades,
    OrderBook,
    streamNormalized,
    normalizeDerivativeTickers,
} from 'tardis-dev';
import { findProgramAddressSync } from '@project-serum/anchor/dist/cjs/utils/pubkey';

// File containing info of serum markets being quoted
import IDS from './IDS.json';
import { getMintDecimals } from '../node_modules/@project-serum/serum/lib/market';
import Decimal from "decimal.js";
import { privateEncrypt } from 'crypto';

require('dotenv').config({ path: '.env' });

console.log("PATH: ", process.env.KEYPAIR);

// Custom quote parameters
//#region
const paramsFileName = process.env.PARAMS || 'misprice_taker_params.json';
const params = JSON.parse(
    fs.readFileSync(
        path.resolve(__dirname, `../params/${paramsFileName}`),
        'utf-8',
    ),
);

//#endregion

//keypair path
//#region 

// Path to keypair
const payer = new Account(
    JSON.parse(
        fs.readFileSync(
            process.env.KEYPAIR || os.homedir() + '/.config/solana/mm.json',
            'utf-8',
        ),
    ),
);

//#endregion

//config and IDs 
//#region

const config = new Config(IDS);

//group Ids
const groupIds = config.getGroupWithName(params.group) as GroupConfig;
if (!groupIds) {
    throw new Error(`Group ${params.group} not found`);
}
const cluster = groupIds.cluster as Cluster;
const entropyProgramId = groupIds.entropyProgramId;
const entropyGroupKey = groupIds.publicKey;
const control = { isRunning: true, interval: params.interval };



//#endregion


//marketcontext information
//#region

type MarketContext = {
    marketName: string;
    params: any;
    config: PerpMarketConfig;
    market: PerpMarket;
    marketIndex: number;
    bids: BookSide;
    asks: BookSide;
    lastBookUpdate: number;

    tardisBook: TardisBook;
    lastTardisUpdate: number;

    fundingRate: number;
    lastTardisFundingRateUpdate: number;

    sequenceAccount: PublicKey;
    sequenceAccountBump: number;

    sentBidPrice: number;
    sentAskPrice: number;
    lastOrderUpdate: number;
};

//#endregion

//rng
//#region

function getRandomNumber(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.random() * (max - min + 1) + min;
}

//#endregion

//get entropy account and market context
async function listenAccountAndMarketState(
    connection: Connection,
    group: EntropyGroup,
    state: {
        cache: EntropyCache;
        entropyAccount: EntropyAccount;
        marketContexts: MarketContext[];
    },
    stateRefreshInterval: number,
) {
    while (control.isRunning) {
        try {
            const inBasketOpenOrders = state.entropyAccount
                .getOpenOrdersKeysInBasket()
                .filter((pk) => !pk.equals(zeroKey));

            const allAccounts = [
                group.entropyCache,
                state.entropyAccount.publicKey,
                ...inBasketOpenOrders,
                ...state.marketContexts.map(
                    (marketContext) => marketContext.market.bids,
                ),
                ...state.marketContexts.map(
                    (marketContext) => marketContext.market.asks,
                ),
            ];

            const ts = getUnixTs() / 1000;
            const accountInfos = await getMultipleAccounts(connection, allAccounts);

            const cache = new EntropyCache(
                accountInfos[0].publicKey,
                EntropyCacheLayout.decode(accountInfos[0].accountInfo.data),
            );

            const entropyAccount = new EntropyAccount(
                accountInfos[1].publicKey,
                EntropyAccountLayout.decode(accountInfos[1].accountInfo.data),
            );
            const openOrdersAis = accountInfos.slice(
                2,
                2 + inBasketOpenOrders.length,
            );
            for (let i = 0; i < openOrdersAis.length; i++) {
                const ai = openOrdersAis[i];
                const marketIndex = entropyAccount.spotOpenOrders.findIndex((soo) =>
                    soo.equals(ai.publicKey),
                );
                entropyAccount.spotOpenOrdersAccounts[marketIndex] =
                    OpenOrders.fromAccountInfo(
                        ai.publicKey,
                        ai.accountInfo,
                        group.dexProgramId,
                    );
            }

            accountInfos
                .slice(
                    2 + inBasketOpenOrders.length,
                    2 + inBasketOpenOrders.length + state.marketContexts.length,
                )
                .forEach((ai, i) => {
                    state.marketContexts[i].bids = new BookSide(
                        ai.publicKey,
                        state.marketContexts[i].market,
                        BookSideLayout.decode(ai.accountInfo.data),
                    );
                });

            accountInfos
                .slice(
                    2 + inBasketOpenOrders.length + state.marketContexts.length,
                    2 + inBasketOpenOrders.length + 2 * state.marketContexts.length,
                )
                .forEach((ai, i) => {
                    state.marketContexts[i].lastBookUpdate = ts;
                    state.marketContexts[i].asks = new BookSide(
                        ai.publicKey,
                        state.marketContexts[i].market,
                        BookSideLayout.decode(ai.accountInfo.data),
                    );
                });

            state.entropyAccount = entropyAccount;
            state.cache = cache;
        } catch (e) {
            console.error(
                `${new Date().getUTCDate().toString()} failed when loading state`,
                e,
            );
        } finally {
            await sleep(stateRefreshInterval);
        }
    }
}



//load price oracle cache, acct info, and bids and asks for all perp markets
async function loadAccountAndMarketState(
    connection: Connection,
    group: EntropyGroup,
    oldEntropyAccount: EntropyAccount,
    marketContexts: MarketContext[],
): Promise<{
    cache: EntropyCache;
    entropyAccount: EntropyAccount;
    marketContexts: MarketContext[];
}> {
    const inBasketOpenOrders = oldEntropyAccount
        .getOpenOrdersKeysInBasket()
        .filter((pk) => !pk.equals(zeroKey));

    const allAccounts = [
        group.entropyCache,
        oldEntropyAccount.publicKey,
        ...inBasketOpenOrders,
        ...marketContexts.map((marketContext) => marketContext.market.bids),
        ...marketContexts.map((marketContext) => marketContext.market.asks),
    ];

    const ts = getUnixTs() / 1000;
    const accountInfos = await getMultipleAccounts(connection, allAccounts);

    const cache = new EntropyCache(
        accountInfos[0].publicKey,
        EntropyCacheLayout.decode(accountInfos[0].accountInfo.data),
    );

    const entropyAccount = new EntropyAccount(
        accountInfos[1].publicKey,
        EntropyAccountLayout.decode(accountInfos[1].accountInfo.data),
    );
    const openOrdersAis = accountInfos.slice(2, 2 + inBasketOpenOrders.length);
    for (let i = 0; i < openOrdersAis.length; i++) {
        const ai = openOrdersAis[i];
        const marketIndex = entropyAccount.spotOpenOrders.findIndex((soo) =>
            soo.equals(ai.publicKey),
        );
        entropyAccount.spotOpenOrdersAccounts[marketIndex] =
            OpenOrders.fromAccountInfo(
                ai.publicKey,
                ai.accountInfo,
                group.dexProgramId,
            );
    }

    accountInfos
        .slice(
            2 + inBasketOpenOrders.length,
            2 + inBasketOpenOrders.length + marketContexts.length,
        )
        .forEach((ai, i) => {
            marketContexts[i].bids = new BookSide(
                ai.publicKey,
                marketContexts[i].market,
                BookSideLayout.decode(ai.accountInfo.data),
            );
        });

    accountInfos
        .slice(
            2 + inBasketOpenOrders.length + marketContexts.length,
            2 + inBasketOpenOrders.length + 2 * marketContexts.length,
        )
        .forEach((ai, i) => {
            marketContexts[i].lastBookUpdate = ts;
            marketContexts[i].asks = new BookSide(
                ai.publicKey,
                marketContexts[i].market,
                BookSideLayout.decode(ai.accountInfo.data),
            );
        });

    return {
        cache,
        entropyAccount,
        marketContexts,
    };
}

//keep FTX perp books updated via websocket using tardis
async function listenFtxBooks(marketContexts: MarketContext[]) {
    // console.log('listen ftx books')
    const symbolToContext = Object.fromEntries(
        marketContexts.map((mc) => [mc.marketName, mc]),
    );

    const messages = streamNormalized(
        {
            exchange: 'ftx',
            symbols: marketContexts.map((mc) => mc.marketName),
        },
        normalizeTrades,
        normalizeBookChanges,
    );

    for await (const msg of messages) {
        if (msg.type === 'book_change') {
            symbolToContext[msg.symbol].tardisBook.update(msg);
            symbolToContext[msg.symbol].lastTardisUpdate =
                msg.timestamp.getTime() / 1000;
        }
    }
}



//keep FTX perp funding rates updated via websocket using Tardis
async function listenFtxFundingRates(marketContexts: MarketContext[]) {
    // console.log('listen ftx funding rates')
    const symbolToContext = Object.fromEntries(
        marketContexts.map((mc) => [mc.marketName, mc]),
    );

    const messages = streamNormalized(
        {
            exchange: 'ftx',
            symbols: marketContexts.map((mc) => mc.marketName),
        },
        normalizeDerivativeTickers
    );

    for await (const msg of messages) {
        if (msg.type === 'derivative_ticker' && msg.fundingRate !== undefined) {
            symbolToContext[msg.symbol].fundingRate = msg.fundingRate;
            symbolToContext[msg.symbol].lastTardisFundingRateUpdate =
                msg.timestamp.getTime() / 1000;
        }
    }
}

//marketmaker
async function fullMarketMaker() {
    //connection to client
    //#region



    console.log(new Date().toISOString(), "Loading Market Making Params", params);
    console.log(new Date().toISOString(), "Establishing Client Connection...");
    const connection = new Connection(
        process.env.ENDPOINT_URL || config.cluster_urls[cluster],
        'processed' as Commitment,
    );
    const client = new EntropyClient(connection, entropyProgramId);
    console.log(new Date().toISOString(), "Client Connection Established...");

    //#endregion


    //load groups
    //#region

    console.log(new Date().toISOString(), "Loading Entropy Market Groups...");
    const entropyGroup = await client.getEntropyGroup(entropyGroupKey);

    //#endregion

    //load entropyaccount
    //#region

    console.log(new Date().toISOString(), "Loading Entropy Account Details...");
    // load entropyAccount
    let entropyAccount: EntropyAccount;
    if (params.entropyAccountName) {
        entropyAccount = await loadEntropyAccountWithName(
            client,
            entropyGroup,
            payer,
            params.entropyAccountName,
        );
    } else if (params.entropyAccountPubkey) {
        entropyAccount = await loadEntropyAccountWithPubkey(
            client,
            entropyGroup,
            payer,
            new PublicKey(params.entropyAccountPubkey),
        );
    } else {
        throw new Error(
            'Please add entropyAccountName or entropyAccountPubkey to params file',
        );
    }

    //#endregion

    //get btcmarketcontext information
    //#region

    const perpMarkets: PerpMarket[] = [];
    const marketContexts: MarketContext[] = [];

    let btcMarketContext: MarketContext | null = null;;
    for (const baseSymbol in params.assets) {
        console.log(new Date().toISOString(), 'Spinning market for: ', baseSymbol);
        const perpMarketConfig = getPerpMarketByBaseSymbol(
            groupIds,
            baseSymbol,
        ) as PerpMarketConfig;

        const [sequenceAccount, sequenceAccountBump] = findProgramAddressSync(
            [new Buffer(perpMarketConfig.name, 'utf-8'), payer.publicKey.toBytes()],
            seqEnforcerProgramId,
        );

        const perpMarket = await client.getPerpMarket(
            perpMarketConfig.publicKey,
            perpMarketConfig.baseDecimals,
            perpMarketConfig.quoteDecimals,
        );

        perpMarkets.push(perpMarket);

        marketContexts.push({
            marketName: perpMarketConfig.name,
            params: params.assets[baseSymbol].perp,
            config: perpMarketConfig,
            market: perpMarket,
            marketIndex: perpMarketConfig.marketIndex,
            bids: await perpMarket.loadBids(connection),
            asks: await perpMarket.loadAsks(connection),
            lastBookUpdate: 0,
            tardisBook: new TardisBook(),
            lastTardisUpdate: 0,
            sequenceAccount,
            sequenceAccountBump,
            sentBidPrice: 0,
            sentAskPrice: 0,
            lastOrderUpdate: 0,
            fundingRate: 0,
            lastTardisFundingRateUpdate: 0,
        });

        if (baseSymbol === 'BTC')
            btcMarketContext = marketContexts[marketContexts.length - 1];
    }

    //#endregion

    // Initialize all the sequence accounts
    //#region

    const seqAccInstrs = marketContexts.map((mc) =>
        makeInitSequenceInstruction(
            mc.sequenceAccount,
            payer.publicKey,
            mc.sequenceAccountBump,
            mc.marketName,
        ),
    );
    const seqAccTx = new Transaction();
    seqAccTx.add(...seqAccInstrs);

    //#endregion

    //initial transaction
    const seqAccTxid = await client.sendTransaction(seqAccTx, payer, [], undefined, undefined, "Init tx for all markets");

    //load state and define refresh interval
    //#region

    const state = await loadAccountAndMarketState(
        connection,
        entropyGroup,
        entropyAccount,
        marketContexts,
    );

    const stateRefreshInterval = params.stateRefreshInterval || 5000;
    listenAccountAndMarketState(
        connection,
        entropyGroup,
        state,
        stateRefreshInterval,
    );

    //#endregion

    //listen for market context
    //#region

    const listenableMarketContexts = marketContexts.filter((context) => {
        return !(context.params.disableFtxBook || false)
    });

    // console.log('listenable market contexts: ', listenableMarketContexts.map((context) => context.marketName));
    listenFtxBooks(listenableMarketContexts);
    listenFtxFundingRates(listenableMarketContexts);

    //#endregion


    //cancel orders
    //#region

    process.on('SIGINT', function () {
        console.log('Caught keyboard interrupt. Canceling orders');
        control.isRunning = false;
        onExit(client, payer, entropyGroup, entropyAccount, marketContexts);
    });

    //#endregion





    //while running
    while (control.isRunning) {
        try {

            //set up initial info stuff
            //#region

            entropyAccount = state.entropyAccount;

            let j = 0;
            let tx = new Transaction();

            //#endregion

            //for each market context
            for (let i = 0; i < marketContexts.length; i++) {

                //define variables 
                //#region

                const perpMarket = perpMarkets[i];
                let ftxBook = marketContexts[i].tardisBook;
                let ftxFundingRate = marketContexts[i].fundingRate;
                let IVFundingOffset = 0;



                //#endregion

                //special conditions for btc squared
                //#region
                if (marketContexts[i].marketName === "BTC^2-PERP") {
                    if (btcMarketContext === null) {
                        throw new Error("btc market context is null");
                    }
                    ftxBook = btcMarketContext.tardisBook;
                    ftxFundingRate = btcMarketContext.fundingRate;

                    //load implied vol oracle price from the cache

                    const IVoracleCache = state.cache.priceCache[2];
                    const IVoraclePriceI8048 = IVoracleCache.price;
                    const IVoraclePrice = IVoraclePriceI8048.toNumber();
                    IVFundingOffset = calcFundingFromIV(IVoraclePrice, 7);
                }
                //#endregion

                //make instructions
                //#region

                const instrSet = makeMarketUpdateInstructions(
                    entropyGroup,
                    state.cache,
                    entropyAccount,
                    marketContexts[i],
                    ftxBook,
                    ftxFundingRate,
                    IVFundingOffset,
                );



                if (instrSet.length > 0) {

                    //for each instruction make a new transaction
                    instrSet.forEach((ix) => tx.add(ix));
                    j++;
                    if (j === params.batch) {
                        client.sendTransaction(tx, payer, [], undefined, undefined, `${marketContexts[i].marketName} market update tx`);
                        tx = new Transaction();
                        j = 0;
                    }
                }

                //#endregion
            }

            if (tx.instructions.length) {
                client.sendTransaction(tx, payer, [], null, undefined, 'Updating all markets');
            }
        } catch (e) {
            console.log(e);
        } finally {
            await sleep(control.interval);
        }
    }
}

//get best bid and ask 
//#region

class TardisBook extends OrderBook {
    getSizedBestBid(quoteSize: number): number | undefined {
        let rem = quoteSize;
        for (const bid of this.bids()) {
            rem -= bid.amount * bid.price;
            if (rem <= 0) {
                return bid.price;
            }
        }
        return undefined;
    }
    getSizedBestAsk(quoteSize: number): number | undefined {
        let rem = quoteSize;
        for (const ask of this.asks()) {
            rem -= ask.amount * ask.price;
            if (rem <= 0) {
                return ask.price;
            }
        }
        return undefined;
    }
}

//#endregion


//calculate funding
//#region 

//calculate funding from IV function
function calcFundingFromIV(
    IVoraclePrice: number,
    days: number
): number {
    return (IVoraclePrice / 100) ** 2 / 365 * days;
}

//#endregion

//how the market maker updates stuff
function makeMarketUpdateInstructions(
    //params
    //#region
    group: EntropyGroup,
    cache: EntropyCache,
    entropyAccount: EntropyAccount,
    marketContext: MarketContext,
    ftxBook: TardisBook,
    ftxFundingRate: number,
    IVFundingOffset: number,
    //#endregion
): TransactionInstruction[] {
    // Right now only uses the perp

    //defining constants from marketcontext 
    //#region


    const marketIndex = marketContext.marketIndex;
    const market = marketContext.market;
    const bids = marketContext.bids;
    const asks = marketContext.asks;


    const oracleCache = cache.priceCache[marketIndex];
    const oraclePriceI8048 = oracleCache.price;
    const oraclePrice = group.cachePriceToUi(
        oraclePriceI8048, marketIndex
    );

    //#endregion

    //update log
    //#region

    const lastUpdate = oracleCache.lastUpdate;
    //console.log("lastUpdate: ", Date.now() / 1000 - lastUpdate.toNumber());


    //#endregion

    //get ftx info
    //#region

    let ftxBid = ftxBook.getSizedBestBid(
        marketContext.params.ftxSize || 100000,
    );
    let ftxAsk = ftxBook.getSizedBestAsk(
        marketContext.params.ftxSize || 100000,
    );
    let ftxFunding = ftxFundingRate || 0.0;

    let foundFTX = true;
    if (ftxBid === undefined || ftxAsk === undefined) {
        foundFTX = false;
        // TODO deal with this better; probably cancel all if there are any orders open
        ftxBid = new Decimal(oraclePrice).sub(0.01 * oraclePrice).toNumber();
        ftxAsk = new Decimal(oraclePrice).add(0.01 * oraclePrice).toNumber();
    }

    // For BTC^2, square BTC Price and normalize
    if (marketContext.marketName === "BTC^2-PERP") {
        if (foundFTX) {
            ftxBid = new Decimal(ftxBid).pow(2).toNumber() / 1000000;
            ftxAsk = new Decimal(ftxAsk).pow(2).toNumber() / 1000000;
            //console.log(`ftx quote for ${marketContext.marketName}: ${ftxBid} @ ${ftxAsk}`)
            ftxFunding = new Decimal(ftxFundingRate).toNumber();
        } else {

            //console.log(`oracle quote for ${marketContext.marketName}: ${ftxBid} @ ${ftxAsk}`)
        }

    }

    else {

    }
    //#endregion

    //defining parameters for trading
    //#region

    const fairBid = ftxBid;
    const fairAsk = ftxAsk;

    const fairValue = (fairBid + fairBid) / 2;
    const ftxSpread = (fairAsk - fairBid) / fairValue;
    const equity = entropyAccount.computeValue(group, cache).toNumber();
    const perpAccount = entropyAccount.perpAccounts[marketIndex];

    const basePos = perpAccount.getBasePositionUi(market);
    const fundingBias = ftxFunding || 0;

    const timeLimit = marketContext.params.timeLimit;

    const sizePerc = marketContext.params.sizePerc;
    const takePerc = marketContext.params.takePerc;

    const maxTakePortPerc = marketContext.params.maxTakePortPerc;
    const maxTakePortNotional = marketContext.params.maxTakePortNotional;
    const minTakeNotional = marketContext.params.minTakeNotional;
    const maxTakeNotional = marketContext.params.maxTakeNotional;
    const minTakePerc = marketContext.params.minTakePerc;
    const maxTakePerc = marketContext.params.maxTakePerc;


    const leanCoeff = marketContext.params.leanCoeff;
    const edge = (marketContext.params.edge || 0.0015) + ftxSpread / 2;
    const bias = marketContext.params.bias;
    const mispriced = marketContext.params.mispriceThresh;
    const requoteThresh = marketContext.params.requoteThresh;
    const takeSpammers = marketContext.params.takeSpammers;
    const spammerCharge = marketContext.params.spammerCharge;
    const size = (equity * sizePerc) / fairValue;
    const lean = 0//(-leanCoeff * basePos) / size;

    //#endregion

    //special condition for btc^2
    //#region



    if (marketContext.marketName === "BTC^2-PERP") {

    }

    //#endregion

    //main bot stuff
    //#region

    //liquidity provision
    //#region

    let bidPrice = fairValue * (1 - edge + lean + bias + 1.3 * IVFundingOffset);
    let askPrice = fairValue * (1 + edge + lean + bias + 1.7 * IVFundingOffset);

    let [modelBidPrice, nativeBidSize] = market.uiToNativePriceQuantity(
        bidPrice,
        size,
    );
    let [modelAskPrice, nativeAskSize] = market.uiToNativePriceQuantity(
        askPrice,
        size,
    );
    let [modelBidPrice2, nativeBidSize2] = market.uiToNativePriceQuantity(
        bidPrice * 0.993,
        size * 4.87,
    );
    let [modelAskPrice2, nativeAskSize2] = market.uiToNativePriceQuantity(
        askPrice * 1.007,
        size * 4.95,
    );

    console.log('--------------------------------------------------------------------------------------------------')
    console.log(`${marketContext.marketName}`)
    console.log(`${marketContext.marketName}: liquidity provision info`)
    console.log(`${marketContext.marketName}: tight quote:`)
    console.log(`${marketContext.marketName}: ${modelBidPrice} at ${modelAskPrice}, ${nativeBidSize} by ${nativeAskSize}`)
    console.log(`${marketContext.marketName}: wide quote:`)
    console.log(`${marketContext.marketName}: ${modelBidPrice2} at ${modelAskPrice2}, ${nativeBidSize2} by ${nativeAskSize2}`)
    console.log('--------------------------------------------------------------------------------------------------')


    const bestBid = bids.getBest();
    const bestAsk = asks.getBest();

    const bookAdjBid =
        bestAsk !== undefined
            ? BN.min(bestAsk.priceLots.sub(ONE_BN), modelBidPrice)
            : modelBidPrice;
    const bookAdjAsk =
        bestBid !== undefined
            ? BN.max(bestBid.priceLots.add(ONE_BN), modelAskPrice)
            : modelAskPrice;

    const bookAdjBid2 =
        bestAsk !== undefined
            ? BN.min(bestAsk.priceLots.sub(ONE_BN), modelBidPrice2)
            : modelBidPrice2;
    const bookAdjAsk2 =
        bestBid !== undefined
            ? BN.max(bestBid.priceLots.add(ONE_BN), modelAskPrice2)
            : modelAskPrice2;



    //console.log(new Date().toISOString(), `${marketContext.marketName} model bid: `, modelBidPrice.toString(), 'model ask: ', modelAskPrice.toString(), 'oracle px: ', new Decimal(oraclePrice));


    //#endregion

    //moving orders based on updates
    //#region


    let moveOrders = false;
    if (marketContext.lastBookUpdate >= marketContext.lastOrderUpdate) {
        // if entropy book was updated recently, then EntropyAccount was also updated
        const openOrders = entropyAccount
            .getPerpOpenOrders()
            .filter((o) => o.marketIndex === marketIndex);
        moveOrders = openOrders.length < 2 || openOrders.length > 2;
        for (const o of openOrders) {
            const refPrice = o.side === 'buy' ? bookAdjBid : bookAdjAsk;
            moveOrders =
                moveOrders ||
                Math.abs(o.price.toNumber() / refPrice.toNumber() - 1) > requoteThresh;
        }
    } else {
        // If order was updated before EntropyAccount, then assume that sent order already executed
        moveOrders =
            moveOrders ||
            Math.abs(marketContext.sentBidPrice / bookAdjBid.toNumber() - 1) >
            requoteThresh ||
            Math.abs(marketContext.sentAskPrice / bookAdjAsk.toNumber() - 1) >
            requoteThresh;
    }

    //#endregion

    //transaction building
    const instructions: TransactionInstruction[] = [
        makeCheckAndSetSequenceNumberInstruction(
            marketContext.sequenceAccount,
            payer.publicKey,
            Math.round(getUnixTs() * 1000),
        ),
    ];


    //taking mispricings

    //adjusting order info
    //#region

    let allowLiftAsk = true;
    let allowHitBid = true;

    const takeSize = (equity * takePerc) / fairValue;

    let mispricedBid = (1 + mispriced) * fairValue;
    let mispricedAsk = (1 - mispriced) * fairValue;

    let hitBidSize = (equity * takePerc) / mispricedBid;
    let liftAskSize = (equity * takePerc) / mispricedBid;

    //#endregion

    //per trade info
    //#region
    let notionalHitBidSize = mispricedBid * hitBidSize;
    let notionalLiftAskSize = mispricedAsk * liftAskSize;


    console.log('--------------------------------------------------------------------------------------------------')
    console.log(`${marketContext.marketName}: taker info`)
    console.log(`${marketContext.marketName}: per-trade %: ${takePerc}`)
    console.log(`${marketContext.marketName}: fairvalue: ${fairValue}`)
    console.log(`${marketContext.marketName}: threshold: ${mispriced}`)
    console.log(`${marketContext.marketName}: lift offer below ${mispricedAsk}`)
    console.log(`for $${notionalLiftAskSize}`)
    console.log(`liftAskSize: ${liftAskSize}`)
    console.log(`${marketContext.marketName}: hit bid above ${mispricedBid}`)
    console.log(`for $${notionalHitBidSize}`)
    console.log(`hitBidSize: ${hitBidSize}`)
    console.log('--------------------------------------------------------------------------------------------------')

    //#endregion

    //per trade resizing
    //#region

    if (notionalHitBidSize > maxTakeNotional) {
        console.log(`${marketContext.marketName}: WARNING: NOTIONAL PER TRADE LIMIT `)
        console.log(`${marketContext.marketName}: intended hit bid would be ${notionalLiftAskSize}`)
        console.log(`${marketContext.marketName}: this is greater than notional per trade limit of $${maxTakeNotional}`)
        console.log(`${marketContext.marketName}: resizing down to trade limit of $${maxTakeNotional}`)
        notionalHitBidSize = maxTakeNotional;
        hitBidSize = notionalHitBidSize / mispricedBid;
        console.log(`hitBidSize: ${hitBidSize}`)
    }

    if (notionalLiftAskSize > maxTakeNotional) {
        console.log(`${marketContext.marketName}: WARNING: NOTIONAL PER TRADE LIMIT `)
        console.log(`${marketContext.marketName}: intended lift ask would be ${notionalLiftAskSize}`)
        console.log(`${marketContext.marketName}: this is greater than notional per trade limit of $${maxTakeNotional}`)
        console.log(`${marketContext.marketName}: resizing down to trade limit of $${maxTakeNotional}`)
        notionalLiftAskSize = maxTakeNotional;
        liftAskSize = notionalLiftAskSize / mispricedAsk;
        console.log(`liftAskSize: ${liftAskSize}`)
    }

    //#endregion

    //timing info
    //#region
    let secondsSinceBoot = Date.now() / 1000 - bootTime;
    let secondSinceLastTrade = Date.now() / 1000 - lastTradeTime;
    let timeUntilTrade = Math.max(0, lastTradeTime + timeLimit - Date.now() / 1000);
    let inTimeout = (Date.now() / 1000 < lastTradeTime + timeLimit) ? true : false;

    /*
    console.log('--------------------------------------------------------------------------------------------------')
    console.log(`seconds since boot: ${secondsSinceBoot}`)
    console.log(`timelimit: ${timeLimit}`)
    console.log(`seconds since last trade: ${secondSinceLastTrade}`)
    console.log(`time until trade: ${timeUntilTrade}`)
    console.log(`are we in timeout: ${inTimeout}`)
    console.log('--------------------------------------------------------------------------------------------------')
    //console.log(`last trade time was: ${Date.now() - lastTradeTime} seconds ago`)
    */

    //#endregion

    //position + portfolio info
    //#region
    let notionalPosition = basePos * oraclePrice;
    let equityPerc = 100 * notionalPosition / equity
    let intendedPositionChangeHitBid = notionalPosition - notionalHitBidSize;
    let intendedPositionChangeHitBidPerc = 100 * intendedPositionChangeHitBid / equity;
    let intendedPositionChangeLiftAsk = notionalPosition + notionalLiftAskSize;
    let intendedPositionChangeLiftAskPerc = 100 * intendedPositionChangeLiftAsk / equity;

    console.log('--------------------------------------------------------------------------------------------------')
    console.log(`total equity: ${equity}`)
    console.log(`${marketContext.marketName}: base position: ${basePos}`)
    console.log(`${marketContext.marketName}: notional position: $${notionalPosition}`)
    console.log(`${marketContext.marketName}: percent of equity: ${equityPerc} % `)
    console.log(`${marketContext.marketName}: post-take percent of equity (hit bid): ${intendedPositionChangeHitBidPerc} % `)
    console.log(`${marketContext.marketName}: post-take percent of equity (lift ask): ${intendedPositionChangeLiftAskPerc} % `)
    console.log('--------------------------------------------------------------------------------------------------')


    //#endregion

    //position limiting
    //#region

    //resizes order if above position limit

    if (intendedPositionChangeHitBid < -maxTakePortNotional) {
        console.log(`${marketContext.marketName}: WARNING: NOTIONAL POSITION LIMIT `)
        console.log(`${marketContext.marketName}: intended hit bid would increase portfolio deployment to $${intendedPositionChangeHitBid}`)
        console.log(`${marketContext.marketName}: this exceeds notional position limit of $-${maxTakePortNotional}`)
        console.log(`${marketContext.marketName}: resizing down to trade limit of $${maxTakePortNotional}`)
        notionalHitBidSize = (maxTakePortNotional + notionalPosition);
        intendedPositionChangeHitBid = notionalPosition - notionalHitBidSize;
        intendedPositionChangeHitBidPerc = 100 * intendedPositionChangeHitBid / equity;
        console.log(`${marketContext.marketName}: new notional: $${intendedPositionChangeHitBid}`)
        console.log(`${marketContext.marketName}: new percentage: ${intendedPositionChangeHitBidPerc}%`)
        hitBidSize = notionalHitBidSize / mispricedBid;
        console.log(`hitBidSize: ${hitBidSize}`)
    }
    if (intendedPositionChangeHitBidPerc < -maxTakePortPerc * 100) {
        console.log(`${marketContext.marketName}: WARNING: PERCENTAGE POSITION LIMIT `)
        console.log(`${marketContext.marketName}: intended hit bid would increase portfolio deployment to ${intendedPositionChangeHitBidPerc}%`)
        console.log(`${marketContext.marketName}: this exceeds percentage position limit of -${maxTakePortPerc * 100}%`)
        console.log(`${marketContext.marketName}: resizing down to trade limit of ${maxTakePortPerc * 100}%`)
        notionalHitBidSize = (maxTakePortPerc * equity + notionalPosition);
        intendedPositionChangeHitBid = notionalPosition - notionalHitBidSize;
        intendedPositionChangeHitBidPerc = 100 * intendedPositionChangeHitBid / equity;
        console.log(`${marketContext.marketName}: new notional: $${intendedPositionChangeHitBid}`)
        console.log(`${marketContext.marketName}: new percentage: ${intendedPositionChangeHitBidPerc}%`)
        hitBidSize = notionalHitBidSize / mispricedBid;
        console.log(`hitBidSize: ${hitBidSize}`)
    }

    if (intendedPositionChangeLiftAsk > maxTakePortNotional) {
        console.log(`${marketContext.marketName}: WARNING: NOTIONAL POSITION LIMIT `)
        console.log(`${marketContext.marketName}: intended lift ask would increase portfolio deployment to $${intendedPositionChangeLiftAsk}`)
        console.log(`${marketContext.marketName}: this exceeds notional position limit of $${maxTakePortNotional}`)
        console.log(`${marketContext.marketName}: resizing down to trade limit of $${maxTakePortNotional}`)
        notionalLiftAskSize = maxTakePortNotional - notionalPosition;
        intendedPositionChangeLiftAsk = notionalPosition + notionalLiftAskSize;
        intendedPositionChangeLiftAskPerc = 100 * intendedPositionChangeLiftAsk / equity;
        console.log(`${marketContext.marketName}: new notional: $${intendedPositionChangeLiftAsk}`)
        console.log(`${marketContext.marketName}: new percentage: ${intendedPositionChangeLiftAskPerc}%`)
        liftAskSize = notionalLiftAskSize / mispricedAsk;
        console.log(`liftAskSize: ${liftAskSize}`)
    }
    if (intendedPositionChangeLiftAskPerc > maxTakePortPerc * 100) {
        console.log(`${marketContext.marketName}: WARNING: PERCENTAGE POSITION LIMIT `)
        console.log(`${marketContext.marketName}: intended lift ask would increase portfolio deployment to ${intendedPositionChangeLiftAskPerc}%`)
        console.log(`${marketContext.marketName}: resizing down to trade limit of ${maxTakePortPerc * 100}%`)
        notionalLiftAskSize = maxTakePortPerc * equity - notionalPosition;
        intendedPositionChangeLiftAsk = notionalPosition + notionalLiftAskSize;
        intendedPositionChangeLiftAskPerc = 100 * intendedPositionChangeLiftAsk / equity;
        console.log(`${marketContext.marketName}: new notional: $${intendedPositionChangeLiftAsk}`)
        console.log(`${marketContext.marketName}: new percentage: ${intendedPositionChangeLiftAskPerc}%`)
        liftAskSize = notionalLiftAskSize / mispricedAsk;
        console.log(`liftAskSize: ${liftAskSize}`)


    }


    //#endregion

    //taker instructions
    //#region

    let [takerModelBid, nativeBidSize_Taker] = market.uiToNativePriceQuantity(
        mispricedBid,
        hitBidSize,
    );
    let [takerModelAsk, nativeAskSize_Taker] = market.uiToNativePriceQuantity(
        mispricedAsk,
        liftAskSize,
    );




    if (bestBid !== undefined && bestBid?.price > mispricedBid && !inTimeout && allowHitBid) {
        console.log('--------------------------------------------------------------------------------------------------')
        console.log(`${marketContext.marketName}: mispriced by ${bestBid?.price / fairValue} `)
        console.log(`${marketContext.marketName}: bestBid: ${bestBid?.price} above ${mispricedBid} `)
        console.log(`${marketContext.marketName}: selling ${nativeBidSize_Taker} at ${mispricedBid} `)
        console.log('--------------------------------------------------------------------------------------------------')
        const takerSell = makePlacePerpOrderInstruction(
            entropyProgramId,
            group.publicKey,
            entropyAccount.publicKey,
            payer.publicKey,
            cache.publicKey,
            market.publicKey,
            market.bids,
            market.asks,
            market.eventQueue,
            entropyAccount.getOpenOrdersKeysInBasket(),
            takerModelBid,
            nativeBidSize_Taker,
            new BN(Date.now()),
            'sell',
            'ioc',
        );
        //instructions.push(takerSell);
        lastTradeTime = Date.now() / 1000;

    } else if (bestAsk !== undefined && bestAsk?.price < mispricedAsk && !inTimeout && allowLiftAsk) {
        console.log('--------------------------------------------------------------------------------------------------')
        console.log(`${marketContext.marketName} mispriced by ${bestAsk?.price / fairValue} `)
        console.log(`${marketContext.marketName} bestAsk: ${bestAsk?.price} above ${mispricedAsk} `)
        console.log(`${marketContext.marketName}: buying ${nativeBidSize_Taker} at ${mispricedAsk} `)
        console.log('--------------------------------------------------------------------------------------------------')
        const takerBuy = makePlacePerpOrderInstruction(
            entropyProgramId,
            group.publicKey,
            entropyAccount.publicKey,
            payer.publicKey,
            cache.publicKey,
            market.publicKey,
            market.bids,
            market.asks,
            market.eventQueue,
            entropyAccount.getOpenOrdersKeysInBasket(),
            takerModelBid,
            nativeAskSize_Taker,
            new BN(Date.now()),
            'buy',
            'ioc',
        );
        //instructions.push(takerBuy);
        lastTradeTime = Date.now() / 1000;
    }

    //#endregion

    //clearing 1 lot orders that are used to manipulate price
    //#region

    if (
        takeSpammers &&
        bestBid !== undefined &&
        bestBid.sizeLots.eq(ONE_BN) &&
        bestBid.priceLots.toNumber() / modelAskPrice.toNumber() - 1 >
        spammerCharge * edge + 0.0005
    ) {
        const takerSell = makePlacePerpOrderInstruction(
            entropyProgramId,
            group.publicKey,
            entropyAccount.publicKey,
            payer.publicKey,
            cache.publicKey,
            market.publicKey,
            market.bids,
            market.asks,
            market.eventQueue,
            entropyAccount.getOpenOrdersKeysInBasket(),
            bestBid.priceLots,
            ONE_BN,
            new BN(Date.now()),
            'sell',
            'ioc',
        );
        //instructions.push(takerSell);
    } else if (
        takeSpammers &&
        bestAsk !== undefined &&
        bestAsk.sizeLots.eq(ONE_BN) &&
        modelBidPrice.toNumber() / bestAsk.priceLots.toNumber() - 1 >
        spammerCharge * edge + 0.0005
    ) {
        const takerBuy = makePlacePerpOrderInstruction(
            entropyProgramId,
            group.publicKey,
            entropyAccount.publicKey,
            payer.publicKey,
            cache.publicKey,
            market.publicKey,
            market.bids,
            market.asks,
            market.eventQueue,
            entropyAccount.getOpenOrdersKeysInBasket(),
            bestAsk.priceLots,
            ONE_BN,
            new BN(Date.now()),
            'buy',
            'ioc',
        );
        //instructions.push(takerBuy);
    }
    //#endregion

    //moving orders  
    if (moveOrders) {
        // cancel all, requote
        const cancelAllInstr = makeCancelAllPerpOrdersInstruction(
            entropyProgramId,
            group.publicKey,
            entropyAccount.publicKey,
            payer.publicKey,
            market.publicKey,
            market.bids,
            market.asks,
            new BN(20),
        );

        const placeBidInstr = makePlacePerpOrderInstruction(
            entropyProgramId,
            group.publicKey,
            entropyAccount.publicKey,
            payer.publicKey,
            cache.publicKey,
            market.publicKey,
            market.bids,
            market.asks,
            market.eventQueue,
            entropyAccount.getOpenOrdersKeysInBasket(),
            bookAdjBid,
            nativeBidSize,
            new BN(Date.now()),
            'buy',
            'postOnlySlide',
        );

        const placeBidInstr2 = makePlacePerpOrderInstruction(
            entropyProgramId,
            group.publicKey,
            entropyAccount.publicKey,
            payer.publicKey,
            cache.publicKey,
            market.publicKey,
            market.bids,
            market.asks,
            market.eventQueue,
            entropyAccount.getOpenOrdersKeysInBasket(),
            bookAdjBid2,
            nativeBidSize2,
            new BN(Date.now()),
            'buy',
            'postOnlySlide',
        );

        const placeAskInstr = makePlacePerpOrderInstruction(
            entropyProgramId,
            group.publicKey,
            entropyAccount.publicKey,
            payer.publicKey,
            cache.publicKey,
            market.publicKey,
            market.bids,
            market.asks,
            market.eventQueue,
            entropyAccount.getOpenOrdersKeysInBasket(),
            bookAdjAsk,
            nativeAskSize,
            new BN(Date.now()),
            'sell',
            'postOnlySlide',
        );

        const placeAskInstr2 = makePlacePerpOrderInstruction(
            entropyProgramId,
            group.publicKey,
            entropyAccount.publicKey,
            payer.publicKey,
            cache.publicKey,
            market.publicKey,
            market.bids,
            market.asks,
            market.eventQueue,
            entropyAccount.getOpenOrdersKeysInBasket(),
            bookAdjAsk2,
            nativeAskSize2,
            new BN(Date.now()),
            'sell',
            'postOnlySlide',
        );

        instructions.push(cancelAllInstr);
        //instructions.push(placeBidInstr);
        //instructions.push(placeBidInstr2);

        //instructions.push(placeAskInstr);
        //instructions.push(placeAskInstr2);

        /*
        console.log(
            new Date().toISOString(), `${ marketContext.marketName } Requoting sentBidPx: ${ marketContext.sentBidPrice } newBidPx: ${ bookAdjBid } sentAskPx: ${ marketContext.sentAskPrice } newAskPx: ${ bookAdjAsk } spread: ${ bookAdjAsk.toNumber() - bookAdjBid.toNumber() } `,
        );
        */
        marketContext.sentBidPrice = bookAdjBid.toNumber();
        marketContext.sentAskPrice = bookAdjAsk.toNumber();
        marketContext.lastOrderUpdate = getUnixTs() / 1000;
    } else {
        /*
        console.log(
            new Date().toISOString(), `${ marketContext.marketName } Not requoting... No need to move orders`,
        );
        */
    }

    //other cases
    //#region
    // if instruction is only the sequence enforcement, then just send empty
    if (instructions.length === 1) {
        return [];
    } else {
        return instructions;
    }
    //#endregion


    //#endregion
}

//exit logic  
async function onExit(
    client: EntropyClient,
    payer: Account,
    group: EntropyGroup,
    entropyAccount: EntropyAccount,
    marketContexts: MarketContext[],
) {
    await sleep(control.interval);
    entropyAccount = await client.getEntropyAccount(
        entropyAccount.publicKey,
        group.dexProgramId,
    );
    let tx = new Transaction();
    const txProms: any[] = [];
    for (let i = 0; i < marketContexts.length; i++) {
        const mc = marketContexts[i];
        const cancelAllInstr = makeCancelAllPerpOrdersInstruction(
            entropyProgramId,
            group.publicKey,
            entropyAccount.publicKey,
            payer.publicKey,
            mc.market.publicKey,
            mc.market.bids,
            mc.market.asks,
            new BN(20),
        );
        tx.add(cancelAllInstr);
        if (tx.instructions.length === params.batch) {
            console.log(new Date().toISOString(), `${mc.marketName} cancelling all orders`);
            txProms.push(client.sendTransaction(tx, payer, [], undefined, undefined, mc.marketName));
            tx = new Transaction();
        }
    }

    if (tx.instructions.length) {
        txProms.push(client.sendTransaction(tx, payer, [], undefined, undefined, "Cancelling order for all markets: "));
    }
    const txids = await Promise.all(txProms);
    txids.forEach((txid) => {
        console.log(new Date().toISOString(), `cancel successful: ${txid.toString()} `);
    });
    process.exit();
}

function startMarketMaker() {
    if (control.isRunning) {
        console.log('run market maker')
        fullMarketMaker().finally(startMarketMaker);
    }
}

process.on('unhandledRejection', function (err, promise) {
    console.error(
        'Unhandled rejection (promise: ',
        promise,
        ', reason: ',
        err,
        ').',
    );
});

let bootTime = Date.now() / 1000;
let lastTradeTime = Date.now() / 1000 - 3600;
startMarketMaker();