const axios = require("axios");
const dotenv = require("dotenv");
const datefns = require("date-fns");
const mongoose = require("mongoose");
const pact = require("pact-lang-api");

const batch_no_use = 1;
const server_no_use = 1;

dotenv.config();
mongoose.connect(process.env.mongo_sandbox);

/* 
    Configuration of MongoDB collection that contains the batch txs to send, which covers users' pending balances; 
        we have 8 servers running which process the txs in 4 batches. Each batch covers ~5K txs.
*/
const paymentBatchesSchema = mongoose.Schema(
    {
        username: String,
        address: String,
        pendingBalanceMorning: Number,
        pendingBalanceEvening: Number,
        server: Number,
        batch: Number,
        investigate: Boolean,
        setTimestamp: Number,
        processedTimeStamp: Number, 
        requestKey: String,
        status: String
    }
);
const paymentBatches = mongoose.model("payment-batches", paymentBatchesSchema, "payment-batches");

/* 
    Configuration of MongoDB collection that contains users' wallet info, which are all created on sign-up.
*/
const accountsWalletDescsSchema = mongoose.Schema(
    {
        username: String,
        accountName: String,
        accountDesignation: String,
        publicKey: String,
        aggregateBalance: Number,
        pendingBalanceMorning: Number,
        pendingBalanceEvening: Number
    }
);
const accountsWalletDescs = mongoose.model("accounts-wallet-descs", accountsWalletDescsSchema, "accounts-wallet-descs");

/* 
    Configuration of MongoDB collection that contains notifications info; 
        this info is used to send users' a notice of payment on the completion of their txs.
*/
const notificationsDescsSchema = mongoose.Schema(
    {
        by: String,
        target: String,
        byProfileImage: String,
        type: String,
        message: String,
        link: String,
        read: Boolean,
        timeStamp: Number
    }
);
const notificationsDescs = mongoose.model("notifications-descs", notificationsDescsSchema, "notifications-descs");

const formatPayoutBalance = new Intl.NumberFormat(
    'en-US',
    {
        useGrouping: false,
        minimumFractionDigits: 2,
        maximumFractionDigits: 12
    }
);

const finalizeUserPayouts = async (server_no, batch_no) => {
    /* 
        Pull of all pending txs that need to be sent from the payment-batches collection. 
            Note that a processedTimeStamp of 0 indicates that the tx has not been yet processed and investiage false means that the pending balance does not appear suspicious.
    */
    const payoutData = await paymentBatches.find({server: server_no, batch: batch_no, investigate: false, processedTimeStamp: 0});

    const ttl = 28800;
    const gasLimit = 2320;
    const gasPrice = 0.0000010000;
    const finulabBank = process.env.finulab_bank;
    const operationsAdmin = process.env.operations_admin;
    const from = "finulab-bank", chainId = "2", gasStation = process.env.gas_station;
    const gasAccount = `k:${pact.crypto.restoreKeyPairFromSecretKey(gasStation).publicKey}`;
    const networkId = "mainnet01", chainwebSendURLpt1 = "https://api.chainweb.com/chainweb/0.0/mainnet01/chain/", chainwebSendURLpt2 = "/pact/api/v1/send";

    const envData = {
        "ks": {
            "keys": [
                pact.crypto.restoreKeyPairFromSecretKey(gasStation).publicKey
            ],
            "pred": "keys-all"
        },
        "finux-operations-admin": {
            "keys": [
                pact.crypto.restoreKeyPairFromSecretKey(operationsAdmin).publicKey
            ],
            "pred": "keys-all"
        },
        "finulab-bank": {
            "keys": [
                pact.crypto.restoreKeyPairFromSecretKey(finulabBank).publicKey
            ],
            "pred": "keys-all"
        }
    }

    const capabilityData = {
        publicKey: pact.crypto.restoreKeyPairFromSecretKey(gasStation).publicKey,
        secretKey: gasStation,
        clist: [
            {"name": "coin.GAS", "args": []},
        ]
    }
    const capabilityDataOne = {
        publicKey: pact.crypto.restoreKeyPairFromSecretKey(operationsAdmin).publicKey,
        secretKey: operationsAdmin
    }
    const generalCapabilityData = [capabilityData, capabilityDataOne];

    for(let i = 0; i < payoutData.length; i++) {
        const now = new Date();
        const creationTime = datefns.getUnixTime(now);

        const to = `k:${payoutData[i]["address"]}`;
        const amount = `${Number(formatPayoutBalance.format(payoutData[i]["pendingBalanceMorning"] + payoutData[i]["pendingBalanceEvening"]))}`;

        const regex = /\./g;
        const amountStringPreInterlude = amount;
        const amountStringInterlude = (amountStringPreInterlude.match(regex) || []).length;
        const amountString = amountStringInterlude === 1 ? amount : parseFloat(amount).toFixed(1);

        const envDataSpecific = {
            ...envData, 
            [to]: {
                "keys": [
                    to.slice(2, to.length)
                ],
                "pred": "keys-all"
            }
        }

        const capabilityDataTwo = {
            publicKey: pact.crypto.restoreKeyPairFromSecretKey(finulabBank).publicKey,
            secretKey: finulabBank,
            clist: [
                {"name": "free.finux.TRANSFER", "args": [from, to, Number(amount)]}
            ]
        }
        const generalCapabilityDataSpecific = [...generalCapabilityData, capabilityDataTwo];

        /*
            Use of the pact-lang-api package to create the individual cmd which is sent via axios.post().
        */
        const pactCmd = pact.simple.exec.createCommand(generalCapabilityDataSpecific, undefined,
            `(free.finux.transfer-create "${from}" "${to}" (read-keyset "${to}") ${amountString})`,
            envDataSpecific, pact.lang.mkMeta(gasAccount, chainId, gasPrice, gasLimit, creationTime, ttl), networkId
        );
        const chainwebResponse = await axios.post(`${chainwebSendURLpt1}${chainId}${chainwebSendURLpt2}`, pactCmd);
        
        if(chainwebResponse.status === 200) {
            if(chainwebResponse.data === undefined || chainwebResponse.data === null) {
                await paymentBatches.updateOne(
                    {username: payoutData[i]["username"], address: payoutData[i]["address"], server: payoutData[i]["server"], batch: payoutData[i]["batch"]}, 
                    {$set: {status: "error"}}
                );
            } else {
                const resKeys = Object.keys(chainwebResponse.data);
                if(resKeys.includes("requestKeys")) {
                    const newPaymentNotification = new notificationsDescs(
                        {
                            by: "finulab",
                            target: payoutData[i]["username"],
                            byProfileImage: "",
                            type: "payment",
                            message: `weekly-rewards: you received ${amount} finux tokens!`,
                            link: "/wallet",
                            read: false,
                            timeStamp: creationTime
                        }
                    );
                    await newPaymentNotification.save();
                    
                    await accountsWalletDescs.updateOne(
                        {username: payoutData[i]["username"], publicKey: to.slice(2, to.length)},
                        {$inc: {pendingBalanceMorning: -payoutData[i]["pendingBalanceMorning"], pendingBalanceEvening: -payoutData[i]["pendingBalanceEvening"]}}
                    );
    
                    await paymentBatches.updateOne(
                        {username: payoutData[i]["username"], address: payoutData[i]["address"], server: payoutData[i]["server"], batch: payoutData[i]["batch"]}, 
                        {$set: {processedTimeStamp: creationTime, requestKey: chainwebResponse.data["data"]["requestKeys"][0], status: "success-validate"}}
                    );
                } else {
                    await paymentBatches.updateOne(
                        {username: payoutData[i]["username"], address: payoutData[i]["address"], server: payoutData[i]["server"], batch: payoutData[i]["batch"]}, 
                        {$set: {status: "error"}}
                    );
                }
            }
        } else {
            await paymentBatches.updateOne(
                {username: payoutData[i]["username"], address: payoutData[i]["address"], server: payoutData[i]["server"], batch: payoutData[i]["batch"]}, 
                {$set: {status: "error"}}
            );
        }
    }

    return 0;
}

finalizeUserPayouts(server_no_use, batch_no_use).then(
    () => {process.exit();}
).catch(
    (error) => {console.log(error); process.exit();}
);