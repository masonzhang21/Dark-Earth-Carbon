const axios = require("axios");

const {initializeApp} = require("firebase-admin/app");
const {getFirestore} = require("firebase-admin/firestore");
const {onSchedule} = require("firebase-functions/v2/scheduler");
const functions = require("firebase-functions");

initializeApp();
const db = getFirestore();

// cache formulations
const formulations = {};
const getFormulation = async formulation => {
	if (!formulations[formulation]) {
		formulations[formulation] = (await db.collection("formulations").doc(formulation).get()).data();
	}
	return formulations[formulation];
};
/**
 * Calculates the supply and demand projections.
 * 
 * Logic: 
 *  Orders - supply of BCF: qty of all orders with delivery date < 30 && status not "Delivered/Cancelled" and customer "DEC"
 *  Orders - demand of BCF: qty - prodQty of all orders with delivery date < 30 and status is "Received || In Progress"
 *  Orders/Formulations - demand of ingredients: prodQty * formulation of all orders with delivery date < 45 (exception: nonActivated orders, which is < 30) and status "Received"
 *  Inputs - supply of ingredients/biomass: qty of all inputs with delivery date < 30 and status "Scheduled"
 *  Biochar Prod - supply of biochar: average of the past 14 days 
 *  Biochar Prod - demand of biomass: average of the past 14 days 
 * 
 * @param {string} prefixPath - The prefix path for the database operations.
 * @returns {Promise} - A promise that resolves when the supply and demand calculation is complete.
 */
async function calculateSupplyDemandProjections(prefixPath) {
	const getTable = table => db.collection(`${prefixPath}/${table}`);
	const inventoryItems = (await getTable("inventory").listDocuments()).map(doc => doc.id);
	const supply = inventoryItems.reduce((acc, id) => ({...acc, [id]: 0}), {});
	const demand = inventoryItems.reduce((acc, id) => ({...acc, [id]: 0}), {});

	const PROJECTION_RANGE = 30; // number of days we're forecasting
	const now = new Date();
    const nowMinus14 = new Date(now.getFullYear(), now.getMonth(), now.getDate() - 14); // for historical avg biochar supply and biomass demand
	const projectionEnd = new Date(now.getFullYear(), now.getMonth(), now.getDate() + PROJECTION_RANGE);
    const projectionEndPlus14 = new Date(projectionEnd.getFullYear(), projectionEnd.getMonth(), projectionEnd.getDate() + 14);

	// calculate bcf supply using orders
	const bcfSupplyOrders = await getTable("orders").where("deliveryDate", "<", projectionEnd).where("status", "not-in", ["Delivered", "Cancelled"]).get();
    for (const order of bcfSupplyOrders.docs) {
		const orderData = order.data();
        // manually check if customer is DEC, we can't do this in the query b/c it's a nested field
        if (orderData["customer"][0]["customerName"] === "DEC") {
            supply[orderData.formulation[0]["id"]] += orderData.quantity;
        }
	}
    // calculate bcf demand using orders
    const bcfDemandOrders = await getTable("orders").where("deliveryDate", "<", projectionEnd).where("status", "in", ["Received", "In Progress"]).get();
	for (const order of bcfDemandOrders.docs) {
		const orderData = order.data();
        demand[orderData.formulation[0]["id"]] += orderData.quantity - orderData.productionQuantity;
	}
	// calculate ingredient demand using orders
	const ingredientDemandOrders = await getTable("orders")
		.where("deliveryDate", "<", projectionEndPlus14)
		.where("status", "==", "Received")
		.get();
	for (const order of ingredientDemandOrders.docs) {
        const orderData = order.data();
        const deliveryDate = new Date(orderData.deliveryDate["_seconds"] * 1000);
		if (orderData.isActivated === false && deliveryDate > projectionEnd) {
			// since this order isn't activated and in the 30-44 day range, we don't need to consider it
            console.log("skipping order", order.id)
			continue;
		}
		const formulationData = await getFormulation(orderData.formulation[0]["id"]);
		for (const ingredient in formulationData) {
            // capitalize words
			demand[ingredient] += orderData.productionQuantity * formulationData[ingredient];
		}
	}
	// calculate input supplies using biochar-prod and inputs.
	const inputsSupply = await getTable("inputs").where("deliveryDate", "<", projectionEnd).where("status", "==", "Scheduled").get();
	for (const input of inputsSupply.docs) {
		const inputData = input.data();
        const qtyField = inputData["type"] === "Biomass" ? "quantityTons" : "quantityLiters"
        if (inputData[qtyField]) {
            // it's technically possible for the qtyField to be undefined since we can't require it
            supply[inputData["input"][0]["id"]] += inputData[qtyField];
        }
	}
	// average of past 14 days of biochar production and biomass use
	const biocharProdSupply = await getTable("biochar-prod").where("endDate", "<", now).where("endDate", ">", nowMinus14).get();    
	for (const productionLot of biocharProdSupply.docs) {
		const productionData = productionLot.data();
        supply["Biochar"] += productionData.quantityLiters / 14;
        if (productionData.feedstock && productionData.biomassQuantity) {
            // add to feedstock consumption 
            const feedstock = productionData.feedstock[0]["id"]
            if (productionData.biomassQuantity) {
                // check to make sure biomassQuantity is set because it is optional
                demand[feedstock] += +(productionData.biomassQuantity/14).toFixed(1)
            }
        }
	}
    supply["Biochar"] = Math.round(supply["Biochar"])

	return {supply, demand};
}

/**
 * WARNING: This function will become very expensive as time goes on, please use with caution and mainly for testing purposes.
 * 
 * Calculates the current inventory on hand based on various operations.
 * 
 * Logic: 
 * For each order that's in progress, complete, or delivered:
 *     debit its ingredient budget (prodQty * formulation) from inventory
 * For each order that's complete:
 *     debit stockBCF from inventory
 * For each order that's delivered to DEC: 
 *     credit quantity to inventory
 * For each input that's received: 
 *     credit quantity to inventory
 * For each biochar production:
 *     credit quantity to inventory
 * For each reconciliation:
 *     credit/debit quantity to inventory
 * 
 * @param {string} prefixPath - The prefix path for the database operations.
 * @returns {Promise} - A promise that resolves when the inventory calculation is complete.
 */
async function calculateInventoryOnHand(prefixPath) {
	const getTable = table => db.collection(`${prefixPath}/${table}`);

	const inventoryItems = (await getTable("inventory").listDocuments()).map(doc => doc.id);
	const inventory = inventoryItems.reduce((acc, id) => ({...acc, [id]: 0}), {});
	const orders = await getTable("orders").where("status", "in", ["In Progress", "Complete", "Delivered"]).get();
	for (const order of orders.docs) {
		const orderData = order.data();
		const formulationData = await getFormulation(orderData.formulation[0]["id"]);
		for (const ingredient in formulationData) {
			inventory[ingredient] -= orderData.productionQuantity * formulationData[ingredient];
		}
		if (orderData.status === "Complete") {
			const stockBCF = orderData.quantity - orderData.productionQuantity;
			inventory[orderData.formulation[0]["id"]] -= stockBCF;
		}
		if (orderData.status === "Delivered" && orderData.customer[0]["customerName"] === "DEC") {
			inventory[orderData.formulation[0]["id"]] += orderData.quantity;
		}
	}
	const inputs = await getTable("inputs").where("status", "==", "Obtained").get();
	for (const input of inputs.docs) {
		const inputData = input.data();
        const qtyField = inputData["type"] === "Biomass" ? "quantityTons" : "quantityLiters"
		inventory[inputData["input"][0]["id"]] += inputData[qtyField];
	}
	const biocharProd = await getTable("biochar-prod").get();
	for (const productionLot of biocharProd.docs) {
		const productionData = productionLot.data();
		inventory["Biochar"] += productionData.quantityLiters;
	}
	const reconciliations = await getTable("reconciliations").get();
	for (const reconciliation of reconciliations.docs) {
		const reconciliationData = reconciliation.data();
		inventory[reconciliationData["item"][0]["id"]] += reconciliationData.quantity;
	}
    return inventory;
}

/**
 * Sends a notification to a Microsoft Teams user.
 * 
 * @param {string} userEmail - The email address of the recipient.
 * @param {string} title - The title of the notification.
 * @param {string} message - The message body of the notification.
 */function sendTeamsNotification(userEmail, title, message) {
    const getNameFromEmail = email => {
        const name = email.split("@")[0];
        return name.charAt(0).toUpperCase() + name.slice(1).toLowerCase();
    };
    console.log("Sending Teams Notification:", title, message)

    const WEBHOOK_URL =
        "https://upendoagri.webhook.office.com/webhookb2/26ae62c6-c8c1-4cea-97a9-9443d37cbe42@846f11ef-840f-4f58-8b7b-13c8fda667b0/IncomingWebhook/49e8d86852854e358c10b178f03b75a8/c9b0ad22-9635-4d18-b253-c14e521fde41";
    const USER_MENTION_MARKER = "<at>tag</at>";

    let textPayload = `${USER_MENTION_MARKER} \n\n ${message}`;
    let formattedCardPayload = {
        type: "message",
        attachments: [
            {
                contentType: "application/vnd.microsoft.card.adaptive",
                content: {
                    type: "AdaptiveCard",
                    body: [
                        {
                            type: "TextBlock",
                            size: "Medium",
                            weight: "Bolder",
                            text: title,
                        },
                        {
                            type: "TextBlock",
                            text: textPayload,
                            wrap: true,
                        },
                    ],
                    $schema: "http://adaptivecards.io/schemas/adaptive-card.json",
                    version: "1.0",
                    msteams: {
                        entities: [
                            {
                                type: "mention",
                                text: USER_MENTION_MARKER,
                                mentioned: {
                                    id: `${userEmail}`,
                                    name: getNameFromEmail(userEmail),
                                },
                            },
                        ],
                    },
                },
            },
        ],
    };
    axios
        .post(WEBHOOK_URL, formattedCardPayload)
        .then(res => {
            console.log(`statusCode: ${res.status}`);
        })
        .catch(error => {
            console.error(error);
        });
}
/**
 * Schedules warnings for orders based on their delivery and activation dates and current phase.
 * 
 * Makes a Teams post if: 
 *     - an order is within 16 days of delivery and needs to be activated and is in "Received" phase, 
 *     - an order is within 2 days of delivery and does not need to be activated and is in "Received" phase
 *     - an order is within 2 days of delivery and is in "In Progress" phase
 *     - an order's activationDate was 15 days ago and is in "In Progress" phase
 * 
 * @param {string} collectionPathPrefix - The prefix path for the database operations.
 */
function scheduleWarnings(collectionPathPrefix) {
    const MS_IN_DAY = 24 * 60 * 60 * 1000;
    // returns the number of days (rounded down) between two dates
    const getDayDelta = (date1, date2) => {
        const diffTime = date2 - date1;
        const diffDays = Math.round(diffTime / MS_IN_DAY);
        return diffDays;
    };
    const now = new Date();
    const date2 = new Date(now.getTime() + 2 * MS_IN_DAY);
    const date15 = new Date(now.getTime() - 15 * MS_IN_DAY);
    const date16 = new Date(now.getTime() + 16 * MS_IN_DAY);

    db.collection(`${collectionPathPrefix}/orders`).where("isActivated", "==", true).where("deliveryDate", "<", date16).where("status", "==", "Received").get().then(orders => {
        for (const order of orders.docs) {
            const orderData = order.data();
            const title = `Order Needs To Begin Activation Now`;
            const deliveryDate = new Date(orderData.deliveryDate["_seconds"] * 1000)
            const message = `Order ${orderData.orderNumber} is due in ${getDayDelta(now, deliveryDate)} days on ${deliveryDate.toLocaleDateString('en-US', { month: 'long', day: 'numeric' })} and still needs to be activated.`
            sendTeamsNotification(orderData.assignee, title, message);
        }
    })
    db.collection(`${collectionPathPrefix}/orders`).where("isActivated", "==", false).where("deliveryDate", "<", date2).where("status", "==", "Received").get().then(orders => {
        for (const order of orders.docs) {
            const orderData = order.data();
            const title = `Order Completion Reminder`;
            const deliveryDate = new Date(orderData.deliveryDate["_seconds"] * 1000)
            const message = `Order ${orderData.orderNumber} is due in ${getDayDelta(now, deliveryDate)} days on ${deliveryDate.toLocaleDateString('en-US', { month: 'long', day: 'numeric' })} and its status is still _Received_.`
            sendTeamsNotification(orderData.assignee, title, message);
        }
    })
    db.collection(`${collectionPathPrefix}/orders`).where("isActivated", "==", true).where("activationDate", "<", date15).where("status", "==", "In Progress").get().then(orders => {
        for (const order of orders.docs) {
            const orderData = order.data();
            const title = `Order Has Finished Activating`;
            const activationDate = new Date(orderData.activationDate["_seconds"] * 1000)
            const message = `Order ${orderData.orderNumber} was activated ${getDayDelta(activationDate, now)} days ago on ${activationDate.toLocaleDateString('en-US', { month: 'long', day: 'numeric' })}.`
            sendTeamsNotification(orderData.assignee, title, message);
        }
    })
    db.collection(`${collectionPathPrefix}/orders`).where("deliveryDate", "<", date2).where("status", "==", "In Progress").get().then(orders => {
        for (const order of orders.docs) {
            const orderData = order.data();
            const title = `Order Delivery Date Approaching`;
            const deliveryDate = new Date(orderData.deliveryDate["_seconds"] * 1000)
            const message = `Order ${orderData.orderNumber} needs to be delivered in ${getDayDelta(now, deliveryDate)} days on ${deliveryDate.toLocaleDateString('en-US', { month: 'long', day: 'numeric' })} and is still _In Progress_.`
            sendTeamsNotification(orderData.assignee, title, message);
        }
    })
}

const schedule = {
    schedule: "every day 06:00",
    timeZone: "Africa/Nairobi"
}

exports.dataHousekeeping = onSchedule(schedule, async event => {
	const sites = (await db.collection("sites").listDocuments()).map(doc => doc.id);
	for (const site of sites) {
        console.log("Housekeeping for", site)
		const collectionPathPrefix = `sites/${site}`;
		calculateSupplyDemandProjections(collectionPathPrefix).then(({supply, demand}) => {
            console.log(supply, demand)
			const inventoryRef = db.collection(`${collectionPathPrefix}/inventory`);
			// supply and demand have same keys, for loop that gets both vals per key
			for (const item in supply) {
                // update supply and demand forecasts in inventory
				inventoryRef.doc(item).update({supplyForecast: supply[item] || 0, demandForecast: demand[item] || 0});
			}
		});
        scheduleWarnings(collectionPathPrefix)
	}
});

/*
Use firebase functions:shell to test, enter testSupplyDemandCalculations({data: ""})
*/
exports.testSupplyDemandCalculations = functions.https.onCall(async (data, context) => {
    const collectionPathPrefix = `test/mock-site`;
    calculateSupplyDemandProjections(collectionPathPrefix).then(({supply, demand}) => {
        console.log(supply)
        console.log(demand)
    });
})
exports.testWarnings = functions.https.onCall(async (data, context) => {
    const collectionPathPrefix = `test/mock-site`
    scheduleWarnings(collectionPathPrefix)
})
exports.testInventoryOnHand = functions.https.onCall(async (data, context) => {
    const inventory = await calculateInventoryOnHand("test/mock-site")
    console.log(inventory)
})