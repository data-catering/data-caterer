/*
have a list of scenarios that users can view
data generation scenarios:
- financial accounts and transactions
- e-commerce site orders
- customer on-boarding (application, status)

data validation scenarios:
- field validation
- relationship validation
- upstream validation

data cleanup scenario:
- delete generated data
- delete downstream data
 */

let rawTxnContainer = document.getElementById("raw-transaction-container");
let allTransactionCardContainer = document.getElementById("transaction-card-container");
let validationContainer = document.getElementById("validation-container");

let generateButton = document.getElementById("generate-button");
let validateButton = document.getElementById("validate-button");
let cleanupButton = document.getElementById("cleanup-button");

let isGenerateData = true;
let txnId = 1;

async function createTransactionCards() {
    let max = 20;
    let startDate = new Date(2020, 1, 1).getTime();

    while (rawTxnContainer.childElementCount <= max && isGenerateData) {
        if (rawTxnContainer.childElementCount > 2) {
            let randomWait = Math.floor(Math.random() * 1000 + 500);
            await wait(randomWait);
        }
        let txnCard = document.createElement("div");
        txnCard.classList.add("card");
        txnCard.setAttribute("id", `txn${txnId}`);
        let txnContainer = document.createElement("div");
        txnContainer.classList.add("card-container");
        let index = Math.floor(Math.random() * transactionTypes.length);
        let currTxn = transactionTypes[index];

        let fullCurrDate = addDays(startDate, Math.floor(Math.random() * 20)).toISOString();
        let currDate = fullCurrDate.split('T')[0];
        let currAmount = Math.round(Math.random() * 20000) / 100;

        let randDescVal = Math.random();
        let rawDescription = currTxn.description;
        if (randDescVal < 0.3) {
            rawDescription = `${currTxn.description} ${Math.floor(Math.random() * 90000) + 10000}`;
        } else if (randDescVal < 0.5) {
            rawDescription = `${(Math.random() + 1).toString(36).substring(6)} ${currTxn.description}`;
        }
        let rawTxnText = `{"date":"${fullCurrDate}","desc":"${rawDescription}","amount":"${String(currAmount)}"}\n`;

        let date = document.createElement("p");
        date.classList.add("date-text", "card-text");
        date.innerText = currDate;
        let description = document.createElement("h4");
        description.classList.add("description-text", "card-text");
        description.innerText = Math.random() < 0.1 ? rawDescription : currTxn.description;
        let category = document.createElement("p");
        category.classList.add("category-text", "card-text");
        category.innerText = Math.random() < 0.1 ? "" : currTxn.category;
        let amount = document.createElement("p");
        amount.classList.add("amount-text", "card-text");
        amount.innerText = `$${String(currAmount)}`;

        let rawTxnEntry = document.createElement("code");
        rawTxnEntry.innerText = rawTxnText;
        rawTxnContainer.append(rawTxnEntry);
        rawTxnContainer.scrollTop = rawTxnContainer.scrollHeight;

        txnContainer.append(date, description, category, amount);
        txnCard.append(txnContainer);
        await wait(200);
        allTransactionCardContainer.append(txnCard);
        allTransactionCardContainer.scrollTop = allTransactionCardContainer.scrollHeight;
        startDate = currDate;
        txnId += 1;
    }
}

async function validateTransactions() {
    // foreach record generated, go one by one through each and validate with transition
    isGenerateData = false;
    if (validationContainer.style.display === "none") {
        validationContainer.style.display = "block";
    }
    let currentValidations = Array.from(validationContainer.children);
    let txnCards = Array.from(allTransactionCardContainer.children).reverse();
    for (const txnCard of txnCards) {
        txnCard.style.backgroundColor = "";
        if (txnCard.childElementCount > 1) {
            txnCard.removeChild(txnCard.lastChild);
        }
    }
    await wait(2000);
    txnCards = Array.from(allTransactionCardContainer.children).reverse();

    for (const txnCard of txnCards) {
        await wait(1000);
        let txnDetails = txnCard.firstElementChild;

        let validationResult = true;
        let failedSummary = document.createElement("ol");
        failedSummary.style.display = "none";
        let failedSummaryText = document.createElement("a");
        failedSummaryText.classList.add("fail-reason-button");
        failedSummaryText.innerText = "Reason >";
        failedSummaryText.addEventListener("click", function () {
            if (failedSummary.style.display === "none") {
                failedSummary.style.display = "contents";
            } else {
                failedSummary.style.display = "none";
            }
        });

        currentValidations.forEach(validation => {
            let field = validation.getAttribute("field");
            let fieldValue = txnDetails.getElementsByClassName(`${field}-text`).item(0).innerText;
            let validationType = validation.getAttribute("validationType");

            if (validationType === "isNullable") {
                let nullCheck = validation.getElementsByTagName("select").item(0).value;
                if (nullCheck === "Not Null") {
                    let res = fieldValue !== "";
                    validationResult &= res;
                    if (!res) failedSummary.append(listOption(`${field} null`));
                } else {
                    let res = fieldValue === "";
                    validationResult &= res;
                    if (!res) failedSummary.append(listOption(`${field} not null`));
                }
            } else if (validationType === "In") {
                let possibleValues = validation.getElementsByTagName("input").item(0).value.split(",").map(x => x.trim());
                let res = possibleValues.includes(fieldValue);
                validationResult &= res;
                if (!res) failedSummary.append(listOption(`${field} not in ${validation.getElementsByTagName("input").item(0).value}`));
            } else if (validationType === "Matches") {
                let res = new RegExp(validation.getElementsByTagName("input").item(0).value).test(fieldValue);
                validationResult &= res;
                if (!res) failedSummary.append(listOption(`${field} not matches ${validation.getElementsByTagName("input").item(0).value}`));
            } else if (validationType === "Less Than") {
                let res = Number(fieldValue.substring(1)) < Number(validation.getElementsByTagName("input").item(0).value);
                validationResult &= res;
                if (!res) failedSummary.append(listOption(`${field} not less than ${validation.getElementsByTagName("input").item(0).value}`));
            }
        });
        txnCard.style.backgroundColor = validationResult ? "hsl(120, 40%, 45%)" : "hsl(2, 40%, 45%)";
        if (failedSummary.childElementCount > 0) {
            failedSummaryText.append(failedSummary);
            txnCard.append(failedSummaryText);
        }
    }
}

function createValidationRules() {
    validationContainer.style.display = "none";
    // have a list of 4 validations rules that users could alter
    let validationRules = [
        {"field": "description", "validation": {"type": "text", "title": "Matches", "default": "^[A-Z][a-z]+$"}},
        {
            "field": "category",
            "validation": {"type": "text", "title": "In", "default": "Financial,Food,Travel,Transport,Technology"}
        },
        {"field": "amount", "validation": {"type": "number", "title": "Less Than", "default": "175"}},
    ];

    validationRules.forEach(validationRule => {
        let validationRuleContainer = document.createElement("div");
        validationRuleContainer.classList.add("validation-rule-container");

        let validationField = document.createElement("code");
        validationField.innerText = validationRule.field;
        validationRuleContainer.setAttribute("field", validationRule.field);

        if (validationRule.validation.type === "select") {
            let validationSelect = document.createElement("select");
            validationRule.validation.options.forEach(opt => {
                let option = document.createElement("option");
                option.value = opt;
                option.innerText = opt;
                validationSelect.append(option);
            });
            validationField.style.paddingRight = "3px";
            validationRuleContainer.setAttribute("validationType", "isNullable");
            validationRuleContainer.append(validationField, validationSelect);
        } else if (validationRule.validation.type === "text" || validationRule.validation.type === "number") {
            let validationTitle = document.createElement("p");
            validationTitle.innerHTML = validationField.outerHTML + ` ${validationRule.validation.title}: `;
            validationRuleContainer.setAttribute("validationType", validationRule.validation.title);

            let textBox = document.createElement("input");
            textBox.classList.add("md-input");
            textBox.type = validationRule.validation.type;
            textBox.value = validationRule.validation.default;
            textBox.innerText = validationRule.validation.default;
            textBox.style.marginLeft = "3px";

            validationRuleContainer.append(validationTitle, textBox);
        }

        validationContainer.append(validationRuleContainer);
    });
}

async function cleanupTransactions() {
    // go through each record and remove it
    isGenerateData = false;
    await wait(2000);
    let txnCards = Array.from(allTransactionCardContainer.children).reverse();

    for (const txnCard of txnCards) {
        await wait(500);
        allTransactionCardContainer.removeChild(txnCard);
        rawTxnContainer.removeChild(rawTxnContainer.lastChild);
    }
}

function tableData(value) {
    let element = document.createElement("td");
    element.innerText = value;
    return element;
}

function getNextColour(index) {
    // change hue from 0 to 360 by steps of 31, saturation=45%, lightness=75%
    let hue = index * 39 % 360;
    let saturation = 30;
    let lightness = 85;
    return `hsl(${hue} ${saturation}% ${lightness}%)`;
}

function addDays(date, days) {
    var result = new Date(date);
    result.setDate(result.getDate() + days);
    return result;
}

function listOption(text) {
    let option = document.createElement("li");
    option.innerText = text;
    return option;
}

const wait = function (ms = 1500) {
    return new Promise(resolve => {
        setTimeout(resolve, ms);
    });
};

let transactionTypes = [
    {"description": "ATM Withdrawal", "category": "Financial", "logo": ""},
    {"description": "Salary", "category": "Financial", "logo": ""},
    {"description": "7-Eleven", "category": "Food", "logo": ""},
    {"description": "Costco", "category": "Food", "logo": ""},
    {"description": "McDonald's", "category": "Food", "logo": ""},
    {"description": "Sushi Place", "category": "Food", "logo": ""},
    {"description": "Subway", "category": "Food", "logo": ""},
    {"description": "Bus Fare", "category": "Transport", "logo": ""},
    {"description": "Uber", "category": "Transport", "logo": ""},
    {"description": "Gucci", "category": "Clothes", "logo": ""},
    {"description": "H&M", "category": "Clothes", "logo": ""},
    {"description": "Agoda", "category": "Travel", "logo": ""},
    {"description": "Hilton Hotels", "category": "Travel", "logo": ""},
    {"description": "Qantas", "category": "Travel", "logo": ""},
    {"description": "Amazon", "category": "Technology", "logo": ""},
    {"description": "Google", "category": "Technology", "logo": ""},
    {"description": "Microsoft", "category": "Technology", "logo": ""},
    {"description": "Steam", "category": "Games", "logo": ""},
];

generateButton.addEventListener("click", async function () {
    isGenerateData = true;
    await createTransactionCards();
});
validateButton.addEventListener("click", await validateTransactions());
cleanupButton.addEventListener("click", await cleanupTransactions());

createValidationRules();
await createTransactionCards();
