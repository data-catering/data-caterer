// window.onerror = function myErrorHandler(errorMsg, url, lineNumber) {
//     console.log(errorMsg);
//     return false;
// }
// window.addEventListener("error", function (e) {
//     e.preventDefault();
//     console.log(e);
//     return false;
// });
// window.addEventListener("unhandledrejection", function (e) {
//     e.preventDefault();
//     console.log(e);
// });

import {camelize, createAccordionItem} from "./shared.js";

const baseDataTypes = ["string", "integer", "long", "short", "decimal", "double", "float", "date", "timestamp", "binary", "array", "struct"];
const dataTypeOptionsMap = new Map();
const defaultDataTypeOptions = {
    enableEdgeCases: {default: "false", type: "text", choice: ["true", "false"]},
    edgeCaseProbability: {default: 0.0, type: "number", min: 0.0, max: 1.0},
    isUnique: {default: "false", type: "text", choice: ["true", "false"]},
    seed: {default: -1, type: "number", min: -1, max: 9223372036854775807},
    sql: {default: "", type: "text"},
    oneOf: {default: [], type: "text"}
};

function getNumberOptions(min, max) {
    let minMaxOpt = min && max ? {min: min, max: max} : {};
    return {
        min: {default: 0, type: "number", ...minMaxOpt},
        max: {default: 1000, type: "number", ...minMaxOpt},
        stddev: {default: 1.0, type: "number", min: 0.0, max: 100000000.0},
        mean: {default: 500, type: "number", ...minMaxOpt}
    };
}

dataTypeOptionsMap.set("string", {
    ...defaultDataTypeOptions,
    minLen: {default: 1, type: "number", min: 0, max: 1000},
    maxLen: {default: 10, type: "number", min: 0, max: 1000},
    expression: {default: "", type: "text"},
    enableNull: {default: "false", type: "text", choice: ["true", "false"]},
    nullProbability: {default: 0.0, type: "number", min: 0.0, max: 1.0},
    regex: {default: "", type: "text"}
});
dataTypeOptionsMap.set("integer", {...defaultDataTypeOptions, ...getNumberOptions(-2147483648, 2147483647)});
dataTypeOptionsMap.set("long", {...defaultDataTypeOptions, ...getNumberOptions(-9223372036854775808, 9223372036854775807)});
dataTypeOptionsMap.set("short", {...defaultDataTypeOptions, ...getNumberOptions(-32768, 32767)});
dataTypeOptionsMap.set("decimal", {
    ...defaultDataTypeOptions,
    ...getNumberOptions(),
    numericPrecision: {default: 10, type: "number", min: 0, max: 2147483647},
    numericScale: {default: 0, type: "number", min: 0, max: 2147483647}
});
dataTypeOptionsMap.set("double", {...defaultDataTypeOptions, ...getNumberOptions()});
dataTypeOptionsMap.set("float", {...defaultDataTypeOptions, ...getNumberOptions()});
dataTypeOptionsMap.set("date", {
    ...defaultDataTypeOptions,
    min: {default: formatDate(true), type: "date", min: "0001-01-01", max: "9999-12-31"},
    max: {default: formatDate(false), type: "date", min: "0001-01-01", max: "9999-12-31"}
});
dataTypeOptionsMap.set("timestamp", {
    ...defaultDataTypeOptions,
    min: {
        default: formatDate(true, true),
        type: "datetime-local",
        min: "0001-01-01 00:00:00",
        max: "9999-12-31 23:59:59"
    },
    max: {
        default: formatDate(false, true),
        type: "datetime-local",
        min: "0001-01-01 00:00:00",
        max: "9999-12-31 23:59:59"
    }
});
dataTypeOptionsMap.set("binary", {
    ...defaultDataTypeOptions,
    minLen: {default: 1, type: "number", min: 0, max: 2147483647},
    maxLen: {default: 20, type: "number", min: 0, max: 2147483647},
});
dataTypeOptionsMap.set("array", {
    ...defaultDataTypeOptions,
    arrayMinLen: {default: 0, type: "number", min: 0, max: 2147483647},
    arrayMaxLen: {default: 5, type: "number", min: 0, max: 2147483647},
    arrayType: {default: "string", type: "text", choice: baseDataTypes}
});
dataTypeOptionsMap.set("struct", {...defaultDataTypeOptions});

const validationTypeOptionsMap = new Map();
const defaultValidationOptions = {
    description: {default: "", type: "text"},
    errorThreshold: {default: 0.0, type: "number", min: 0.0},
}
validationTypeOptionsMap.set("column", {
    ...defaultValidationOptions,
    defaultChildColumn: {default: "", type: "text", required: ""},
    equal: {default: "", type: "text"},
    notEqual: {default: "", type: "text"},
    null: {default: "isNull", type: "badge"},
    notNull: {default: "isNotNull", type: "badge"},
    contains: {default: "", type: "text"},
    notContains: {default: "", type: "text"},
    unique: {default: "", type: "text"},
    lessThan: {default: "", type: "text"},
    lessThanOrEqual: {default: "", type: "text"},
    greaterThan: {default: "", type: "text"},
    greaterThanOrEqual: {default: "", type: "text"},
    between: {default: "", type: "min-max"},
    notBetween: {default: "", type: "min-max"},
    in: {default: "", type: "text"},    // provide as list?
    matches: {default: "", type: "text"},
    notMatches: {default: "", type: "text"},
    startsWith: {default: "", type: "text"},
    notStartsWith: {default: "", type: "text"},
    endsWith: {default: "", type: "text"},
    notEndsWith: {default: "", type: "text"},
    size: {default: 0, type: "number"},
    notSize: {default: 0, type: "number"},
    lessThanSize: {default: 0, type: "number"},
    lessThanOrEqualSize: {default: 0, type: "number"},
    greaterThanSize: {default: 0, type: "number"},
    greaterThanOrEqualSize: {default: 0, type: "number"},
    luhnCheck: {default: "luhnCheck", type: "badge"},
    hasType: {default: "string", type: "text", choice: baseDataTypes},
    sql: {default: "", type: "text"},
});
validationTypeOptionsMap.set("groupBy", {
    ...defaultValidationOptions,
    defaultChildGroupByColumns: {default: "", type: "text", required: ""},
    count: {default: "", type: "text"},
    sum: {default: "", type: "text"},
    min: {default: "", type: "text"},
    max: {default: "", type: "text"},
    average: {default: "", type: "text"},
    standardDeviation: {default: "", type: "text"},
});
validationTypeOptionsMap.set("upstream", {
    ...defaultValidationOptions,
    defaultChildUpstreamTaskName: {default: "", type: "text", required: ""},
    joinColumns: {default: "", type: "text"},
    joinType: {
        default: "outer",
        type: "text",
        choice: ["inner", "outer", "left_outer", "right_outer", "left_semi", "anti", "cross"]
    },
    joinExpr: {default: "", type: "text"}
});
validationTypeOptionsMap.set("columnNames", {
    ...defaultValidationOptions,
    countEqual: {default: 0, type: "number"},
    countBetween: {default: 0, type: "min-max"},
    matchOrder: {default: "", type: "text"},
    matchSet: {default: "", type: "text"},
});


const toastPosition = document.getElementById("toast-container");
const addTaskButton = document.getElementById("add-task-button");
const tasksDiv = document.getElementById("tasks-details-body");
let numDataSources = 1;
let numFields = 0;
let numValidations = 0;
let numAddAttributeButton = 0;

tasksDiv.append(await createDataSourceForPlan(numDataSources));
addTaskButton.addEventListener("click", async function () {
    numDataSources += 1;
    let divider = document.createElement("hr");
    let newDataSource = await createDataSourceForPlan(numDataSources);
    tasksDiv.append(divider, newDataSource);
});

//create row with data source name and checkbox elements for generation and validation
async function createDataSourceForPlan(index) {
    let dataSourceRow = document.createElement("div");
    dataSourceRow.setAttribute("class", "row mb-3");
    let dataSourceConfig = await createDataSourceConfiguration(index);
    dataSourceRow.append(dataSourceConfig);
    return dataSourceRow;
}

function createDataConfigElement(index, name) {
    const nameCapitalize = name.charAt(0).toUpperCase() + name.slice(1);
    let dataConfigContainer = document.createElement("div");
    dataConfigContainer.setAttribute("id", `data-source-${name}-config-container`);
    dataConfigContainer.setAttribute("class", "mt-1");
    // let dataConfigHeader = document.createElement("h4");
    // dataConfigHeader.innerText = nameCapitalize;
    // dataConfigContainer.append(dataConfigHeader);

    let checkboxOptions = ["auto", "manual"];
    for (let checkboxOption of checkboxOptions) {
        let formCheck = document.createElement("div");
        formCheck.setAttribute("class", "form-check");
        let checkboxInput = document.createElement("input");
        let checkboxId = `${checkboxOption}-${name}-checkbox`;
        checkboxInput.setAttribute("class", "form-check-input");
        checkboxInput.setAttribute("type", "checkbox");
        checkboxInput.setAttribute("value", checkboxOption);
        checkboxInput.setAttribute("name", `data-${name}-conf-${index}`);
        checkboxInput.setAttribute("id", checkboxId);

        let label = document.createElement("label");
        label.setAttribute("class", "form-check-label");
        label.setAttribute("for", checkboxId);
        label.innerText = checkboxOption.charAt(0).toUpperCase() + checkboxOption.slice(1);

        formCheck.append(checkboxInput, label);
        dataConfigContainer.append(formCheck);
        addDataConfigCheckboxListener(checkboxInput, name);
    }
    return createAccordionItem(`${index}-${name}`, nameCapitalize, "", dataConfigContainer);
}

function addDataConfigCheckboxListener(element, name) {
    let configContainer = element.parentElement.parentElement;
    if (element.getAttribute("value") === "manual") {
        element.addEventListener("change", (event) => {
            manualCheckboxListenerDisplay(event, configContainer, name);
        });
    }
}

function manualCheckboxListenerDisplay(event, configContainer, name) {
    let querySelector = name === "generation" ? "#data-source-schema-container" : "#data-source-validation-container";
    let schemaContainer = configContainer.querySelector(querySelector);
    if (event.currentTarget.checked) {
        if (schemaContainer === null) {
            let newElement = name === "generation" ? createManualSchema() : createManualValidation();
            configContainer.append(newElement);
        } else {
            schemaContainer.style.display = "inherit";
        }
    } else {
        if (schemaContainer !== null) {
            schemaContainer.style.display = "none";
        }
    }
}

async function createDataConnectionInput(index) {
    let baseTaskDiv = document.createElement("div");
    baseTaskDiv.setAttribute("class", "row");
    let taskNameInput = document.createElement("input");
    taskNameInput.setAttribute("id", "task-name-" + index);
    taskNameInput.setAttribute("aria-label", "Task name");
    taskNameInput.setAttribute("type", "text");
    taskNameInput.setAttribute("placeholder", "task-" + index);
    taskNameInput.setAttribute("class", "form-control input-field task-name-field");
    taskNameInput.setAttribute("required", "");
    taskNameInput.value = "task-" + index;
    let taskNameFormFloating = createFormFloating("Task name", taskNameInput);

    let dataConnectionSelect = document.createElement("select");
    dataConnectionSelect.setAttribute("id", "data-source-connection-" + index);
    dataConnectionSelect.setAttribute("aria-label", "Data source");
    dataConnectionSelect.setAttribute("placeholder", "");
    dataConnectionSelect.setAttribute("class", "form-control input-field data-connection-name");
    let dataConnectionFormFloating = createFormFloating("Data source", dataConnectionSelect);
    baseTaskDiv.append(taskNameFormFloating, dataConnectionFormFloating);

    //get list of existing data connections
    await fetch("http://localhost:9090/connection/saved", {
        method: "GET"
    })
        .then(r => r.json())
        .then(respJson => {
            let connections = respJson.connections;
            for (let connection of connections) {
                let option = document.createElement("option");
                option.setAttribute("value", connection.name);
                option.innerText = connection.name;
                dataConnectionSelect.append(option);
            }
        });

    // if list of connections is empty, provide button to add new connection
    if (dataConnectionSelect.hasChildNodes()) {
        return baseTaskDiv;
    } else {
        let createNewConnection = document.createElement("a");
        createNewConnection.setAttribute("type", "button");
        createNewConnection.setAttribute("class", "btn btn-secondary");
        createNewConnection.setAttribute("href", "/connection");
        createNewConnection.innerText = "Create new connection";
        return createNewConnection;
    }
}

/*
Will contain:
- Data generation: auto, manual
    - Record count: total, per column, generated
- Validation: auto, manual
 */
async function createDataSourceConfiguration(index) {
    let divContainer = document.createElement("div");
    divContainer.setAttribute("id", "data-source-config-container-" + index);
    divContainer.setAttribute("class", "data-source-config-container");
    let dataConnectionFormFloating = await createDataConnectionInput(index);
    let dataConfigAccordion = document.createElement("div");
    dataConfigAccordion.setAttribute("class", "accordion mt-2");
    let dataGenConfigContainer = createDataConfigElement(index, "generation");
    let dataValidConfigContainer = createDataConfigElement(index, "validation");
    dataConfigAccordion.append(dataGenConfigContainer, dataValidConfigContainer);
    divContainer.append(dataConnectionFormFloating, dataConfigAccordion);
    return divContainer;
}

function createSchemaField(index) {
    let fieldContainer = document.createElement("div");
    fieldContainer.setAttribute("class", "row g-1 mb-2 data-field-container");
    fieldContainer.setAttribute("id", "data-field-container-" + index);

    let fieldName = document.createElement("input");
    fieldName.setAttribute("id", "field-name-" + index);
    fieldName.setAttribute("aria-label", "Name");
    fieldName.setAttribute("type", "text");
    fieldName.setAttribute("placeholder", "");
    fieldName.setAttribute("class", "form-control input-field data-source-field");
    fieldName.setAttribute("required", "");
    let formFloatingName = createFormFloating("Name", fieldName);

    let fieldTypeSelect = document.createElement("select");
    fieldTypeSelect.setAttribute("id", "field-type-" + index);
    fieldTypeSelect.setAttribute("aria-label", "Type");
    fieldTypeSelect.setAttribute("class", "form-select input-field data-source-field field-type");
    let formFloatingType = createFormFloating("Type", fieldTypeSelect);

    for (const key of dataTypeOptionsMap.keys()) {
        let selectOption = document.createElement("option");
        selectOption.setAttribute("value", key);
        if (key === "string") {
            selectOption.setAttribute("selected", "selected");
        }
        selectOption.innerText = key;
        fieldTypeSelect.append(selectOption);
    }

    fieldContainer.append(formFloatingName, formFloatingType);
    createDataTypeAttributes(fieldContainer, "data-type");
    return fieldContainer;
}

function createAttributeFormFloating(attrMetadata, attributeContainerId, inputClass, attribute) {
    let inputAttr;
    if (attrMetadata.choice) {
        inputAttr = document.createElement("select");
        inputAttr.setAttribute("id", attributeContainerId);
        inputAttr.setAttribute("class", "form-select input-field user-added-attribute " + inputClass);
        for (let choice of attrMetadata.choice) {
            let option = document.createElement("option");
            option.setAttribute("value", choice.toString());
            option.innerText = choice.toString();
            if (choice === attrMetadata["default"]) {
                option.setAttribute("selected", "");
            }
            inputAttr.append(option);
        }
    } else if (attrMetadata["type"] === "badge") {
        return createBadge(attribute);
    } else {
        inputAttr = document.createElement("input");
        inputAttr.setAttribute("id", attributeContainerId);
        inputAttr.setAttribute("type", attrMetadata["type"]);
        inputAttr.setAttribute("aria-label", attribute);
        inputAttr.setAttribute("class", "form-control input-field user-added-attribute " + inputClass);
        inputAttr.setAttribute("required", "");
        inputAttr.value = attrMetadata["default"];
        inputAttr.setAttribute("placeholder", attrMetadata["default"]);
    }

    for (const [key, value] of Object.entries(attrMetadata)) {
        if (key !== "default" && key !== "type" && key !== "choice") {
            inputAttr.setAttribute(key, value);
        }
    }

    return createFormFloating(attribute, inputAttr);
}

// Create a button for overriding attributes of the data field based on data type, i.e. set min to 10 for integer
function createDataTypeAttributes(element, elementType) {
    let elementQuerySelector = ".field-type";
    let menuAttributeName = "current-data-type";
    let inputClass = "data-source-field";
    let optionsMap = dataTypeOptionsMap;
    if (elementType === "validation-type") {
        elementQuerySelector = ".validation-type";
        menuAttributeName = "current-validation-type";
        inputClass = "data-validation-field";
        optionsMap = validationTypeOptionsMap;
    }

    //maybe on hover, show defaults
    let containerId = element.getAttribute("id");
    numAddAttributeButton += 1;
    let buttonWithMenuDiv = document.createElement("div");
    buttonWithMenuDiv.setAttribute("class", "col dropdown");
    let addAttributeButton = document.createElement("button");
    let addAttributeId = element.getAttribute("id") + "-add-attribute-button";
    addAttributeButton.setAttribute("id", addAttributeId);
    addAttributeButton.setAttribute("type", "button");
    addAttributeButton.setAttribute("class", "btn btn-secondary dropdown-toggle");
    addAttributeButton.setAttribute("data-bs-toggle", "dropdown");
    addAttributeButton.setAttribute("aria-expanded", "false");
    let addIcon = document.createElement("i");
    addIcon.setAttribute("class", "fa fa-plus");
    addAttributeButton.append(addIcon);
    buttonWithMenuDiv.append(addAttributeButton);

    // menu of attribute based on data type
    let menu = document.createElement("ul");
    menu.setAttribute("class", "dropdown-menu");
    menu.setAttribute("aria-labelledby", addAttributeId);
    menu.setAttribute("id", element.getAttribute("id") + "-add-attribute-menu");
    buttonWithMenuDiv.append(menu);
    element.append(buttonWithMenuDiv);
    // element that decides what attributes are available in the menu
    let menuDeciderElement = element.querySelector(elementQuerySelector);

    // when menu decider is changed, previous attributes should be removed from the `element` children
    // there many also be default required children added, 'defaultChild' prefix
    menuDeciderElement.addEventListener("change", (event) => {
        let optionAttributes = optionsMap.get(event.target.value);
        // remove children from container
        let userAddedElements = Array.from(element.querySelectorAll(".user-added-attribute").values());
        for (let userAddedElement of userAddedElements) {
            element.removeChild(userAddedElement.parentElement);
        }
        // add in default attributes that are required
        let defaultChildKeys = Object.keys(optionAttributes).filter(k => k.startsWith("defaultChild"));
        for (let defaultChildKey of defaultChildKeys) {
            let defaultChildAttributes = optionAttributes[defaultChildKey];
            let attribute = defaultChildKey.replace("defaultChild", "");
            let attributeContainerId = `${containerId}-${attribute}`;
            let inputAttribute = createAttributeFormFloating(defaultChildAttributes, attributeContainerId, inputClass, attribute);
            element.insertBefore(inputAttribute, buttonWithMenuDiv);
        }
    });
    // trigger when loaded to ensure default child is there at start
    menuDeciderElement.dispatchEvent(new Event("change"));

    // when click on the button, show menu of attributes
    addAttributeButton.addEventListener("click", (event) => {
        event.preventDefault();
        menu.open = !menu.open;

        // get current value of the data type
        let currentType = element.querySelector(elementQuerySelector).value;
        let currentMenuType = menu.getAttribute(menuAttributeName);
        if (currentType !== null && (currentMenuType === null || currentMenuType !== currentType)) {
            // clear out menu items
            menu.replaceChildren();
            let attributesForDataType = optionsMap.get(currentType);
            // menu items are the different attribute available for the type selected
            for (let [key] of Object.entries(attributesForDataType)) {
                if (!key.startsWith("defaultChild")) {
                    let menuItem = document.createElement("li");
                    menuItem.setAttribute("id", key);
                    menuItem.setAttribute("value", key);
                    let menuItemContent = document.createElement("button");
                    menuItemContent.setAttribute("class", "dropdown-item");
                    menuItemContent.setAttribute("type", "button");
                    menuItemContent.setAttribute("value", key);
                    menuItemContent.innerText = key;
                    menuItem.append(menuItemContent);
                    menu.append(menuItem);
                }
            }
            menu.setAttribute(menuAttributeName, currentType);
        }
    });

    // when attribute in menu is clicked, create text box with that attribute and its default value before the add button
    menu.addEventListener("click", (event) => {
        event.preventDefault();
        let attribute = event.target.getAttribute("value");
        // check if attribute already exists
        let attributeContainerId = containerId + "-" + attribute;
        if (element.querySelector("#" + attributeContainerId) === null) {
            // get default value for data type attribute along with other metadata
            let currentMenuType = menu.getAttribute(menuAttributeName);
            let currentTypeAttributes = optionsMap.get(currentMenuType);
            let attrMetadata = currentTypeAttributes[attribute];

            // add attribute field to field container
            if (attrMetadata["type"] === "min-max") {
                let formFloatingAttrMin = createAttributeFormFloating(attrMetadata, attributeContainerId, inputClass, attribute + "Min");
                let formFloatingAttrMax = createAttributeFormFloating(attrMetadata, attributeContainerId, inputClass, attribute + "Max");
                element.insertBefore(formFloatingAttrMin, buttonWithMenuDiv);
                element.insertBefore(formFloatingAttrMax, buttonWithMenuDiv);
            } else {
                let formFloatingAttr = createAttributeFormFloating(attrMetadata, attributeContainerId, inputClass, attribute);
                element.insertBefore(formFloatingAttr, buttonWithMenuDiv);
            }
            // remove item from menu
            let menuChild = menu.querySelector("#" + attribute);
            if (menuChild !== null) {
                menu.removeChild(menuChild);
            }
        }
    });
}

// Schema can be manually created or override automatic config
function createManualSchema() {
    let divContainer = document.createElement("div");
    divContainer.setAttribute("class", "data-source-schema-container");
    divContainer.setAttribute("id", "data-source-schema-container");
    // add new fields to schema
    let addFieldButton = document.createElement("button");
    addFieldButton.setAttribute("class", "btn btn-secondary");
    addFieldButton.setAttribute("type", "button");
    addFieldButton.setAttribute("id", "add-field-btn");
    addFieldButton.innerText = "+ Field";
    addFieldButton.addEventListener("click", function () {
        numFields += 1;
        let newField = createSchemaField(numFields);
        divContainer.insertBefore(newField, addFieldButton);
    });

    let recordCount = createRecordCount();
    divContainer.append(addFieldButton, recordCount);
    return divContainer;
}

function createRecordCount() {
    let recordCountContainer = document.createElement("div");
    recordCountContainer.setAttribute("id", "record-count-container");
    let recordCountHeader = document.createElement("h5");
    recordCountHeader.innerText = "Record count";
    let recordCountRow = document.createElement("div");
    recordCountRow.setAttribute("class", "row record-count-row");
    // have 3 columns
    // - total      -> number or random between min max
    // - per column -> number or random between min max
    // - estimated number of record
    let baseRecordCol = createRecordCountInput("base-record-count", "Records", "1000");
    let baseRecordMinInput = createRecordCountInput("min-gen-record-count", "Min", "1000");
    let baseRecordMaxInput = createRecordCountInput("max-gen-record-count", "Max", "2000");
    let baseRecordBetweenContainer = document.createElement("div");
    baseRecordBetweenContainer.setAttribute("class", "row g-1");
    baseRecordBetweenContainer.append(baseRecordMinInput, baseRecordMaxInput);
    let baseRecordOptions = [{text: "Records", child: baseRecordCol}, {
        text: "Generated records between",
        child: baseRecordBetweenContainer
    }];
    let baseRecordRadio = createRadioButtons("base-record-count-radio", baseRecordOptions);

    let perColumnRecordCol = createRecordCountInput("per-column-record-count", "Per column records", "2");
    let perColumnMinCol = createRecordCountInput("per-column-min-record-count", "Min", "1");
    let perColumnMaxCol = createRecordCountInput("per-column-max-record-count", "Max", "2");
    let perColumnBetweenContainer = document.createElement("div");
    perColumnBetweenContainer.setAttribute("class", "row g-1");
    perColumnBetweenContainer.append(perColumnMinCol, perColumnMaxCol);
    let perColumnOptions = [{text: "None"}, {
        text: "Per column",
        child: perColumnRecordCol
    }, {text: "Per column between", child: perColumnBetweenContainer}];
    let perColumnRadio = createRadioButtons("per-column-record-count-radio", perColumnOptions);
    // above per column radio is choice of columns
    let perColumnText = document.createElement("input");
    perColumnText.setAttribute("id", "per-column-names");
    perColumnText.setAttribute("class", "form-control input-field record-count-field");
    perColumnText.setAttribute("type", "text");
    perColumnText.setAttribute("aria-label", "Column(s)");
    perColumnText.setAttribute("placeholder", "");
    let perColumnFormFloating = createFormFloating("Column(s)", perColumnText);
    // TODO when perColumnText is empty, disable checkbox for per column
    let perColumnContainer = document.createElement("div");
    perColumnContainer.setAttribute("class", "col");
    perColumnContainer.append(perColumnRadio, perColumnFormFloating);

    let estimatedRecordCountContainer = document.createElement("div");
    estimatedRecordCountContainer.setAttribute("class", "col");
    let estimatedRecordCount = document.createElement("p");
    estimatedRecordCount.innerText = "Estimate number of records: 1000";
    estimatedRecordCountContainer.append(estimatedRecordCount);

    recordCountRow.append(baseRecordRadio, perColumnContainer, estimatedRecordCountContainer);
    $(recordCountRow).find("input[type=radio][name=base-record-count-radio],input[type=radio][name=per-column-record-count-radio]").change(function () {
        let newEstimate = estimateRecordCount(recordCountRow)["estimateRecords"];
        estimatedRecordCount.innerText = "Estimate number of records: " + newEstimate;
    });
    estimateRecordCount(recordCountRow);
    recordCountContainer.append(recordCountHeader, recordCountRow);
    return recordCountContainer;
}

function estimateRecordCount(recordCountRow) {
    let recordCountSummary = {};
    let baseRecordCheck = $(recordCountRow).find("input[name=base-record-count-radio]:checked").parent().find(".record-count-field");
    let baseRecordCount;
    if (baseRecordCheck.length > 1) {
        let minBase = Number($(baseRecordCheck).filter("input[aria-label=Min]").val());
        let maxBase = Number($(baseRecordCheck).filter("input[aria-label=Max]").val());
        baseRecordCount = (maxBase + minBase) / 2;
        recordCountSummary["recordsMin"] = minBase;
        recordCountSummary["recordsMax"] = maxBase;
    } else {
        baseRecordCount = Number(baseRecordCheck.val());
        recordCountSummary["records"] = baseRecordCount;
    }

    let perColumnCheck = $(recordCountRow).find("input[name=per-column-record-count-radio]:checked").parent().find(".record-count-field");
    let perColumnCount;
    if (perColumnCheck.length > 1) {
        let minPerCol = Number($(perColumnCheck).filter("input[aria-label=Min]").val());
        let maxPerCol = Number($(perColumnCheck).filter("input[aria-label=Max]").val());
        perColumnCount = (maxPerCol + minPerCol) / 2;
        recordCountSummary["perColumnRecordsMin"] = minPerCol;
        recordCountSummary["perColumnRecordsMax"] = maxPerCol;
    } else if (perColumnCheck.length === 1) {
        perColumnCount = Number(perColumnCheck.val());
        recordCountSummary["perColumnRecords"] = perColumnCount;
    } else {
        perColumnCount = 1;
    }
    if (perColumnCheck.length >= 1) {
        let perColumNames = $(recordCountRow).find("#per-column-names").val();
        recordCountSummary["perColumnNames"] = perColumNames.split(",");
    }

    recordCountSummary["estimateRecords"] = baseRecordCount * perColumnCount;
    return recordCountSummary;
}

function createRecordCountInput(id, label, value) {
    let recordCountInput = document.createElement("input");
    recordCountInput.setAttribute("id", id);
    recordCountInput.setAttribute("class", "form-control input-field record-count-field");
    recordCountInput.setAttribute("type", "number");
    recordCountInput.setAttribute("aria-label", label);
    recordCountInput.setAttribute("placeholder", value);
    recordCountInput.setAttribute("value", value);
    recordCountInput.setAttribute("min", "0");
    return createFormFloating(label, recordCountInput);
}

/*
Different types of validation:
- Basic column
- Dataset (column names, row count)
- Group by/aggregate
- Upstream
- External source (great expectations)
 */
function createManualValidation() {
    let divContainer = document.createElement("div");
    divContainer.setAttribute("class", "data-source-validation-container");
    divContainer.setAttribute("id", "data-source-validation-container");
    // add new validations
    let addValidationButton = document.createElement("button");
    addValidationButton.setAttribute("class", "btn btn-secondary");
    addValidationButton.setAttribute("type", "button");
    addValidationButton.setAttribute("id", "add-validation-btn");
    addValidationButton.innerText = "+ Validation";
    addValidationButton.addEventListener("click", function () {
        numValidations += 1;
        let newValidation = createValidation(numValidations);
        divContainer.insertBefore(newValidation, addValidationButton);
    });

    divContainer.append(addValidationButton);
    return divContainer;
}

function createValidation(index) {
    let validationContainer = document.createElement("div");
    validationContainer.setAttribute("class", "row g-1 mb-2 data-validation-container");
    validationContainer.setAttribute("id", "data-validation-container-" + index);

    let validationTypeSelect = document.createElement("select");
    validationTypeSelect.setAttribute("id", "validation-type-" + index);
    validationTypeSelect.setAttribute("aria-label", "Type");
    validationTypeSelect.setAttribute("class", "form-select input-field data-validation-field validation-type");
    let formFloatingType = createFormFloating("Type", validationTypeSelect);

    for (const key of validationTypeOptionsMap.keys()) {
        let selectOption = document.createElement("option");
        selectOption.setAttribute("value", key);
        if (key === "column") {
            selectOption.setAttribute("selected", "selected");
        }
        selectOption.innerText = key;
        validationTypeSelect.append(selectOption);
    }

    validationContainer.append(formFloatingType);
    createDataTypeAttributes(validationContainer, "validation-type");
    return validationContainer;
}

function createRadioButtons(name, options) {
    let radioButtonContainer = document.createElement("div");
    radioButtonContainer.setAttribute("id", name);
    radioButtonContainer.setAttribute("class", "col");
    for (const [i, option] of options.entries()) {
        let formCheck = document.createElement("div");
        formCheck.setAttribute("class", "form-check");
        let formInput = document.createElement("input");
        formInput.setAttribute("class", "form-check-input");
        formInput.setAttribute("type", "radio");
        formInput.setAttribute("name", name);
        formInput.setAttribute("id", `radio-${name}-${i}`);
        if (i === 0) {
            formInput.setAttribute("checked", "");
        }
        let formLabel = document.createElement("label");
        formLabel.setAttribute("class", "form-check-label");
        formLabel.setAttribute("for", `radio-${name}-${i}`);
        formLabel.innerText = option.text;

        formCheck.append(formInput, formLabel);
        if (option.child) formCheck.append(option.child);
        radioButtonContainer.append(formCheck);
    }
    return radioButtonContainer;
}

submitForm();

function submitForm() {
    let form = document.getElementById("plan-form");
    form.addEventListener("submit", async function (e) {
        e.preventDefault();
        // collect all the user inputs
        let planName = form.querySelector("#plan-name").value;
        let allDataSources = form.querySelectorAll(".data-source-config-container");
        const runId = crypto.randomUUID();
        let allUserInputs = [];
        for (let dataSource of allDataSources) {
            // all have class 'input-field'
            // data-source-property    -> data source connection config
            // data-source-field        -> data source field definition
            let currentDataSource = {};
            // get data connection name
            let dataConnectionSelect = dataSource.querySelector(".data-connection-name");
            currentDataSource["name"] = dataConnectionSelect.value;

            // get all fields
            let dataSourceFields = Array.from(dataSource.querySelectorAll(".data-field-container").values());
            // get name, type and options applied to each field (class .data-source-field)
            let dataFieldsWithAttributes = dataSourceFields.map(field => {
                let fieldAttributes = Array.from(field.querySelectorAll(".data-source-field").values());
                return fieldAttributes.reduce((options, attr) => {
                    let label = camelize(attr.getAttribute("aria-label"));
                    let fieldValue = attr.value;
                    if (label === "name" || label === "type") {
                        options[label] = fieldValue;
                    } else {
                        let currOpts = (options["options"] || new Map());
                        currOpts.set(label, fieldValue);
                        options["options"] = currOpts;
                    }
                    return options;
                }, {});
            });
            currentDataSource["fields"] = Object.values(dataFieldsWithAttributes);

            // get all validations
            let dataSourceValidations = Array.from(dataSource.querySelectorAll(".data-validation-container").values());
            let dataValidationsWithAttributes = dataSourceValidations.map(validation => {
                let validationAttributes = Array.from(validation.querySelectorAll(".data-validation-field").values());
                return validationAttributes.reduce((options, attr) => {
                    let label = camelize(attr.getAttribute("aria-label"));
                    let fieldValue = attr.value;
                    if (label === "name" || label === "type") {
                        options[label] = fieldValue;
                    } else {
                        let currOpts = (options["options"] || new Map());
                        currOpts.set(label, fieldValue);
                        options["options"] = currOpts;
                    }
                    return options;
                }, {});
            });
            currentDataSource["validations"] = Object.values(dataValidationsWithAttributes);

            let recordCountRow = dataSource.querySelector(".record-count-row");
            let recordCountSummary = estimateRecordCount(recordCountRow);
            delete recordCountSummary.estimateRecords;
            currentDataSource["count"] = recordCountSummary;
            allUserInputs.push(currentDataSource);
        }
        // data-source-container -> data source connection props, data-source-config-container
        // data-source-config-container -> data-source-generation-config-container
        // data-source-generation-config-container -> data-source-schema-container
        // data-source-schema-container -> data-field-container
        const requestBody = {name: planName, id: runId, dataSources: allUserInputs}
        console.log(JSON.stringify(requestBody));
        fetch("http://localhost:9090/run", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(requestBody)
        })
            .catch(err => {
                console.error(err);
                const toast = new bootstrap.Toast(createToast("Plan run", `Plan run failed! Error: ${err}`));
                toast.show();
            })
            .then(r => r.text())
            .then(async r => {
                const toast = new bootstrap.Toast(createToast("Plan run", `Plan run started! Msg: ${r}`));
                toast.show();
                // poll every 1 second for status of plan run
                let currentStatus = "started";
                while (currentStatus !== "finished" && currentStatus !== "failed") {
                    await fetch(`http://localhost:9090/run/status/${runId}`, {
                        method: "GET",
                        headers: {
                            Accept: "application/json"
                        }
                    })
                        .catch(err => {
                            console.error(err);
                            const toast = new bootstrap.Toast(createToast(planName, `Plan run failed! Error: ${err}`, "fail"));
                            toast.show();
                        })
                        .then(resp => resp.json())
                        .then(respJson => {
                            let latestStatus = respJson.status;
                            if (latestStatus !== currentStatus) {
                                currentStatus = latestStatus;
                                let type = "running";
                                let msg = `Plan run update, status: ${latestStatus}`;
                                if (currentStatus === "finished") {
                                    type = "success";
                                    msg = "Successfully completed!";
                                } else if (currentStatus === "failed") {
                                    type = "fail";
                                    let failReason = respJson.failedReason.length > 200 ? respJson.failedReason.substring(0, 200) + "..." : respJson.failedReason;
                                    msg = `Plan failed! Error: ${failReason}`;
                                }
                                const toast = new bootstrap.Toast(createToast(planName, msg, type));
                                toast.show();
                            }
                        });
                    await wait(500);
                }
            });
    });
}

function createToast(header, message, type) {
    let toast = document.createElement("div");
    toast.setAttribute("class", "toast");
    toast.setAttribute("role", "alert");
    toast.setAttribute("aria-live", "assertive");
    toast.setAttribute("aria-atomic", "true");
    let toastHeader = document.createElement("div");
    toastHeader.setAttribute("class", "toast-header");
    let icon = document.createElement("i");
    if (type === "success") {
        icon.setAttribute("class", "bi bi-check-square-fill");
        icon.setAttribute("style", "color: green");
    } else if (type === "fail") {
        icon.setAttribute("class", "bi bi-exclamation-square-fill");
        icon.setAttribute("style", "color: red");
    } else {
        icon.setAttribute("class", "bi bi-caret-right-square-fill");
        icon.setAttribute("style", "color: orange");
    }
    let strong = document.createElement("strong");
    strong.setAttribute("class", "me-auto");
    strong.innerText = header;
    let small = document.createElement("small");
    small.innerText = "Now";
    let button = document.createElement("button");
    button.setAttribute("type", "button");
    button.setAttribute("class", "btn-close");
    button.setAttribute("data-bs-dismiss", "toast");
    button.setAttribute("aria-label", "Close");
    let toastBody = document.createElement("div");
    toastBody.setAttribute("class", "toast-body");
    toastBody.innerText = message;
    toastHeader.append(icon, strong, small, button);
    toast.append(toastHeader, toastBody);
    toastPosition.append(toast);
    return toast;
}

function formatDate(isMin, isTimestamp) {
    let currentDate = new Date();
    if (isMin) {
        currentDate.setDate(currentDate.getDate() - 365);
    }
    return isTimestamp ? currentDate.toISOString() : currentDate.toISOString().split("T")[0];
}

function createFormFloating(floatingText, inputAttr) {
    let formFloatingAttr = document.createElement("div");
    formFloatingAttr.setAttribute("class", "col form-floating");
    let labelAttr = document.createElement("label");
    labelAttr.setAttribute("for", inputAttr.getAttribute("id"));
    labelAttr.innerText = floatingText;
    formFloatingAttr.append(inputAttr, labelAttr);
    return formFloatingAttr;
}

function createBadge(text) {
    let badge = document.createElement("span");
    badge.setAttribute("class", "col badge bg-secondary m-2");
    badge.innerText = text;
    return badge;
}

const wait = function (ms = 1000) {
    return new Promise(resolve => {
        setTimeout(resolve, ms);
    });
};
