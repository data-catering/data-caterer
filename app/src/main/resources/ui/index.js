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

import {
    camelize,
    createAccordionItem,
    createBadge,
    createButton,
    createCloseButton,
    createFormFloating,
    createFormText,
    createInput,
    createInputGroup,
    createSelect
} from "./shared.js";

const baseDataTypes = ["string", "integer", "long", "short", "decimal", "double", "float", "date", "timestamp", "binary", "array", "struct"];
const dataTypeOptionsMap = new Map();
const defaultDataTypeOptions = {
    enableEdgeCases: {
        default: "false",
        type: "text",
        choice: ["true", "false"],
        help: "Enable generating edge case values for data type."
    },
    edgeCaseProbability: {
        default: 0.0,
        type: "number",
        min: 0.0,
        max: 1.0,
        step: 0.001,
        help: "Probability of generating edge case values. Range from 0-1."
    },
    isUnique: {default: "false", type: "text", choice: ["true", "false"], help: "Generate only unique values."},
    seed: {
        default: -1,
        type: "number",
        min: -1,
        max: 9223372036854775807,
        help: "Seed for generating consistent random values."
    },
    sql: {default: "", type: "text", help: "SQL expression for generating data."},
    oneOf: {default: [], type: "text", help: "Generated values will be one of the defined values. Comma separated."},
    omit: {
        default: "false",
        type: "text",
        choice: ["true", "false"],
        help: "Exclude the column from the final output. Can be used for intermediate data generation."
    },
};

function getNumberOptions(min, max) {
    let minMaxOpt = min && max ? {min: min, max: max} : {};
    return {
        min: {default: 0, type: "number", ...minMaxOpt, help: "Minimum generated value."},
        max: {default: 1000, type: "number", ...minMaxOpt, help: "Maximum generated value."},
        stddev: {
            default: 1.0,
            type: "number",
            min: 0.0,
            max: 100000000.0,
            help: "Standard deviation of generated values."
        },
        mean: {default: 500, type: "number", ...minMaxOpt, help: "Mean of generated values."}
    };
}

dataTypeOptionsMap.set("string", {
    ...defaultDataTypeOptions,
    minLen: {default: 1, type: "number", min: 0, max: 1000, help: "Minimum length of generated values."},
    maxLen: {default: 10, type: "number", min: 0, max: 1000, help: "Maximum length of generated values."},
    expression: {default: "", type: "text", help: "Faker expression to generate values."},
    enableNull: {default: "false", type: "text", choice: ["true", "false"], help: "Enable generation of null values."},
    nullProbability: {
        default: 0.0,
        type: "number",
        min: 0.0,
        max: 1.0,
        step: 0.001,
        help: "Probability of generating null values. Range from 0-1."
    },
    regex: {default: "", type: "text", help: "Regex for generating values."}
});
dataTypeOptionsMap.set("integer", {...defaultDataTypeOptions, ...getNumberOptions(-2147483648, 2147483647)});
dataTypeOptionsMap.set("long", {...defaultDataTypeOptions, ...getNumberOptions(-9223372036854775808, 9223372036854775807)});
dataTypeOptionsMap.set("short", {...defaultDataTypeOptions, ...getNumberOptions(-32768, 32767)});
dataTypeOptionsMap.set("decimal", {
    ...defaultDataTypeOptions,
    ...getNumberOptions(),
    numericPrecision: {
        default: 10,
        type: "number",
        min: 0,
        max: 2147483647,
        help: "Precision for generated decimal values."
    },
    numericScale: {default: 0, type: "number", min: 0, max: 2147483647, help: "Scale for geneated decimal values."}
});
dataTypeOptionsMap.set("double", {...defaultDataTypeOptions, ...getNumberOptions()});
dataTypeOptionsMap.set("float", {...defaultDataTypeOptions, ...getNumberOptions()});
dataTypeOptionsMap.set("date", {
    ...defaultDataTypeOptions,
    min: {
        default: formatDate(true),
        type: "date",
        min: "0001-01-01",
        max: "9999-12-31",
        help: "Minimum date of generated values. Expected format 'yyyy-MM-dd'."
    },
    max: {
        default: formatDate(false),
        type: "date",
        min: "0001-01-01",
        max: "9999-12-31",
        help: "Maximum date of generated values. Expected format 'yyyy-MM-dd'."
    }
});
dataTypeOptionsMap.set("timestamp", {
    ...defaultDataTypeOptions,
    min: {
        default: formatDate(true, true),
        type: "datetime-local",
        min: "0001-01-01 00:00:00",
        max: "9999-12-31 23:59:59",
        help: "Minimum timestamp of generated values. Expected format 'yyyy-MM-dd HH:mm:ss'."
    },
    max: {
        default: formatDate(false, true),
        type: "datetime-local",
        min: "0001-01-01 00:00:00",
        max: "9999-12-31 23:59:59",
        help: "Maximum timestamp of generated values. Expected format 'yyyy-MM-dd HH:mm:ss'."
    }
});
dataTypeOptionsMap.set("binary", {
    ...defaultDataTypeOptions,
    minLen: {default: 1, type: "number", min: 0, max: 2147483647, help: "Minimum length of generated values."},
    maxLen: {default: 20, type: "number", min: 0, max: 2147483647, help: "Maximum length of generated values."},
});
dataTypeOptionsMap.set("array", {
    ...defaultDataTypeOptions,
    arrayMinLen: {default: 0, type: "number", min: 0, max: 2147483647, help: "Minimum generated array length."},
    arrayMaxLen: {default: 5, type: "number", min: 0, max: 2147483647, help: "Maximum generated array length."},
    arrayType: {default: "string", type: "text", choice: baseDataTypes, help: "Data type of array values."}
});
dataTypeOptionsMap.set("struct", {...defaultDataTypeOptions});

const validationTypeOptionsMap = new Map();
const defaultValidationOptions = {
    description: {default: "", type: "text", help: "Description of validation. Used in report."},
    errorThreshold: {
        default: 0.0,
        type: "number",
        min: 0.0,
        help: "Number or percentage (0.0 to 1.0) of errors before marking validation as failed."
    },
}
validationTypeOptionsMap.set("column", {
    ...defaultValidationOptions,
    defaultChildColumn: {default: "", type: "text", required: "", help: "Column to validate."},
    equal: {
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Not"},
        help: "Equal to value. Select 'Not' for not equals."
    },
    null: {default: "", type: "text", disabled: ""},
    notNull: {default: "", type: "text", disabled: ""},
    contains: {
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Not"},
        help: "Contains value. Select 'Not' for not contains."
    },
    unique: {default: "", type: "text"},
    lessThan: {
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Strictly"},
        help: "Less than value. Select 'Strictly' for less than or equal to."
    },
    greaterThan: {
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Strictly"},
        help: "Greater than value. Select 'Strictly' for greater than or equal to."
    },
    between: {
        default: "",
        type: "min-max",
        group: {type: "checkbox", innerText: "Not"},
        help: "Between values. Select 'Not' for not between."
    },
    in: {default: "", type: "text"},    // provide as list?
    matches: {
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Not"},
        help: "Matches regex. Select 'Not' for not matches regex."
    },
    startsWith: {
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Not"},
        help: "Starts with value. Select 'Not' for not starts with."
    },
    endsWith: {
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Not"},
        help: "Ends with value. Select 'Not' for not ends with."
    },
    size: {
        default: 0,
        type: "number",
        group: {type: "checkbox", innerText: "Not"},
        help: "Equal to size. Select 'Not' for not equal to size."
    },
    lessThanSize: {
        default: 0,
        type: "number",
        group: {type: "checkbox", innerText: "Strictly"},
        help: "Less than size. Select 'Strictly' for less than or equal to size."
    },
    greaterThanSize: {
        default: 0,
        type: "number",
        group: {type: "checkbox", innerText: "Strictly"},
        help: "Greater than size. Select 'Strictly' for greater than or equal to size."
    },
    luhnCheck: {default: "", type: "text", disabled: ""},
    hasType: {default: "string", type: "text", choice: baseDataTypes},
    sql: {default: "", type: "text"},
});
validationTypeOptionsMap.set("groupBy", {
    ...defaultValidationOptions,
    defaultChildGroupByColumns: {
        default: "",
        type: "text",
        required: "",
        help: "Column name(s) to group by. Comma separated."
    },
    count: {default: "", type: "text", help: "Column name to count number of groups after group by."},
    sum: {default: "", type: "text", help: "Column name of values to sum after group by."},
    min: {default: "", type: "text", help: "Column name to find minimum value after group by."},
    max: {default: "", type: "text", help: "Column name to find maximum value after group by."},
    average: {default: "", type: "text", help: "Column name to find average value after group by."},
    standardDeviation: {
        default: "",
        type: "text",
        help: "Column name to find standard deviation value after group by."
    },
});
validationTypeOptionsMap.set("upstream", {
    ...defaultValidationOptions,
    defaultChildUpstreamTaskName: {
        default: "",
        type: "text",
        required: "",
        selector: ".task-name-field",
        help: "Name of upstream data generation task."
    },  //can also have upstream data source
    joinColumns: {default: "", type: "text", help: "Column name(s) to join by."},
    joinType: {
        default: "outer",
        type: "text",
        choice: ["inner", "outer", "left_outer", "right_outer", "left_semi", "anti", "cross"],
        help: "Type of join."
    },
    joinExpr: {default: "", type: "text", help: "Custom join SQL expression."}
});
validationTypeOptionsMap.set("columnNames", {
    ...defaultValidationOptions,
    countEqual: {default: 0, type: "number", help: "Number of columns has to equal value."},
    countBetween: {
        default: 0,
        type: "min-max",
        help: "Number of columns has to be between min and max value (inclusive)."
    },
    matchOrder: {
        default: "",
        type: "text",
        help: "All column names match particular ordering and is complete. Comma separated."
    },
    matchSet: {
        default: "",
        type: "text",
        help: "Column names contains set of expected names. Order is not checked. Comma separated."
    },
});

const configurationOptionsMap = new Map();
configurationOptionsMap.set("flag", {
    enableCount: {
        default: "true",
        type: "text",
        choice: ["true", "false"],
        help: "Count the number of records generated. Can be disabled to improve performance."
    },
    enableGenerateData: {
        default: "true",
        type: "text",
        choice: ["true", "false"],
        help: "Enable/disable data generation."
    },
    enableFailOnError: {
        default: "true",
        type: "text",
        choice: ["true", "false"],
        help: "Whilst saving generated data, if there is an error, it will stop any further data from being generated."
    },
    enableUniqueCheck: {
        default: "false",
        type: "text",
        choice: ["true", "false"],
        help: "Enable/disable generating unique values for columns marked as unique. Can be disabled to improve performance but not guarantee uniqueness."
    },
    enableSinkMetadata: {
        default: "false",
        type: "text",
        choice: ["true", "false"],
        help: "Run data profiling for the generated data. Shown in HTML reports if enableSaveSinkMetadata is enabled."
    },
    enableSaveReports: {
        default: "true",
        type: "text",
        choice: ["true", "false"],
        help: "Enable/disable HTML reports summarising data generated, metadata of data generated (if enableSinkMetadata is enabled) and validation results (if enableValidation is enabled)."
    },
    enableValidation: {
        default: "true",
        type: "text",
        choice: ["true", "false"],
        help: "Run validations as described in plan. Results can be viewed from logs or from HTML report if enableSaveSinkMetadata is enabled."
    },
    enableAlerts: {
        default: "true",
        type: "text",
        choice: ["true", "false"],
        help: "Enable/disable alerts being sent when plan execution is finished (can be configured for success/failure)."
    },
    enableGenerateValidations: {
        default: "false",
        type: "text",
        choice: ["true", "false"],
        paid: "true",
        help: "Enable/disable automatically generating validations based on the data sources defined."
    },
    enableRecordTracking: {
        default: "false",
        type: "text",
        choice: ["true", "false"],
        paid: "true",
        help: "Enable/disable tracking of data records generated."
    },
    enableDeleteGeneratedRecords: {
        default: "false",
        type: "text",
        choice: ["true", "false"],
        paid: "true",
        help: "Delete all generated records based off record tracking (if enableRecordTracking has been set to true whilst generating)."
    },
    enableGeneratePlanAndTasks: {
        default: "false",
        type: "text",
        choice: ["true", "false"],
        paid: "true",
        help: "Enable/disable plan and task automatic generation based off data source connections."
    },
});
configurationOptionsMap.set("folder", {
    generatedReportsFolderPath: {
        default: "",
        type: "text",
        help: "Folder path where generated HTML reports will be saved."
    },
    validationFolderPath: {
        default: "",
        type: "text",
        help: "If using YAML validation file(s), folder path that contains all validation files (can have nested directories)."
    },
    planFilePath: {
        default: "",
        type: "text",
        help: "If using YAML plan file, path to use when generating and/or validating data."
    },
    taskFolderPath: {
        default: "",
        type: "text",
        help: "If using YAML task file(s), folder path that contains all the task files (can have nested directories)."
    },
    generatedPlanAndTasksFolderPath: {
        default: "",
        type: "text",
        paid: "true",
        help: "Folder path where generated plan and task files will be saved."
    },
    recordTrackingFolderPath: {
        default: "",
        type: "text",
        paid: "true",
        help: "Folder path where record tracking files will be saved."
    },
    recordTrackingForValidationFolderPath: {
        default: "",
        type: "text",
        paid: "true",
        help: "Folder path where record tracking for validation files will be saved."
    },
});
configurationOptionsMap.set("metadata", {
    numGeneratedSamples: {
        default: 10,
        type: "number",
        min: 0,
        help: "Number of sample records from generated data to take. Shown in HTML report."
    },
    numRecordsFromDataSource: {
        default: 10000,
        type: "number",
        paid: "true",
        min: 0,
        help: "Number of records read in from the data source that could be used for data profiling."
    },
    numRecordsForAnalysis: {
        default: 10000,
        type: "number",
        paid: "true",
        min: 0,
        help: "Number of records used for data profiling from the records gathered in numRecordsFromDataSource."
    },
    oneOfDistinctCountVsCountThreshold: {
        default: 0.2,
        type: "number",
        paid: "true",
        min: 0.0,
        max: 1.0,
        step: 0.001,
        help: "Threshold ratio to determine if a field is of type oneOf (i.e. a field called status that only contains open or closed. Distinct count = 2, total count = 10, ratio = 2 / 10 = 0.2 therefore marked as oneOf)."
    },
    oneOfMinCount: {
        default: 1000,
        type: "number",
        paid: "true",
        min: 0,
        help: "Minimum number of records required before considering if a field can be of type oneOf."
    },
});
configurationOptionsMap.set("generation", {
    numRecordsPerBatch: {
        default: 100000,
        type: "number",
        min: 0,
        help: "Number of records across all data sources to generate per batch."
    },
    numRecordsPerStep: {
        default: -1,
        type: "number",
        help: "Overrides the count defined in each step with this value if defined (i.e. if set to 1000, for each step, 1000 records will be generated)."
    },
});
configurationOptionsMap.set("validation", {
    numSampleErrorRecords: {
        default: 5,
        type: "number",
        help: "Number of sample error records to show in HTML report. Useful for debugging."
    },
    enableDeleteRecordTrackingFiles: {
        default: "true",
        type: "text",
        choice: ["true", "false"],
        paid: "true",
        help: "Enable/disable to delete record tracking files at end of execution."
    },
});
configurationOptionsMap.set("alert", {
    triggerOn: {
        default: "all",
        type: "text",
        choice: ["all", "failure", "success", "generation_failure", "validation_failure", "generation_success", "validation_success"],
        help: "Condition for triggering alert."
    },
    slackToken: {
        default: "",
        type: "text",
        help: "Slack token to connect to Slack. Check https://api.slack.com/authentication/token-types for more details."
    },
    slackChannels: {
        default: "",
        type: "text",
        help: "Define one or more Slack channels to send alerts to. Comma separated."
    },
});


const toastPosition = document.getElementById("toast-container");
const addTaskButton = document.getElementById("add-task-button");
const tasksDiv = document.getElementById("tasks-details-body");
const foreignKeysDiv = document.getElementById("foreign-keys-details-body");
const configurationDiv = document.getElementById("configuration-details-body");
let numDataSources = 1;
let numFields = 0;
let numValidations = 0;
let numForeignKeys = 0;
let numForeignKeysLinks = 0;
let numAddAttributeButton = 0;

tasksDiv.append(await createDataSourceForPlan(numDataSources));
foreignKeysDiv.append(createForeignKeys());
configurationDiv.append(createConfiguration());
addTaskButton.addEventListener("click", async function () {
    numDataSources += 1;
    let divider = document.createElement("hr");
    let newDataSource = await createDataSourceForPlan(numDataSources, divider);
    tasksDiv.append(newDataSource);
});

//create row with data source name and checkbox elements for generation and validation
async function createDataSourceForPlan(index, divider) {
    let dataSourceRow = document.createElement("div");
    dataSourceRow.setAttribute("class", "mb-3");
    let closeButton = createCloseButton(dataSourceRow);
    let dataSourceConfig = await createDataSourceConfiguration(index, closeButton, divider);
    dataSourceRow.append(dataSourceConfig);
    return dataSourceRow;
}

function createDataConfigElement(index, name) {
    const nameCapitalize = name.charAt(0).toUpperCase() + name.slice(1);
    let dataConfigContainer = document.createElement("div");
    dataConfigContainer.setAttribute("id", `data-source-${name}-config-container`);
    dataConfigContainer.setAttribute("class", "mt-1");

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
        addDataConfigCheckboxListener(index, checkboxInput, name);
    }
    return createAccordionItem(`${index}-${name}`, nameCapitalize, "", dataConfigContainer);
}

function addDataConfigCheckboxListener(index, element, name) {
    let configContainer = element.parentElement.parentElement;
    if (element.getAttribute("value") === "manual") {
        element.addEventListener("change", (event) => {
            manualCheckboxListenerDisplay(index, event, configContainer, name);
        });
    }
}

function manualCheckboxListenerDisplay(index, event, configContainer, name) {
    let querySelector = name === "generation" ? "#data-source-schema-container" : "#data-source-validation-container";
    let schemaContainer = configContainer.querySelector(querySelector);
    if (event.currentTarget.checked) {
        if (schemaContainer === null) {
            let newElement = name === "generation" ? createManualSchema(index) : createManualValidation(index);
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
    let taskNameInput = createInput(`task-name-${index}`, "Task name", "form-control input-field task-name-field", "text", `task-${index}`);
    taskNameInput.setAttribute("required", "");
    let taskNameFormFloating = createFormFloating("Task name", taskNameInput);

    let dataConnectionSelect = createSelect(`data-source-connection-${index}`, "Data source", "form-control input-field data-connection-name");
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
async function createDataSourceConfiguration(index, closeButton, divider) {
    let divContainer = document.createElement("div");
    divContainer.setAttribute("id", "data-source-config-container-" + index);
    divContainer.setAttribute("class", "data-source-config-container");
    let dataConnectionFormFloating = await createDataConnectionInput(index);
    let dataConfigAccordion = document.createElement("div");
    dataConfigAccordion.setAttribute("class", "accordion mt-2");
    let dataGenConfigContainer = createDataConfigElement(index, "generation");
    let dataValidConfigContainer = createDataConfigElement(index, "validation");

    dataConnectionFormFloating.insertBefore(closeButton, dataConnectionFormFloating.firstChild);
    dataConfigAccordion.append(dataGenConfigContainer, dataValidConfigContainer);
    if (divider) {
        divContainer.append(divider);
    }
    divContainer.append(dataConnectionFormFloating, dataConfigAccordion);
    return divContainer;
}

function createSchemaField(index) {
    let fieldContainer = document.createElement("div");
    fieldContainer.setAttribute("class", "row g-1 mb-2 data-field-container");
    fieldContainer.setAttribute("id", "data-field-container-" + index);

    let fieldName = createInput(`field-name-${index}`, "Name", "form-control input-field data-source-field", "text", "");
    fieldName.setAttribute("required", "");
    let formFloatingName = createFormFloating("Name", fieldName);

    let fieldTypeSelect = createSelect(`field-type-${index}`, "Type", "form-select input-field data-source-field field-type");
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

    let accordionItem = createAccordionItem(`column-${index}`, `column-${index}`, "", fieldContainer);
    // when field name changes, update the accordion header
    fieldName.addEventListener("input", (event) => {
        let accordionButton = $(accordionItem).find(".accordion-button");
        accordionButton[0].innerText = event.target.value;
    });
    let closeButton = createCloseButton(accordionItem);
    fieldContainer.append(closeButton, formFloatingName, formFloatingType);
    createDataOrValidationTypeAttributes(fieldContainer, "data-type");
    return accordionItem;
}

function createAttributeFormFloating(attrMetadata, attributeContainerId, inputClass, attribute, col) {
    let inputAttr;
    if (attrMetadata.choice) {
        inputAttr = document.createElement("select");
        inputAttr = createSelect(attributeContainerId, attribute, `form-select input-field ${inputClass}`);
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
        inputAttr = createInput(attributeContainerId, attribute, `form-control input-field ${inputClass}`, attrMetadata["type"], attrMetadata["default"]);
        inputAttr.setAttribute("required", "");
    }

    for (const [key, value] of Object.entries(attrMetadata)) {
        if (key !== "default" && key !== "type" && key !== "choice" && key !== "help" && key !== "group") {
            inputAttr.setAttribute(key, value);
        }
    }
    let formFloatingInput = createFormFloating(attribute, inputAttr);
    // if group is defined, there is additional input required
    // if help is defined, add to container
    if (attrMetadata.group) {
        let startInputDiv = document.createElement("div");
        startInputDiv.setAttribute("class", "input-group-text");
        let startInput = createInput(`${attributeContainerId}-option-input`, attribute, "form-check-input mt-0", attrMetadata.group.type);
        let startInputLabel = document.createElement("label");
        startInputLabel.setAttribute("class", "form-check-label");
        startInputLabel.setAttribute("for", `${attributeContainerId}-option-input`);
        startInputLabel.innerText = attrMetadata.group.innerText;
        startInputDiv.append(startInput, startInputLabel);
        return createInputGroup(startInputDiv, formFloatingInput, col ? col : "col-8");
    } else {
        if (col) {
            formFloatingInput.setAttribute("class", col);
        }
        return formFloatingInput;
    }
}

// Create a button for overriding attributes of the data field based on data type, i.e. set min to 10 for integer
function createDataOrValidationTypeAttributes(element, elementType) {
    let mainContainer = element.parentElement;
    let defaultAttributeClass = "default-attribute";
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
    let addAttributeId = element.getAttribute("id") + "-add-attribute-button";
    let addAttributeButton = createButton(addAttributeId, "add-attribute", "btn btn-secondary dropdown-toggle");
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
        // remove default children from container
        let defaultAddedElements = Array.from(element.querySelectorAll("." + defaultAttributeClass).values());
        for (let defaultAddedElement of defaultAddedElements) {
            // TODO check if userAddedElement is compatible with new 'type' (i.e. change data type from int to long should keep min)
            element.removeChild(defaultAddedElement);
        }
        // remove user added children from container
        let userAddedElements = Array.from(mainContainer.querySelectorAll(".user-added-attribute").values());
        for (let userAddedElement of userAddedElements) {
            mainContainer.removeChild(userAddedElement);
        }

        // add in default attributes that are required
        let defaultChildKeys = Object.keys(optionAttributes).filter(k => k.startsWith("defaultChild"));
        for (let defaultChildKey of defaultChildKeys) {
            let defaultChildAttributes = optionAttributes[defaultChildKey];
            let attribute = defaultChildKey.replace("defaultChild", "");
            let attributeContainerId = `${containerId}-${attribute}`;
            let inputAttribute = createAttributeFormFloating(defaultChildAttributes, attributeContainerId, inputClass, attribute, `col ${defaultAttributeClass}`);
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
            let newAttributeRow = document.createElement("div");
            newAttributeRow.setAttribute("class", "row g-1 m-1 align-items-center user-added-attribute");
            let closeButton = createCloseButton(newAttributeRow);
            newAttributeRow.append(closeButton);
            if (attrMetadata["type"] === "min-max") {
                let formFloatingAttrMin = createAttributeFormFloating(attrMetadata, attributeContainerId, inputClass, attribute + "Min", "col-4");
                let formFloatingAttrMax = createAttributeFormFloating(attrMetadata, attributeContainerId, inputClass, attribute + "Max", "col-3");
                newAttributeRow.append(formFloatingAttrMin, formFloatingAttrMax);
                if (attrMetadata.help) {
                    let helpDiv = createFormText(formFloatingAttrMin.getAttribute("id"), attrMetadata.help, "span");
                    formFloatingAttrMin.setAttribute("aria-describedby", helpDiv.getAttribute("id"));
                    newAttributeRow.append(helpDiv);
                }
                mainContainer.append(newAttributeRow);
            } else {
                let formFloatingAttr = createAttributeFormFloating(attrMetadata, attributeContainerId, inputClass, attribute, "col-7");
                newAttributeRow.append(formFloatingAttr);
                if (attrMetadata.help) {
                    let helpDiv = createFormText(formFloatingAttr.getAttribute("id"), attrMetadata.help, "span");
                    formFloatingAttr.setAttribute("aria-describedby", helpDiv.getAttribute("id"));
                    newAttributeRow.append(helpDiv);
                }
                mainContainer.append(newAttributeRow);
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
function createManualSchema(index) {
    let divContainer = document.createElement("div");
    divContainer.setAttribute("class", "data-source-schema-container");
    divContainer.setAttribute("id", "data-source-schema-container");
    let schemaAccordion = document.createElement("div");
    schemaAccordion.setAttribute("class", "accordion m-2");
    // add new fields to schema
    let addFieldButton = createButton("add-field-btn", "add-field", "btn btn-secondary", "+ Field");
    addFieldButton.addEventListener("click", function () {
        numFields += 1;
        let newField = createSchemaField(numFields);
        schemaAccordion.append(newField);
    });

    let recordCount = createRecordCount(index);
    divContainer.append(addFieldButton, schemaAccordion, recordCount);
    return divContainer;
}

function createPerColumnCountContainer(index) {
    let perColumnRecordCol = createRecordCountInput(index, "per-column-record-count", "Per column records", "2");
    let perColumnMinCol = createRecordCountInput(index, "per-column-min-record-count", "Min", "1");
    let perColumnMaxCol = createRecordCountInput(index, "per-column-max-record-count", "Max", "2");
    let perColumnBetweenContainer = document.createElement("div");
    perColumnBetweenContainer.setAttribute("class", "row g-1");
    perColumnBetweenContainer.append(perColumnMinCol, perColumnMaxCol);
    let perColumnOptions = [{text: "None"}, {
        text: "Per column",
        child: perColumnRecordCol
    }, {text: "Per column between", child: perColumnBetweenContainer}];
    let perColumnRadio = createRadioButtons(index, "per-column-record-count-radio", perColumnOptions);
    // above per column radio is choice of columns
    let perColumnText = createInput(`per-column-names-${index}`, "Column(s)", "form-control input-field record-count-field", "text", "");
    let perColumnFormFloating = createFormFloating("Column(s)", perColumnText);
    // TODO when perColumnText is empty, disable checkbox for per column
    let perColumnContainer = document.createElement("div");
    perColumnContainer.setAttribute("class", "col");
    perColumnContainer.append(perColumnRadio, perColumnFormFloating);
    return perColumnContainer;
}

function createBaseRecordCountContainer(index) {
    let baseRecordCol = createRecordCountInput(index, "base-record-count", "Records", "1000");
    let baseRecordMinInput = createRecordCountInput(index, "min-gen-record-count", "Min", "1000");
    let baseRecordMaxInput = createRecordCountInput(index, "max-gen-record-count", "Max", "2000");
    let baseRecordBetweenContainer = document.createElement("div");
    baseRecordBetweenContainer.setAttribute("class", "row g-1");
    baseRecordBetweenContainer.append(baseRecordMinInput, baseRecordMaxInput);
    let baseRecordOptions = [{text: "Records", child: baseRecordCol}, {
        text: "Generated records between",
        child: baseRecordBetweenContainer
    }];
    return createRadioButtons(index, "base-record-count-radio", baseRecordOptions);
}

function createRecordCount(index) {
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
    let baseRecordRadio = createBaseRecordCountContainer(index);
    let perColumnContainer = createPerColumnCountContainer(index);
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
        recordCountSummary["perColumnNames"] = perColumNames ? perColumNames.split(",") : [];
    }

    recordCountSummary["estimateRecords"] = baseRecordCount * perColumnCount;
    return recordCountSummary;
}

function createRecordCountInput(index, name, label, value) {
    let recordCountInput = createInput(`${name}-${index}`, label, "form-control input-field record-count-field", "number", value);
    let radioGroup = name.startsWith("per-column") ? "per-column-count" : "base-record-count";
    recordCountInput.setAttribute("radioGroup", radioGroup);
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
function createManualValidation(index) {
    let divContainer = document.createElement("div");
    divContainer.setAttribute("class", "data-source-validation-container");
    divContainer.setAttribute("id", "data-source-validation-container");
    let validationAccordion = document.createElement("div");
    validationAccordion.setAttribute("class", "accordion m-2");
    // add new validations
    let addValidationButton = createButton("add-validation-btn", "add-validation", "btn btn-secondary", "+ Validation");
    addValidationButton.addEventListener("click", function () {
        numValidations += 1;
        let newValidation = createValidation(numValidations);
        validationAccordion.append(newValidation);
    });

    divContainer.append(addValidationButton, validationAccordion);
    return divContainer;
}

function updateValidationAccordionHeaderOnInput(validationContainerHeadRow, accordionButton) {
    let defaultAttribute = $(validationContainerHeadRow).find(".default-attribute")[0];
    let defaultValidationInput = $(defaultAttribute).find(".input-field");
    if (defaultValidationInput.length > 0) {
        defaultValidationInput[0].addEventListener("input", (event) => {
            if (accordionButton.innerText.indexOf("-") === -1) {
                accordionButton.innerText = `${accordionButton.innerText} - ${event.target.value}`;
            } else {
                let selectText = accordionButton.innerText.split("-")[0];
                accordionButton.innerText = `${selectText} - ${event.target.value}`;
            }
        });
    }
}

function createValidation(index) {
    let validationContainer = document.createElement("div");
    validationContainer.setAttribute("class", "data-validation-container");
    validationContainer.setAttribute("id", "data-validation-container-" + index);
    let validationContainerHeadRow = document.createElement("div");
    validationContainerHeadRow.setAttribute("class", "row g-1 m-1 align-items-center");
    validationContainer.append(validationContainerHeadRow);

    let validationTypeSelect = createSelect(`validation-type-${index}`, "Type", "form-select input-field data-validation-field validation-type");
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

    let accordionItem = createAccordionItem(`validation-${index}`, `validation-${index}`, "", validationContainer);
    let closeButton = createCloseButton(accordionItem);
    validationContainerHeadRow.append(closeButton, formFloatingType);
    createDataOrValidationTypeAttributes(validationContainerHeadRow, "validation-type");
    // on select change and input of default-attribute, update accordion button
    let accordionButton = $(accordionItem).find(".accordion-button")[0];
    validationTypeSelect.addEventListener("change", (event) => {
        accordionButton.innerText = event.target.value;
        updateValidationAccordionHeaderOnInput(validationContainerHeadRow, accordionButton);
    });
    updateValidationAccordionHeaderOnInput(validationContainerHeadRow, accordionButton);
    return accordionItem;
}

function createRadioButtons(index, name, options) {
    let radioButtonContainer = document.createElement("div");
    radioButtonContainer.setAttribute("id", `${name}-${index}`);
    radioButtonContainer.setAttribute("radioGroup", name);
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

/*
Foreign keys section based off tasks created.
Ability to choose task name and columns. Define custom relationships.
- One to one
- One to many
- Transformations
 */
function createForeignKeys() {
    let foreignKeyContainer = document.createElement("div");
    foreignKeyContainer.setAttribute("class", "foreign-keys-container");
    let foreignKeyAccordion = document.createElement("div");
    foreignKeyAccordion.setAttribute("class", "accordion mt-2");
    let addForeignKeyButton = createButton("add-foreign-key-btn", "add-foreign-key", "btn btn-secondary", "+ Foreign key");
    addForeignKeyButton.addEventListener("click", function () {
        numForeignKeys += 1;
        let newForeignKey = createForeignKey(numForeignKeys);
        foreignKeyAccordion.append(newForeignKey);
    });

    foreignKeyContainer.append(addForeignKeyButton, foreignKeyAccordion);
    return foreignKeyContainer;
}

function createForeignKey(index) {
    let foreignKeyContainer = document.createElement("div");
    foreignKeyContainer.setAttribute("class", "foreign-key-container");
    // main source
    let mainSourceFkHeader = document.createElement("h5");
    mainSourceFkHeader.innerText = "Source";
    let mainSourceForeignKey = document.createElement("div");
    mainSourceForeignKey.setAttribute("class", "foreign-key-main-source");
    let mainForeignKeySource = createForeignKeyInput(index, "foreign-key-source");
    mainSourceForeignKey.append(mainForeignKeySource);
    // links to...
    let linkSourceFkHeader = document.createElement("h5");
    linkSourceFkHeader.innerText = "Links to";
    let linkSourceForeignKeys = document.createElement("div");
    linkSourceForeignKeys.setAttribute("class", "foreign-key-link-sources");
    let addLinkForeignKeyButton = createButton("add-foreign-key-link-btn", "add-link", "btn btn-secondary", "+ Link");
    addLinkForeignKeyButton.addEventListener("click", function () {
        numForeignKeysLinks += 1;
        let newForeignKeyLink = createForeignKeyInput(numForeignKeysLinks, "foreign-key-link");
        linkSourceForeignKeys.insertBefore(newForeignKeyLink, addLinkForeignKeyButton);
    });

    linkSourceForeignKeys.append(addLinkForeignKeyButton);
    numForeignKeysLinks += 1;
    let newForeignKeyLink = createForeignKeyInput(numForeignKeysLinks, "foreign-key-link");

    linkSourceForeignKeys.insertBefore(newForeignKeyLink, addLinkForeignKeyButton);
    let accordionItem = createAccordionItem(`foreign-key-${index}`, `Foreign key ${index}`, "", foreignKeyContainer);
    let closeButton = createCloseButton(accordionItem);
    foreignKeyContainer.append(closeButton, mainSourceFkHeader, mainSourceForeignKey, linkSourceFkHeader, linkSourceForeignKeys);
    return accordionItem;
}

function createForeignKeyInput(index, name) {
    let foreignKey = document.createElement("div");
    foreignKey.setAttribute("class", `row m-1 ${name}-source`);
    // input is task name -> column(s)
    let taskNameSelect = createSelect(`foreign-key-task-name-${index}`, "Task", `form-select input-field ${name}`);
    let taskNameFloating = createFormFloating("Task", taskNameSelect);
    // get the latest list of task names
    taskNameSelect.addEventListener("click", function () {
        taskNameSelect.replaceChildren();
        let taskNames = Array.from(document.querySelectorAll(".task-name-field").values());
        for (const taskName of taskNames) {
            let selectOption = document.createElement("option");
            selectOption.setAttribute("value", taskName.value);
            selectOption.innerText = taskName.value;
            taskNameSelect.append(selectOption);
        }
    });
    taskNameSelect.dispatchEvent(new Event("click"));

    let columnNamesInput = createInput(`${name}-column-${index}`, "Columns", `form-control input-field ${name}`, "text", "");
    columnNamesInput.setAttribute("required", "");
    let columnNameFloating = createFormFloating("Column(s)", columnNamesInput);

    if (name === "foreign-key-link") {
        let closeButton = createCloseButton(foreignKey);
        foreignKey.append(closeButton);
    }
    foreignKey.append(taskNameFloating, columnNameFloating);
    return foreignKey;
}

/*
Configuration for job execution. Includes:
- Flags for enabling/disabling features
- Metadata
- Generation
- Validation
- Folder pathways
- Alerts
- Runtime
 */
function createConfiguration() {
    let configurationContainer = document.createElement("div");
    configurationContainer.setAttribute("class", "accordion mt-2");
    let configIndex = 0;
    for (let [configKey, configOptions] of configurationOptionsMap.entries()) {
        configIndex += 1;
        // create accordion item with configKey header and configOptions as grid content
        let header = configKey.charAt(0).toUpperCase() + configKey.slice(1);
        let configOptionsContainer = document.createElement("div");
        configOptionsContainer.setAttribute("class", "m-1 configuration-options-container");
        for (let [optKey, options] of Object.entries(configOptions)) {
            let newConfigOptionRow = document.createElement("div");
            newConfigOptionRow.setAttribute("class", "row g-1 m-1 align-items-center user-added-attribute");
            let configOptionInput = createConfigurationOption(configKey, optKey, options);
            newConfigOptionRow.append(configOptionInput);
            if (options.help) {
                let formText = createFormText(optKey, options.help, "span");
                formText.setAttribute("class", "col-8");
                newConfigOptionRow.append(formText);
            }
            configOptionsContainer.append(newConfigOptionRow);
        }
        let accordionItem = createAccordionItem(`config-${configIndex}`, header, "", configOptionsContainer);
        configurationContainer.append(accordionItem);
    }
    return configurationContainer;
}

function createConfigurationOption(configKey, config, options) {
    let configLabel = options["paid"] ? `${config}*` : config;
    let colWrapper = document.createElement("div");
    colWrapper.setAttribute("class", "col-4");
    if (options.choice && options.choice.includes("true")) {
        let formSwitch = document.createElement("div");
        formSwitch.setAttribute("class", "form-check form-switch m-1");
        let switchInput = document.createElement("input");
        switchInput.setAttribute("class", "form-check-input input-configuration");
        switchInput.setAttribute("configuration-parent", configKey);
        switchInput.setAttribute("configuration", config);
        switchInput.setAttribute("type", "checkbox");
        switchInput.setAttribute("role", "switch");
        switchInput.setAttribute("id", "switch-" + config);
        if (options.default === "true") {
            switchInput.setAttribute("checked", "");
        }
        let switchLabel = document.createElement("label");
        switchLabel.setAttribute("class", "form-check-label");
        switchLabel.setAttribute("for", "switch-" + config);
        switchLabel.innerText = configLabel;
        formSwitch.append(switchInput, switchLabel);
        colWrapper.append(formSwitch);
    } else if (options.choice) {
        let selectInput = createSelect(`select-${config}`, configLabel, "form-select input-field input-configuration m-1");
        selectInput.setAttribute("configuration-parent", configKey);
        selectInput.setAttribute("configuration", config);
        for (let choice of options.choice) {
            let option = document.createElement("option");
            option.setAttribute("value", choice.toString());
            option.innerText = choice.toString();
            if (choice === options["default"]) {
                option.setAttribute("selected", "");
            }
            selectInput.append(option);
        }
        let formFloatingAttr = createFormFloating(configLabel, selectInput);
        formFloatingAttr.setAttribute("class", "form-floating");
        colWrapper.append(formFloatingAttr);
    } else {
        let formInput = createInput(`config-${config}`, config, "form-control input-field input-configuration", options.type, options["default"]);
        formInput.setAttribute("configuration-parent", configKey);
        formInput.setAttribute("configuration", config);

        for (const [key, value] of Object.entries(options)) {
            if (key !== "default" && key !== "type" && key !== "choice" && key !== "help") {
                formInput.setAttribute(key, value);
            }
        }
        let formFloatingAttr = createFormFloating(configLabel, formInput);
        formFloatingAttr.setAttribute("class", "form-floating");
        colWrapper.append(formFloatingAttr);
    }
    return colWrapper;
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
            let taskName = dataSource.querySelector(".task-name-field");
            currentDataSource["taskName"] = taskName.value;

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

        let foreignKeyContainers = Array.from(document.querySelectorAll(".foreign-key-container").values());
        let mappedForeignKeys = foreignKeyContainers.map(fkContainer => {
            let fkSource = $(fkContainer).find(".foreign-key-main-source");
            let fkSourceDetails = getForeignKeyDetail(fkSource[0]);
            let fkLinks = $(fkContainer).find(".foreign-key-link-source");
            let fkLinkArray = [];
            for (let fkLink of fkLinks) {
                let fkLinkDetails = getForeignKeyDetail(fkLink);
                fkLinkArray.push(fkLinkDetails);
            }
            return {source: fkSourceDetails, links: fkLinkArray};
        });

        let configurationOptionContainers = Array.from(document.querySelectorAll(".configuration-options-container").values());
        let mappedConfiguration = new Map();
        configurationOptionContainers.forEach(configurationOptionContainer => {
            let inputConfigurations = Array.from(configurationOptionContainer.querySelectorAll(".input-configuration").values());
            let baseConfig = inputConfigurations[0].getAttribute("configuration-parent");
            let options = new Map();
            for (let option of inputConfigurations) {
                options.set(option.getAttribute("configuration"), option.value);
            }
            mappedConfiguration.set(baseConfig, options);
        });

        const requestBody = {
            name: planName,
            id: runId,
            dataSources: allUserInputs,
            foreignKeys: mappedForeignKeys,
            configuration: mappedConfiguration
        };
        console.log(JSON.stringify(requestBody));
        console.log(mappedForeignKeys);
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
                console.log(r);
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

function getForeignKeyDetail(element) {
    let taskName = $(element).find("select[aria-label=Task]").val();
    let columns = $(element).find("input[aria-label=Columns]").val();
    return {taskName: taskName, columns: columns};
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
    let button = createButton("", "Close", "btn-close");
    button.setAttribute("data-bs-dismiss", "toast");
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

const wait = function (ms = 1000) {
    return new Promise(resolve => {
        setTimeout(resolve, ms);
    });
};
