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
    createAccordionItem,
    createCloseButton,
    createFieldValidationCheck,
    createFormFloating,
    createInput,
    createSelect,
    createToast, executePlan, wait
} from "./shared.js";
import {createForeignKeys, createForeignKeysFromPlan, getForeignKeys} from "./helper-foreign-keys.js";
import {
    createConfiguration,
    createConfigurationFromPlan,
    createNewConfigRow,
    getConfiguration
} from "./helper-configuration.js";
import {createGenerationElements, createManualSchema, getGeneration} from "./helper-generation.js";
import {createManualValidation, createValidationFromPlan, getValidations} from "./helper-validation.js";
import {createCountElementsFromPlan, getRecordCount} from "./helper-record-count.js";
import {configurationOptionsMap, reportConfigKeys, reportOptionsMap} from "./configuration-data.js";

const addTaskButton = document.getElementById("add-task-button");
const tasksDiv = document.getElementById("tasks-details-body");
const foreignKeysDiv = document.getElementById("foreign-keys-details-body");
const configurationDiv = document.getElementById("configuration-details-body");
const expandAllButton = document.getElementById("expand-all-button");
const collapseAllButton = document.getElementById("collapse-all-button");
const relationshipExampleSwitch = document.getElementById("showRelationshipExample");
const perColumnExampleSwitch = document.getElementById("showPerColumnExample");
const planName = document.getElementById("plan-name");
let numDataSources = 1;

tasksDiv.append(await createDataSourceForPlan(numDataSources));
foreignKeysDiv.append(createForeignKeys());
configurationDiv.append(createConfiguration());
createFieldValidationCheck(planName);
addTaskButton.addEventListener("click", async function () {
    numDataSources += 1;
    let divider = document.createElement("hr");
    let newDataSource = await createDataSourceForPlan(numDataSources, divider);
    tasksDiv.append(newDataSource);
});
expandAllButton.addEventListener("click", function () {
    $(document).find(".accordion-button.collapsed").click();
});
collapseAllButton.addEventListener("click", function () {
    $(document).find(".accordion-button:not(.collapsed)").click();
});
relationshipExampleSwitch.addEventListener("click", function () {
    let txn1 = document.getElementById("with-relationship-example-txn-1");
    let txn2 = document.getElementById("with-relationship-example-txn-2");
    if (txn1.classList.contains("example-1-enabled")) {
        txn1.innerText = "ACC951";
        txn2.innerText = "ACC159";
        txn1.classList.replace("example-1-enabled", "example-1-disabled");
        txn2.classList.replace("example-2-enabled", "example-2-disabled");
    } else {
        txn1.innerText = "ACC123";
        txn2.innerText = "ACC789";
        txn1.classList.replace("example-1-disabled", "example-1-enabled");
        txn2.classList.replace("example-2-disabled", "example-2-enabled");
    }
});
perColumnExampleSwitch.addEventListener("click", function () {
    let table = $("#with-per-unique-column-values-example-transactions");
    let colIndex = [1, 2, 4, 5];
    if ($(perColumnExampleSwitch).is(":checked")) {
        colIndex.forEach(i => $(table).bootstrapTable("showRow", {index: i}));
    } else {
        colIndex.forEach(i => $(table).bootstrapTable("hideRow", {index: i}));
    }
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

function createIconWithConnectionTooltip(dataConnectionSelect) {
    let iconDiv = document.createElement("i");
    iconDiv.setAttribute("class", "bi bi-info-circle");
    iconDiv.setAttribute("data-bs-toggle", "tooltip");
    iconDiv.setAttribute("data-bs-placement", "top");
    iconDiv.setAttribute("data-bs-container", "body");
    iconDiv.setAttribute("data-bs-html", "true");
    iconDiv.setAttribute("data-bs-title", "Connection options");
    new bootstrap.Tooltip(iconDiv);
    // on select change, update icon title
    dataConnectionSelect.addEventListener("change", (event) => {
        let connectionName = event.target.value;
        fetch(`http://localhost:9898/connection/${connectionName}`, {method: "GET"})
            .then(r => {
                if (r.ok) {
                    return r.json();
                } else {
                    r.text().then(text => {
                        createToast(`Get connection ${connectionName}`, `Failed to get connection ${connectionName}! Error: ${err}`, "fail");
                        throw new Error(text);
                    });
                }
            })
            .then(respJson => {
                if (respJson) {
                    let optionsToShow = {};
                    optionsToShow["type"] = respJson.type;
                    for (let [key, value] of Object.entries(respJson.options)) {
                        if (key !== "user" && key !== "password") {
                            optionsToShow[key] = value;
                        }
                    }
                    let summary = Object.entries(optionsToShow).map(kv => `${kv[0]}: ${kv[1]}`).join("<br>");
                    iconDiv.setAttribute("data-bs-title", summary);
                    new bootstrap.Tooltip(iconDiv);
                }
            });
    });
    return iconDiv;
}

async function createDataConnectionInput(index) {
    let baseTaskDiv = document.createElement("div");
    baseTaskDiv.setAttribute("class", "row g-2 align-items-center");
    let taskNameInput = createInput(`task-name-${index}`, "Task name", "form-control input-field task-name-field", "text", `task-${index}`);
    taskNameInput.setAttribute("required", "");
    createFieldValidationCheck(taskNameInput);
    let taskNameFormFloating = createFormFloating("Task name", taskNameInput);

    let dataConnectionSelect = createSelect(`data-source-connection-${index}`, "Data source", "selectpicker form-control input-field data-connection-name");
    dataConnectionSelect.setAttribute("title", "Select data source...");
    dataConnectionSelect.setAttribute("data-header", "Select data source...");
    let dataConnectionCol = document.createElement("div");
    dataConnectionCol.setAttribute("class", "col");
    dataConnectionCol.append(dataConnectionSelect);

    let iconDiv = createIconWithConnectionTooltip(dataConnectionSelect);
    let iconCol = document.createElement("div");
    iconCol.setAttribute("class", "col-md-auto");
    iconCol.append(iconDiv);

    // let inputGroup = createInputGroup(dataConnectionSelect, iconDiv, "col");
    // $(inputGroup).find(".input-group").addClass("align-items-center");
    baseTaskDiv.append(taskNameFormFloating, dataConnectionCol, iconCol);

    //get list of existing data connections
    await fetch("http://localhost:9898/connections", {method: "GET"})
        .then(r => {
            if (r.ok) {
                return r.json();
            } else {
                r.text().then(text => {
                    createToast("Get connections", `Get connections failed! Error: ${err}`, "fail");
                    throw new Error(text);
                });
            }
        })
        .then(respJson => {
            if (respJson) {
                let connections = respJson.connections;
                for (let connection of connections) {
                    let option = document.createElement("option");
                    option.setAttribute("value", connection.name);
                    option.innerText = connection.name;
                    dataConnectionSelect.append(option);
                }
            }
        });

    // if list of connections is empty, provide button to add new connection
    if (dataConnectionSelect.childElementCount > 0) {
        $(dataConnectionSelect).selectpicker();
        return baseTaskDiv;
    } else {
        let createNewConnection = document.createElement("a");
        createNewConnection.setAttribute("type", "button");
        createNewConnection.setAttribute("class", "btn btn-primary");
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

    dataConfigAccordion.append(dataGenConfigContainer, dataValidConfigContainer);
    if (divider) {
        divContainer.append(divider);
    }
    dataConnectionFormFloating.append(closeButton);
    divContainer.append(dataConnectionFormFloating, dataConfigAccordion);
    return divContainer;
}

function createReportConfiguration() {
    let reportDetailsBody = document.getElementById("report-details-body");
    let configOptionsContainer = document.createElement("div");
    configOptionsContainer.setAttribute("class", "m-1 configuration-options-container");
    reportDetailsBody.append(configOptionsContainer);
    for (let [idx, entry] of Object.entries(reportConfigKeys)) {
        let configRow = createNewConfigRow(entry[0], entry[1], configurationOptionsMap.get(entry[0])[entry[1]]);
        let inputVal = $(configRow).find("input, select")[0];
        if (inputVal) {
            inputVal.id = inputVal.id + "-report";
        }
        configOptionsContainer.append(configRow);
    }
}

createReportConfiguration();
submitForm();
savePlan();
deleteDataRun();

function getPlanDetails(form) {
    let planName = form.querySelector("#plan-name").value;
    let allDataSources = form.querySelectorAll(".data-source-config-container");
    const runId = crypto.randomUUID();
    let allUserInputs = [];
    for (let dataSource of allDataSources) {
        let currentDataSource = {};
        // get data connection name
        currentDataSource["name"] = $(dataSource).find("select[class~=data-connection-name]").val();
        currentDataSource["taskName"] = dataSource.querySelector(".task-name-field").value;

        getGeneration(dataSource, currentDataSource);
        getValidations(dataSource, currentDataSource);
        getRecordCount(dataSource, currentDataSource);
        allUserInputs.push(currentDataSource);
    }

    let mappedForeignKeys = getForeignKeys();
    let mappedConfiguration = getConfiguration();

    const requestBody = {
        name: planName,
        id: runId,
        dataSources: allUserInputs,
        foreignKeys: mappedForeignKeys,
        configuration: mappedConfiguration
    };
    return {planName, runId, requestBody};
}

function checkFormValidity(form) {
    expandAllButton.dispatchEvent(new Event("click"));
    let isValid = form.checkValidity();
    if (isValid) {
        wait(500).then(r => collapseAllButton.dispatchEvent(new Event("click")));
        return true;
    } else {
        form.reportValidity();
        return false;
    }
}

function getPlanDetailsAndRun(form) {
    // collect all the user inputs
    let {planName, runId, requestBody} = getPlanDetails(form);
    console.log(JSON.stringify(requestBody));
    executePlan(requestBody, planName, runId);
}

function submitForm() {
    let form = document.getElementById("plan-form");
    let submitPlanButton = document.getElementById("submit-plan");
    submitPlanButton.addEventListener("click", function () {
        let isValidForm = checkFormValidity(form);
        if (isValidForm) {
            getPlanDetailsAndRun(form);
        }
    });
}

function deleteDataRun() {
    let form = document.getElementById("plan-form");
    let deleteDataButton = document.getElementById("delete-data");
    deleteDataButton.addEventListener("click", function () {
        let isValidForm = checkFormValidity(form);
        if (isValidForm) {
            let {planName, runId, requestBody} = getPlanDetails(form);
            executePlan(requestBody, planName, runId, "delete");
        }
    });
}

function savePlan() {
    let savePlanButton = document.getElementById("save-plan");
    savePlanButton.addEventListener("click", function () {
        let form = document.getElementById("plan-form");
        let {planName, requestBody} = getPlanDetails(form);
        console.log(JSON.stringify(requestBody));
        fetch("http://localhost:9898/plan", {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(requestBody)})
            .catch(err => {
                console.error(err);
                createToast(planName, `Plan save failed! Error: ${err}`, "fail");
            })
            .then(r => {
                if (r.ok) {
                    return r.text();
                } else {
                    r.text().then(text => {
                        createToast(planName, `Plan ${planName} save failed! Error: ${text}`, "fail");
                        throw new Error(text);
                    });
                }
            })
            .then(resp => {
                if (resp.includes("fail")) {
                    createToast(planName, `Plan ${planName} save failed!`, "fail");
                } else {
                    createToast(planName, `Plan ${planName} saved.`, "success");
                }
            })
    });
}

// check if sent over from edit plan with plan-name
const currUrlParams = window.location.search.substring(1);

if (currUrlParams.includes("plan-name=")) {
    // then get the plan details and fill in the form
    let planName = currUrlParams.substring(currUrlParams.indexOf("=") + 1);
    await fetch(`http://localhost:9898/plan/${planName}`, {method: "GET"})
        .then(r => {
            if (r.ok) {
                return r.json();
            } else {
                r.text().then(text => {
                    createToast(planName, `Plan ${planName} failed! Error: ${text}`, "fail");
                    throw new Error(text);
                });
            }
        })
        .then(async respJson => {
            document.getElementById("plan-name").value = planName;
            // clear out default data source
            document.querySelector(".data-source-config-container").remove();
            let tasksDetailsBody = document.getElementById("tasks-details-body");
            for (const dataSource of respJson.dataSources) {
                numDataSources += 1;
                let newDataSource = await createDataSourceForPlan(numDataSources);
                tasksDetailsBody.append(newDataSource);
                $(newDataSource).find(".task-name-field").val(dataSource.taskName);
                $(newDataSource).find(".data-connection-name").selectpicker("val", dataSource.name)[0].dispatchEvent(new Event("change"));

                createGenerationElements(dataSource, newDataSource, numDataSources);
                createCountElementsFromPlan(dataSource, newDataSource);
                await createValidationFromPlan(dataSource, newDataSource);
            }
            await createForeignKeysFromPlan(respJson);
            createConfigurationFromPlan(respJson);
            wait(500).then(r => $(document).find('button[aria-controls="report-body"]:not(.collapsed),button[aria-controls="configuration-body"]:not(.collapsed)').click());
        });
}
