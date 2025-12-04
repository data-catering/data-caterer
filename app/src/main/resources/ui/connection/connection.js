import {
    addNewDataTypeAttribute,
    createAccordionItem,
    createButton,
    createButtonGroup,
    createCloseButton,
    createFormFloating,
    createInput,
    createSelect,
    createToast,
    syntaxHighlight
} from "../shared.js";
import {dataSourcePropertiesMap} from "../configuration-data.js";
import {apiFetch} from "../config.js";

/**
 * Test a connection and display the result
 * @param {Object} connection - The connection object to test
 * @param {HTMLElement} button - The button element that was clicked (to show loading state)
 */
async function testConnection(connection, button) {
    const originalText = button.innerText;
    button.innerText = "Testing...";
    button.disabled = true;
    
    try {
        const response = await apiFetch("/connection/test", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify(connection)
        });
        
        const result = await response.json();
        // Limit to 100 characters, show ellipsis if truncated
        const resultDetails = result.details ? ` - ${truncateString(result.details, 100)}` : "";
        
        if (result.success) {
            createToast(
                `Test: ${connection.name}`,
                `${result.message}${resultDetails}`,
                "success"
            );
        } else {
            createToast(
                `Test: ${connection.name}`,
                `${result.message}${resultDetails}`,
                "fail"
            );
        }
    } catch (err) {
        // Limit to 100 characters, show ellipsis if truncated
        const resultDetails = err.message ? ` - ${truncateString(err.message, 100)}` : "";
        createToast(
            `Test: ${connection.name}`,
            `Connection test failed: ${resultDetails}`,
            "fail"
        );
    } finally {
        button.innerText = originalText;
        button.disabled = false;
    }
}

/**
 * Test an existing saved connection by name
 * @param {string} connectionName - The name of the saved connection to test
 * @param {HTMLElement} button - The button element that was clicked (to show loading state)
 */
async function testExistingConnection(connectionName, button) {
    const originalText = button.innerText;
    button.innerText = "Testing...";
    button.disabled = true;
    
    try {
        const response = await apiFetch(`/connection/${connectionName}/test`, {
            method: "POST"
        });
        
        const result = await response.json();
        // Limit to 100 characters, show ellipsis if truncated
        const resultDetails = result.details ? ` - ${truncateString(result.details, 100)}` : "";
        if (result.success) {
            createToast(
                `Test: ${connectionName}`,
                `${result.message}${resultDetails}`,
                "success"
            );
        } else {
            createToast(
                `Test: ${connectionName}`,
                `${result.message}${resultDetails}`,
                "fail"
            );
        }
    } catch (err) {
        // Limit to 100 characters, show ellipsis if truncated
        const resultDetails = err.message ? ` - ${truncateString(err.message, 100)}` : "";
        createToast(
            `Test: ${connectionName}`,
            `Connection test failed: ${resultDetails}`,
            "fail"
        );
    } finally {
        button.innerText = originalText;
        button.disabled = false;
    }
}

function truncateString(str, maxLength) {
    return str.length > maxLength ? str.substring(0, maxLength) + "..." : str;
}

/**
 * Build a connection object from a data source container element
 * @param {HTMLElement} container - The data source container element
 * @returns {Object} The connection object
 */
function buildConnectionFromContainer(container) {
    let connection = {};
    let connectionOptions = {};
    let inputFields = Array.from(container.querySelectorAll(".input-field").values());
    
    for (let inputField of inputFields) {
        let ariaLabel = inputField.getAttribute("aria-label");
        if (ariaLabel) {
            if (ariaLabel === "Name") {
                connection["name"] = inputField.value || "test-connection";
            } else if (ariaLabel === "Data source") {
                connection["type"] = inputField.value;
            } else {
                if (inputField.value !== "") {
                    connectionOptions[ariaLabel] = inputField.value;
                }
            }
        }
    }
    connection["options"] = connectionOptions;
    return connection;
}

const addDataSourceButton = document.getElementById("add-data-source-button");
const dataSourceConfigRow = document.getElementById("add-data-source-config-row");
const submitConnectionButton = document.getElementById("submit-connection");
const accordionConnections = document.getElementById("connections-list");
let numDataSources = 1;
let numExistingConnections = 0;

dataSourceConfigRow.append(createDataSourceElement(numDataSources));
addDataSourceButton.addEventListener("click", function () {
    numDataSources += 1;
    let divider = document.createElement("hr");
    let newDataSource = createDataSourceElement(numDataSources, divider);
    dataSourceConfigRow.append(newDataSource);
});

getExistingConnections();

submitConnectionButton.addEventListener("click", async function () {
    //get all the connections created in add-data-source-config-row
    let newConnections = [];
    const allDataSourceContainers = Array.from(dataSourceConfigRow.querySelectorAll(".data-source-container").values());
    for (let container of allDataSourceContainers) {
        let currConnection = {};
        let currConnectionOptions = {};
        let inputFields = Array.from(container.querySelectorAll(".input-field").values());
        for (let inputField of inputFields) {
            let ariaLabel = inputField.getAttribute("aria-label");
            if (ariaLabel) {
                if (ariaLabel === "Name") {
                    currConnection["name"] = inputField.value;
                } else if (ariaLabel === "Data source") {
                    currConnection["type"] = inputField.value;
                } else {
                    if (inputField.value !== "") {
                        currConnectionOptions[ariaLabel] = inputField.value;
                    }
                }
            }
        }
        currConnection["options"] = currConnectionOptions;
        newConnections.push(currConnection);
    }
    //save data to file(s)
    await apiFetch("/connection", {
        method: "POST",
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({"connections": newConnections})
    })
        .catch(err => {
            console.error(err);
            alert(err);
        })
        .then(r => {
            if (r.ok) {
                return r.text().then(text => {
                    createToast(`Save connection(s)`, `Successfully saved connection(s): ${text}`, "success");
                });
            } else {
                r.text().then(text => {
                    createToast(`Save connection(s)`, `Failed to save connection(s)! Error: ${text}`, "fail");
                    throw new Error(text);
                });
            }
        })
        .then(async r => {
            await getExistingConnections();
        });
});

//existing connection list populated via data from files
//TODO allow for Slack and metadata source connections to be created here
async function getExistingConnections() {
    accordionConnections.replaceChildren();
    apiFetch("/connections", {
        method: "GET",
    })
        .then(r => {
            if (r.ok) {
                return r.json();
            } else {
                r.text().then(text => {
                    createToast(`Get connections`, `Failed to get connections! Error: ${text}`, "fail");
                    throw new Error(text);
                });
            }
        })
        .then(respJson => {
            let connections = respJson.connections;
            for (let connection of connections) {
                numExistingConnections += 1;
                
                // Check if connection is from config (default connection) or file (user-created)
                let isFromConfig = connection.source === "config";
                let sourceLabel = isFromConfig ? " (default)" : "";
                let accordionItem = createAccordionItem(numExistingConnections, connection.name + sourceLabel, "", syntaxHighlight(connection));
                
                // Add test connection button
                let testButton = createButton(`connection-test-${connection.name}`, "Test connection", "btn btn-info me-2", "Test");
                testButton.setAttribute("title", "Test this connection");
                testButton.addEventListener("click", async function () {
                    await testExistingConnection(connection.name, testButton);
                });
                
                // Add delete connection button
                let deleteButton = createButton(`connection-delete-${connection.name}`, "Connection delete", "btn btn-danger", "Delete");
                
                // Disable delete button for config-based connections
                if (isFromConfig) {
                    deleteButton.setAttribute("disabled", "true");
                    deleteButton.setAttribute("title", "Default connections from application.conf cannot be deleted via UI");
                    deleteButton.classList.add("btn-secondary");
                    deleteButton.classList.remove("btn-danger");
                }

                deleteButton.addEventListener("click", async function () {
                    if (isFromConfig) {
                        createToast(`${connection.name}`, `Cannot delete default connection. Modify application.conf or environment variables to remove.`, "fail");
                        return;
                    }
                    
                    try {
                        const response = await apiFetch(`/connection/${connection.name}`, {method: "DELETE"});
                        if (response.ok) {
                            accordionConnections.removeChild(accordionItem);
                            createToast(`${connection.name}`, `Connection ${connection.name} deleted!`, "success");
                        } else {
                            const errorText = await response.text();
                            createToast(`${connection.name}`, `Failed to delete connection: ${errorText}`, "fail");
                        }
                    } catch (err) {
                        createToast(`${connection.name}`, `Failed to delete connection: ${err.message}`, "fail");
                    }
                });

                let buttonGroup = createButtonGroup(testButton, deleteButton);
                let header = accordionItem.querySelector(".accordion-header");
                let divContainer = document.createElement("div");
                divContainer.setAttribute("class", "d-flex align-items-center");
                divContainer.append(header.firstChild, buttonGroup);
                header.replaceChildren(divContainer);
                accordionConnections.append(accordionItem);
            }
        });
}

function createDataSourceOptions(element) {
    element.addEventListener("change", function () {
        let dataSourceProps = dataSourcePropertiesMap.get(this.value);
        if (dataSourceProps && dataSourceProps !== "") {
            let currentDataSourceIndexRow = $(element).closest(".data-source-container");
            currentDataSourceIndexRow.find(".row").remove();

            for (const [key, value] of Object.entries(dataSourceProps.properties)) {
                addNewDataTypeAttribute(key, value, `connection-config-${numExistingConnections}-${key}`, "data-source-property", currentDataSourceIndexRow);
            }
        }
    });
}


function createDataSourceElement(index, hr) {
    let divContainer = document.createElement("div");
    divContainer.setAttribute("id", "data-source-container-" + index);
    divContainer.setAttribute("class", "row m-1 data-source-container align-items-center");
    let colName = document.createElement("div");
    colName.setAttribute("class", "col");
    let colSelect = document.createElement("div");
    colSelect.setAttribute("class", "col");
    let colTestButton = document.createElement("div");
    colTestButton.setAttribute("class", "col-auto");

    let dataSourceName = createInput(`data-source-name-${index}`, "Name", "form-control input-field data-source-property", "text", `my-data-source-${index}`);
    let formFloatingName = createFormFloating("Name", dataSourceName);

    let dataSourceSelect = createSelect(`data-source-select-${index}`, "Data source", "selectpicker form-control input-field data-source-property", "Select data source type...");
    dataSourceSelect.setAttribute("data-live-search", "true");

    let dataSourceGroup = createOptGroup("Data Source");
    let metadataSourceGroup = createOptGroup("Metadata Source");
    let alertGroup = createOptGroup("Alert");
    if (dataSourceSelect.childElementCount === 0) {
        dataSourceSelect.append(dataSourceGroup, metadataSourceGroup, alertGroup);

        for (const key of dataSourcePropertiesMap.keys()) {
            let optionProps = dataSourcePropertiesMap.get(key);
            let selectOption = document.createElement("option");
            selectOption.setAttribute("value", key);
            selectOption.innerText = optionProps.Name;
            if (optionProps.disabled === "") {
                selectOption.setAttribute("disabled", "");
            }

            let optGroupLabel = optionProps.optGroupLabel;
            if (optGroupLabel === "Data Source") {
                dataSourceGroup.append(selectOption);
            } else if (optGroupLabel === "Metadata Source") {
                metadataSourceGroup.append(selectOption);
            } else if (optGroupLabel === "Alert") {
                alertGroup.append(selectOption);
            }
        }
    }
    
    // Create test connection button for new connections
    let testButton = createButton(`data-source-test-${index}`, "Test connection", "btn btn-info", "Test Connection");
    testButton.setAttribute("title", "Test this connection before saving");
    testButton.addEventListener("click", async function () {
        const connection = buildConnectionFromContainer(divContainer);
        if (!connection.type) {
            createToast("Test Connection", "Please select a data source type first", "fail");
            return;
        }
        await testConnection(connection, testButton);
    });
    colTestButton.append(testButton);
    
    let closeButton = createCloseButton(divContainer);

    createDataSourceOptions(dataSourceSelect);
    colName.append(formFloatingName);
    colSelect.append(dataSourceSelect);
    $(dataSourceSelect).val("").selectpicker();
    if (hr) {
        divContainer.append(hr);
    }
    divContainer.append(colName, colSelect, colTestButton, closeButton);
    return divContainer;
}

function createOptGroup(label) {
    let metadataSourceGroup = document.createElement("optgroup");
    metadataSourceGroup.setAttribute("label", label);
    return metadataSourceGroup;
}
