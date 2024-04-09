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
    await fetch("http://localhost:9898/connection", {
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
    fetch("http://localhost:9898/connections", {
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
                let accordionItem = createAccordionItem(numExistingConnections, connection.name, "", syntaxHighlight(connection));
                // add in button to delete connection
                let deleteButton = createButton(`connection-delete-${connection.name}`, "Connection delete", "btn btn-danger", "Delete");

                deleteButton.addEventListener("click", async function () {
                    await fetch(`http://localhost:9898/connection/${connection.name}`, {method: "DELETE"});
                    accordionConnections.removeChild(accordionItem);
                    createToast(`${connection.name}`, `Connection ${connection.name} deleted!`, "success");
                });

                let buttonGroup = createButtonGroup(deleteButton);
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

    let dataSourceName = createInput(`data-source-name-${index}`, "Name", "form-control input-field data-source-property", "text", `my-data-source-${index}`);
    let formFloatingName = createFormFloating("Name", dataSourceName);

    let dataSourceSelect = createSelect(`data-source-select-${index}`, "Data source", "selectpicker form-control input-field data-source-property");
    dataSourceSelect.setAttribute("data-live-search", "true");
    dataSourceSelect.setAttribute("title", "Select data source type...");
    dataSourceSelect.setAttribute("data-header", "Select data source type...");

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
    let closeButton = createCloseButton(divContainer);

    createDataSourceOptions(dataSourceSelect);
    colName.append(formFloatingName);
    colSelect.append(dataSourceSelect);
    $(dataSourceSelect).val("").selectpicker();
    if (hr) {
        divContainer.append(hr);
    }
    divContainer.append(colName, colSelect, closeButton);
    return divContainer;
}

function createOptGroup(label) {
    let metadataSourceGroup = document.createElement("optgroup");
    metadataSourceGroup.setAttribute("label", label);
    return metadataSourceGroup;
}
