import {camelize, createAccordionItem} from "./shared.js";

const dataSourcePropertiesMap = new Map();
dataSourcePropertiesMap.set("csv", {
    Name: "CSV",
    Path: "/tmp/generated-data/csv",
});
dataSourcePropertiesMap.set("json", {
    Name: "JSON",
    Path: "/tmp/generated-data/json",
});
dataSourcePropertiesMap.set("cassandra", {
    Name: "Cassandra",
    URL: "localhost:9042",
    User: "cassandra",
    Password: "cassandra",
    Keyspace: "",
    Table: "",
});
dataSourcePropertiesMap.set("postgres", {
    Name: "Postgres",
    URL: "jdbc:postgres://localhost:5432/customer", //later add in prefix text for jdbc URL
    User: "postgres",
    Password: "postgres",
    Schema: "",
    Table: "",
});

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
    let newDataSource = createDataSourceElement(numDataSources);
    dataSourceConfigRow.append(divider, newDataSource);
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
            let label = camelize(inputField.getAttribute("aria-label").toLowerCase());
            if (label === "name") {
                currConnection["name"] = inputField.value;
            } else if (label === "dataSource") {
                currConnection["type"] = inputField.value;
            } else {
                currConnectionOptions[label] = inputField.value;
            }
        }
        currConnection["options"] = currConnectionOptions;
        newConnections.push(currConnection);
    }
    //save data to file(s)
    await fetch("http://localhost:9090/connection/create", {
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
        .then(async r => {
            await getExistingConnections();
        });
});

//existing connection list populated via data from files
async function getExistingConnections() {
    accordionConnections.replaceChildren();
    fetch("http://localhost:9090/connection/saved", {
        method: "GET",
    })
        .then(r => r.json())
        .then(respJson => {
            let connections = respJson.connections;
            for (let connection of connections) {
                numExistingConnections += 1;
                // remove password from connection details
                if (connection.options.password) {
                    connection.options.password = "***";
                }
                let jsonConnection = JSON.stringify(connection);
                let accordionItem = createAccordionItem(numExistingConnections, connection.name, jsonConnection);
                accordionConnections.append(accordionItem);
            }
        });
}

function createDataSourceOptions(element) {
    element.addEventListener("change", function () {
        let dataSourceProps = dataSourcePropertiesMap.get(this.value);
        if (dataSourceProps && dataSourceProps !== "") {
            let currentDataSourceIndexRow = this.parentElement.parentElement.parentElement;
            let dataSourceOptionsRow = document.createElement("div");
            dataSourceOptionsRow.setAttribute("class", "row mt-2 mb-3");
            dataSourceOptionsRow.setAttribute("id", "connection-row");
            let existingOptions = currentDataSourceIndexRow.querySelector("#connection-row");
            if (existingOptions) {
                currentDataSourceIndexRow.removeChild(existingOptions);
            }

            for (const [key, value] of Object.entries(dataSourceProps)) {
                let optionCol = document.createElement("div");
                optionCol.setAttribute("class", "col-md-auto");
                optionCol.setAttribute("id", key);
                if (key !== "Name") {
                    let formFloating = document.createElement("div");
                    formFloating.setAttribute("class", "form-floating");
                    let label = document.createElement("label");
                    label.setAttribute("for", key);
                    label.innerText = key;
                    let newElement = document.createElement("input");
                    newElement.setAttribute("id", key);
                    newElement.setAttribute("aria-label", key);
                    newElement.setAttribute("value", value);
                    newElement.setAttribute("placeholder", value);
                    newElement.setAttribute("type", "text");
                    newElement.setAttribute("class", "form-control input-field data-source-property");

                    if (key === "Password") {
                        // add toggle visibility
                        newElement.setAttribute("type", "password");
                        let iconHolder = document.createElement("span");
                        iconHolder.setAttribute("class", "input-group-text");
                        let icon = document.createElement("i");
                        icon.setAttribute("class", "fa fa-eye-slash");
                        icon.setAttribute("style", "width: 20px;");
                        iconHolder.append(icon);
                        iconHolder.addEventListener("click", function () {
                            if (newElement.getAttribute("type") === "password") {
                                newElement.setAttribute("type", "text");
                                icon.setAttribute("class", "fa fa-eye");
                            } else {
                                newElement.setAttribute("type", "password");
                                icon.setAttribute("class", "fa fa-eye-slash");
                            }
                        });
                        let inputGroup = document.createElement("div");
                        inputGroup.setAttribute("class", "input-group");
                        formFloating.append(newElement, label);
                        inputGroup.append(formFloating, iconHolder);
                        optionCol.append(inputGroup);
                    } else {
                        formFloating.append(newElement, label);
                        optionCol.append(formFloating);
                    }
                    dataSourceOptionsRow.append(optionCol);
                    currentDataSourceIndexRow.append(dataSourceOptionsRow);
                }
            }
        }
    });
}

function createDataSourceElement(index) {
    let divContainer = document.createElement("div");
    divContainer.setAttribute("id", "data-source-container-" + index);
    divContainer.setAttribute("class", "row m-1 data-source-container");
    let colName = document.createElement("div");
    colName.setAttribute("class", "col");
    let colSelect = document.createElement("div");
    colSelect.setAttribute("class", "col");

    let dataSourceName = document.createElement("input");
    dataSourceName.setAttribute("id", "data-source-name-" + index);
    dataSourceName.setAttribute("type", "text");
    dataSourceName.setAttribute("aria-label", "Name");
    dataSourceName.setAttribute("placeholder", "my-data-source-" + index);
    dataSourceName.setAttribute("class", "form-control input-field data-source-property");
    dataSourceName.value = "my-data-source-" + index;
    let formFloatingName = document.createElement("div");
    formFloatingName.setAttribute("class", "form-floating");
    let labelName = document.createElement("label");
    labelName.setAttribute("for", "data-source-name-" + index);
    labelName.innerText = "Name";
    formFloatingName.append(dataSourceName, labelName);

    let dataSourceSelect = document.createElement("select");
    dataSourceSelect.setAttribute("id", "data-source-select-" + index);
    dataSourceSelect.setAttribute("class", "form-select input-field data-source-property");
    dataSourceSelect.setAttribute("aria-label", "Data source");
    let formFloatingSelect = document.createElement("div");
    formFloatingSelect.setAttribute("class", "form-floating");
    let labelSelect = document.createElement("label");
    labelSelect.setAttribute("for", "data-source-select-" + index);
    labelSelect.innerText = "Data source";
    formFloatingSelect.append(dataSourceSelect, labelSelect);

    let defaultSelectOption = document.createElement("option");
    defaultSelectOption.setAttribute("value", "");
    defaultSelectOption.setAttribute("selected", "");
    defaultSelectOption.setAttribute("disabled", "");
    defaultSelectOption.innerText = "Select data source type...";
    dataSourceSelect.append(defaultSelectOption);

    for (const key of dataSourcePropertiesMap.keys()) {
        let selectOption = document.createElement("option");
        selectOption.setAttribute("value", key);
        selectOption.innerText = dataSourcePropertiesMap.get(key).Name;
        dataSourceSelect.append(selectOption);
    }

    createDataSourceOptions(dataSourceSelect);
    colName.append(formFloatingName);
    colSelect.append(formFloatingSelect);
    divContainer.append(colName, colSelect);
    return divContainer;
}
