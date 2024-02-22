import {
    camelize,
    createAccordionItem,
    createCloseButton,
    createFormFloating,
    createInput,
    createSelect
} from "./shared.js";

const dataSourcePropertiesMap = new Map();
dataSourcePropertiesMap.set("cassandra", {
    Name: "Cassandra",
    URL: "localhost:9042",
    User: "cassandra",
    Password: "cassandra",
    Keyspace: "",
    Table: "",
});
dataSourcePropertiesMap.set("csv", {
    Name: "CSV",
    Path: "/tmp/generated-data/csv",
});
dataSourcePropertiesMap.set("json", {
    Name: "JSON",
    Path: "/tmp/generated-data/json",
});
dataSourcePropertiesMap.set("orc", {
    Name: "ORC",
    Path: "/tmp/generated-data/orc",
});
dataSourcePropertiesMap.set("parquet", {
    Name: "Parquet",
    Path: "/tmp/generated-data/parquet",
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
            let currentDataSourceIndexRow = $(element).closest(".data-source-container");
            let dataSourceOptionsRow = document.createElement("div");
            dataSourceOptionsRow.setAttribute("class", "row mt-2 mb-3");
            dataSourceOptionsRow.setAttribute("id", "connection-row");
            let existingOptions = currentDataSourceIndexRow.find("#connection-row");
            if (existingOptions.length) {
                currentDataSourceIndexRow[0].removeChild(existingOptions[0]);
            }

            for (const [key, value] of Object.entries(dataSourceProps)) {
                let optionCol = document.createElement("div");
                optionCol.setAttribute("class", "col-md-auto");
                optionCol.setAttribute("id", key);
                if (key !== "Name") {
                    let newElement = createInput(key, key, "form-control input-field data-source-property", "text", value);
                    let formFloating = createFormFloating(key, newElement)

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
                        inputGroup.append(formFloating, iconHolder);
                        optionCol.append(inputGroup);
                    } else {
                        optionCol.append(formFloating);
                    }
                    dataSourceOptionsRow.append(optionCol);
                    currentDataSourceIndexRow.append(dataSourceOptionsRow);
                }
            }
        }
    });
}

function createDataSourceElement(index, hr) {
    let divContainer = document.createElement("div");
    divContainer.setAttribute("id", "data-source-container-" + index);
    divContainer.setAttribute("class", "row m-1 data-source-container");
    let colName = document.createElement("div");
    colName.setAttribute("class", "col");
    let colSelect = document.createElement("div");
    colSelect.setAttribute("class", "col");

    let dataSourceName = createInput(`data-source-name-${index}`, "Name", "form-control input-field data-source-property", "text", `my-data-source-${index}`);
    let formFloatingName = createFormFloating("Name", dataSourceName);

    let dataSourceSelect = createSelect(`data-source-select-${index}`, "Data source", "form-select input-field data-source-property");
    let formFloatingSelect = createFormFloating("Data source", dataSourceSelect);

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
    let closeButton = createCloseButton(divContainer);

    createDataSourceOptions(dataSourceSelect);
    colName.append(formFloatingName);
    colSelect.append(formFloatingSelect);
    if (hr) {
        divContainer.append(hr);
    }
    divContainer.append(closeButton, colName, colSelect);
    return divContainer;
}
