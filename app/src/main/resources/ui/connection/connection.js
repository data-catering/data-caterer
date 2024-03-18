import {
    camelize,
    createAccordionItem, createButton, createButtonGroup,
    createCloseButton,
    createFormFloating,
    createInput,
    createSelect, createToast
} from "../shared.js";

const dataSourcePropertiesMap = new Map();
// Data Source
dataSourcePropertiesMap.set("cassandra", {
    optGroupLabel: "Data Source",
    Name: "Cassandra",
    URL: "localhost:9042",
    User: "cassandra",
    Password: "cassandra",
    Keyspace: "",
    Table: "",
});
dataSourcePropertiesMap.set("csv", {
    optGroupLabel: "Data Source",
    Name: "CSV",
    Path: "/tmp/generated-data/csv",
});
dataSourcePropertiesMap.set("http", {
    optGroupLabel: "Data Source",
    Name: "HTTP",
    Username: "",
    Password: "",
});
dataSourcePropertiesMap.set("json", {
    optGroupLabel: "Data Source",
    Name: "JSON",
    Path: "/tmp/generated-data/json",
});
dataSourcePropertiesMap.set("kafka", {
    optGroupLabel: "Data Source",
    Name: "Kafka",
    URL: "localhost:9092",
    Topic: "",
});
dataSourcePropertiesMap.set("mysql", {
    optGroupLabel: "Data Source",
    Name: "MySQL",
    URL: "jdbc:mysql://localhost:3306/customer",
    User: "root",
    Password: "root",
    Schema: "",
    Table: "",
});
dataSourcePropertiesMap.set("orc", {
    optGroupLabel: "Data Source",
    Name: "ORC",
    Path: "/tmp/generated-data/orc",
});
dataSourcePropertiesMap.set("parquet", {
    optGroupLabel: "Data Source",
    Name: "Parquet",
    Path: "/tmp/generated-data/parquet",
});
dataSourcePropertiesMap.set("postgres", {
    optGroupLabel: "Data Source",
    Name: "Postgres",
    URL: "jdbc:postgres://localhost:5432/customer", //later add in prefix text for jdbc URL
    User: "postgres",
    Password: "postgres",
    Schema: "",
    Table: "",
});
dataSourcePropertiesMap.set("solace", {
    optGroupLabel: "Data Source",
    Name: "Solace",
    URL: "smf://host.docker.internal:55554",
    Destination: "/JNDI/Q/test_queue",
});

// Metadata Source
dataSourcePropertiesMap.set("marquez", {
    optGroupLabel: "Metadata Source",
    Name: "Marquez",
    URL: "http://localhost:5001",
    Namespace: "",
    Dataset: "",
});
dataSourcePropertiesMap.set("openapi", {
    optGroupLabel: "Metadata Source",
    Name: "OpenAPI/Swagger",
    Path: "",
});
dataSourcePropertiesMap.set("openmetadata", {
    optGroupLabel: "Metadata Source",
    Name: "Open Metadata",
    URL: "http://localhost:8585/api",
    AuthType: "openmetadata",
    JWT: "",
    TableFQN: "",
});

// Alert
dataSourcePropertiesMap.set("slack", {
    optGroupLabel: "Alert",
    Name: "Slack",
    Token: "",
    Channels: "",
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
            let ariaLabel = inputField.getAttribute("aria-label");
            if (ariaLabel) {
                let label = camelize(ariaLabel.toLowerCase());
                if (label === "name") {
                    currConnection["name"] = inputField.value;
                } else if (label === "dataSource") {
                    currConnection["type"] = inputField.value;
                } else {
                    currConnectionOptions[label] = inputField.value;
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
                return r.text();
            } else {
                r.text().then(text => {
                    new bootstrap.Toast(
                        createToast(`Save connection(s)`, `Failed to save connection(s)! Error: ${text}`, "fail")
                    ).show();
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
                    new bootstrap.Toast(
                        createToast(`Get connections`, `Failed to get connections! Error: ${text}`, "fail")
                    ).show();
                    throw new Error(text);
                });
            }
        })
        .then(respJson => {
            let connections = respJson.connections;
            for (let connection of connections) {
                numExistingConnections += 1;
                let jsonConnection = JSON.stringify(connection);
                let accordionItem = createAccordionItem(numExistingConnections, connection.name, jsonConnection);
                // add in button to delete connection
                let deleteButton = createButton(`connection-delete-${connection.name}`, "Connection delete", "btn btn-danger", "Delete");

                deleteButton.addEventListener("click", async function () {
                    await fetch(`http://localhost:9898/connection/${connection.name}`, {method: "DELETE"});
                    accordionConnections.removeChild(accordionItem);
                    createToast(`${connection.name}`, `Connection ${connection.name} deleted!`);
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
                if (key !== "Name" && key !== "optGroupLabel") {
                    let newElement = createInput(key, key, "form-control input-field data-source-property", "text", value);
                    let formFloating = createFormFloating(key, newElement);

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
                        inputGroup.append(formFloating.firstElementChild, iconHolder);
                        optionCol.append(inputGroup);
                    } else {
                        optionCol = formFloating;
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
            let selectOption = document.createElement("option");
            selectOption.setAttribute("value", key);
            selectOption.innerText = dataSourcePropertiesMap.get(key).Name;
            let optGroupLabel = dataSourcePropertiesMap.get(key).optGroupLabel;
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
    divContainer.append(closeButton, colName, colSelect);
    return divContainer;
}

function createOptGroup(label) {
    let metadataSourceGroup = document.createElement("optgroup");
    metadataSourceGroup.setAttribute("label", label);
    return metadataSourceGroup;
}
