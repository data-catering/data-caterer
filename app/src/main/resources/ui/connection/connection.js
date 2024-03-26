import {
    addNewDataTypeAttribute,
    camelize,
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

const dataSourcePropertiesMap = new Map();
// Data Source
dataSourcePropertiesMap.set("cassandra", {
    optGroupLabel: "Data Source",
    Name: "Cassandra",
    properties: {
        url: {
            displayName: "URL",
            default: "localhost:9042",
            type: "text",
            help: "Hostname and port to connect to Cassandra.",
            required: ""
        },
        username: {
            displayName: "Username",
            default: "cassandra",
            type: "text",
            help: "Username to connect to Cassandra.",
            required: ""
        },
        password: {
            displayName: "Password",
            default: "cassandra",
            type: "password",
            help: "Password to connect to Cassandra.",
            required: ""
        },
        keyspace: {
            displayName: "Keyspace",
            default: "",
            type: "text",
            help: "Keyspace to generate/validate data to/from."
        },
        table: {
            displayName: "Table",
            default: "",
            type: "text",
            help: "Table to generate/validate data to/from."
        }
    }
});
dataSourcePropertiesMap.set("csv", {
    optGroupLabel: "Data Source",
    Name: "CSV",
    properties: {
        path: {
            displayName: "Path",
            default: "/tmp/generated-data/csv",
            type: "text",
            help: "File pathway to save CSV.",
            required: ""
        },
        partitions: {
            displayName: "Num Partitions",
            default: "1",
            type: "number",
            help: "Number of file partitions."
        },
        partitionBy: {
            displayName: "Partition By",
            default: "",
            type: "text",
            help: "Column name(s) to partition by (comma separated)."
        }
    }
});
dataSourcePropertiesMap.set("http", {
    optGroupLabel: "Data Source",
    Name: "HTTP",
    disabled: "",
    properties: {
        username: {
            displayName: "Username",
            default: "",
            type: "text",
            help: "Username to connect to HTTP API."
        },
        password: {
            displayName: "Password",
            default: "",
            type: "password",
            help: "Password to connect to HTTP API."
        },
    }
});
dataSourcePropertiesMap.set("json", {
    optGroupLabel: "Data Source",
    Name: "JSON",
    properties: {
        path: {
            displayName: "Path",
            default: "/tmp/generated-data/json",
            type: "text",
            help: "File pathway to save JSON.",
            required: ""
        },
        partitions: {
            displayName: "Num Partitions",
            default: "1",
            type: "number",
            help: "Number of file partitions."
        },
        partitionBy: {
            displayName: "Partition By",
            default: "",
            type: "text",
            help: "Column name(s) to partition by (comma separated)."
        }
    }
});
dataSourcePropertiesMap.set("kafka", {
    optGroupLabel: "Data Source",
    Name: "Kafka",
    disabled: "",
    properties: {
        url: {
            displayName: "URL",
            default: "localhost:9092",
            type: "text",
            help: "Bootstrap URL to connect to Kafka.",
            required: ""
        },
        topic: {
            displayName: "Topic",
            default: "",
            type: "text",
            help: "Topic to generate/validate data to/from.",
            required: ""
        },
    }
});
dataSourcePropertiesMap.set("mysql", {
    optGroupLabel: "Data Source",
    Name: "MySQL",
    properties: {
        url: {
            displayName: "URL",
            default: "jdbc:mysql://localhost:3306/customer",
            type: "text",
            help: "URL to connect to MySQL.",
            required: ""
        },
        username: {
            displayName: "Username",
            default: "root",
            type: "text",
            help: "Username to connect to MySQL.",
            required: ""
        },
        password: {
            displayName: "Password",
            default: "root",
            type: "password",
            help: "Password to connect to MySQL.",
            required: ""
        },
        schema: {
            displayName: "Schema",
            default: "",
            type: "text",
            help: "Schema to generate/validate data to/from."
        },
        table: {
            displayName: "Table",
            default: "",
            type: "text",
            help: "Table to generate/validate data to/from."
        }
    }
});
dataSourcePropertiesMap.set("orc", {
    optGroupLabel: "Data Source",
    Name: "ORC",
    properties: {
        path: {
            displayName: "Path",
            default: "/tmp/generated-data/orc",
            type: "text",
            help: "File pathway to save ORC.",
            required: ""
        },
        partitions: {
            displayName: "Num Partitions",
            default: "1",
            type: "number",
            help: "Number of file partitions."
        },
        partitionBy: {
            displayName: "Partition By",
            default: "",
            type: "text",
            help: "Column name(s) to partition by (comma separated)."
        }
    }
});
dataSourcePropertiesMap.set("parquet", {
    optGroupLabel: "Data Source",
    Name: "Parquet",
    properties: {
        path: {
            displayName: "Path",
            default: "/tmp/generated-data/parquet",
            type: "text",
            help: "File pathway to save Parquet.",
            required: ""
        },
        partitions: {
            displayName: "Num Partitions",
            default: "1",
            type: "number",
            help: "Number of file partitions."
        },
        partitionBy: {
            displayName: "Partition By",
            default: "",
            type: "text",
            help: "Column name(s) to partition by (comma separated)."
        }
    }
});
dataSourcePropertiesMap.set("postgres", {
    optGroupLabel: "Data Source",
    Name: "Postgres",
    properties: {
        url: {
            displayName: "URL",
            default: "jdbc:postgres://localhost:5432/customer",
            type: "text",
            help: "URL to connect to Postgres.",
            required: ""
        },
        username: {
            displayName: "Username",
            default: "postgres",
            type: "text",
            help: "Username to connect to Postgres.",
            required: ""
        },
        password: {
            displayName: "Password",
            default: "postgres",
            type: "password",
            help: "Password to connect to Postgres.",
            required: ""
        },
        schema: {
            displayName: "Schema",
            default: "",
            type: "text",
            help: "Schema to generate/validate data to/from."
        },
        table: {
            displayName: "Table",
            default: "",
            type: "text",
            help: "Table to generate/validate data to/from."
        }
    }
});
dataSourcePropertiesMap.set("solace", {
    optGroupLabel: "Data Source",
    Name: "Solace",
    disabled: "",
    properties: {
        url: {
            displayName: "URL",
            default: "smf://host.docker.internal:55554",
            type: "text",
            help: "URL to connect to Solace.",
            required: ""
        },
        destination: {
            displayName: "Destination",
            default: "/JNDI/Q/test_queue",
            type: "text",
            help: "JNDI destination to generate/validate data to/from.",
            required: ""
        }
    }
});

// Metadata Source
dataSourcePropertiesMap.set("marquez", {
    optGroupLabel: "Metadata Source",
    Name: "Marquez",
    disabled: "",
    properties: {
        url: {
            displayName: "URL",
            default: "http://localhost:5001",
            type: "text",
            help: "API URL to connect to Marquez.",
            required: ""
        },
        namespace: {
            displayName: "Namespace",
            default: "",
            type: "text",
            help: "Namespace to gather metadata from."
        },
        dataset: {
            displayName: "Dataset",
            default: "",
            type: "text",
            help: "Dataset to gather metadata from."
        }
    }
});
dataSourcePropertiesMap.set("openapi", {
    optGroupLabel: "Metadata Source",
    Name: "OpenAPI/Swagger",
    disabled: "",
    properties: {
        path: {
            displayName: "Path",
            default: "",
            type: "text",
            help: "Path/URL to gather metadata from.",
            required: ""
        }
    }
});
dataSourcePropertiesMap.set("openmetadata", {
    optGroupLabel: "Metadata Source",
    Name: "Open Metadata",
    disabled: "",
    properties: {
        url: {
            displayName: "URL",
            default: "http://localhost:8585/api",
            type: "text",
            help: "API URL to connect to OpenMetadata.",
            required: ""
        },
        authType: {
            displayName: "Auth Type",
            default: "openmetadata",
            type: "text",
            help: "Authentication mechanism used to connect to OpenMetadata.",
            required: ""
        },
        jwt: {
            displayName: "JWT",
            default: "",
            type: "password",
            help: "JWT token."
        },
        tableFQN: {
            displayName: "Table FQN",
            default: "",
            type: "text",
            help: "Table FQN to gather metadata from."
        }
    }
});

// Alert
dataSourcePropertiesMap.set("slack", {
    optGroupLabel: "Alert",
    Name: "Slack",
    properties: {
        token: {
            displayName: "Token",
            default: "",
            type: "password",
            help: "Token to authenticate with Slack.",
            required: ""
        },
        channels: {
            displayName: "Channels",
            default: "",
            type: "text",
            help: "Channel(s) to send alerts to (comma separated).",
            required: ""
        }
    }
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
                return r.text();
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
