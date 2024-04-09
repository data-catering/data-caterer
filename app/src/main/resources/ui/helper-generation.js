import {
    addAccordionCloseButton,
    addNewDataTypeAttribute,
    camelize,
    createAccordionItem,
    createButton,
    createDataOrValidationTypeAttributes,
    createFieldValidationCheck,
    createFormFloating,
    createIconWithConnectionTooltip,
    createInput,
    createSelect,
    createToast,
    findNextLevelNodesByClass,
    getDataConnectionsAndAddToSelect, wait
} from "./shared.js";
import {dataSourcePropertiesMap, dataTypeOptionsMap} from "./configuration-data.js";

export let numFields = 0;

// Schema can be manually created or override automatic config
export function createManualSchema(index, additionalName) {
    let divName = additionalName ? `data-source-schema-container-${additionalName}` : `data-source-schema-container-${index}`;
    let divContainer = document.createElement("div");
    divContainer.setAttribute("class", `card card-body ${divName}`);
    divContainer.setAttribute("id", divName);
    divContainer.style.display = "inherit";
    let manualSchemaHeader = document.createElement("h5");
    manualSchemaHeader.innerText = "Manual Schema";
    let schemaAccordion = document.createElement("div");
    schemaAccordion.setAttribute("class", "accordion m-2");
    schemaAccordion.setAttribute("style", "--bs-accordion-active-bg: lavender");
    // add new fields to schema
    let addFieldButton = createButton(`add-field-btn-${index}`, "add-field", "btn btn-secondary", "+ Field");
    addFieldButton.addEventListener("click", function () {
        numFields += 1;
        let newField = createSchemaField(numFields);
        schemaAccordion.append(newField);
    });

    divContainer.append(manualSchemaHeader, addFieldButton, schemaAccordion);
    return divContainer;
}

function addMetadataConnectionOverrideOptions(metadataConnectionSelect, overrideOptionsContainer, index) {
    metadataConnectionSelect.addEventListener("change", (event) => {
        let metadataSourceName = event.target.value;
        fetch(`http://localhost:9898/connection/${metadataSourceName}`, {method: "GET"})
            .then(r => {
                if (r.ok) {
                    return r.json();
                } else {
                    r.text().then(text => {
                        createToast(`Get connection ${metadataSourceName}`, `Failed to get connection ${metadataSourceName}! Error: ${err}`, "fail");
                        throw new Error(text);
                    });
                }
            })
            .then(respJson => {
                if (respJson) {
                    //remove previous properties
                    overrideOptionsContainer.replaceChildren();
                    let metadataSourceType = respJson.type;
                    let metadataSourceProperties = dataSourcePropertiesMap.get(metadataSourceType).properties;

                    for (const [key, value] of Object.entries(metadataSourceProperties)) {
                        if (value["override"] && value["override"] === "true") {
                            //add properties that can be overridden
                            addNewDataTypeAttribute(key, value, `connection-config-${index}-${key}`, "metadata-source-property", overrideOptionsContainer);
                        }
                    }
                }
            });
    });
}

// allow users to get schema information from a metadata source such as openmetadata or marquez
export async function createAutoSchema(index) {
    let baseContainer = document.createElement("div");
    baseContainer.setAttribute("id", `data-source-auto-schema-container-${index}`);
    baseContainer.setAttribute("class", "card card-body data-source-auto-schema-container");
    baseContainer.style.display = "inherit";
    let autoSchemaHeader = document.createElement("h5");
    autoSchemaHeader.innerText = "Auto Schema";
    let baseTaskDiv = document.createElement("div");
    baseTaskDiv.setAttribute("class", "row m-2 g-2 align-items-center");

    let metadataConnectionSelect = createSelect(`metadata-connection-${index}`, "Metadata source", "selectpicker form-control input-field metadata-connection-name");
    metadataConnectionSelect.setAttribute("title", "Select metadata source...");
    metadataConnectionSelect.setAttribute("data-header", "Select metadata source...");
    let dataConnectionCol = document.createElement("div");
    dataConnectionCol.setAttribute("class", "col");
    dataConnectionCol.append(metadataConnectionSelect);

    let iconDiv = createIconWithConnectionTooltip(metadataConnectionSelect);
    let iconCol = document.createElement("div");
    iconCol.setAttribute("class", "col-md-auto");
    iconCol.append(iconDiv);
    baseTaskDiv.append(dataConnectionCol, iconCol);

    // get connection list, filter only metadata sources
    let metadataConnection = await getDataConnectionsAndAddToSelect(metadataConnectionSelect, baseTaskDiv, "metadata");
    // provide opportunity to override non-connection options for metadata source (i.e. namespace, dataset)
    let overrideOptionsContainer = document.createElement("div");
    // when the metadata source connection is selected, populate override options container with options with "override: true"
    addMetadataConnectionOverrideOptions(metadataConnectionSelect, overrideOptionsContainer, index);

    baseContainer.append(autoSchemaHeader, metadataConnection, overrideOptionsContainer);
    return baseContainer;
}


export function createSchemaField(index) {
    let fieldContainer = document.createElement("div");
    fieldContainer.setAttribute("class", "row g-3 mb-2 align-items-center");
    fieldContainer.setAttribute("id", "data-field-container-" + index);

    let fieldName = createInput(`field-name-${index}`, "Name", "form-control input-field data-source-field is-invalid", "text", "");
    fieldName.setAttribute("required", "");
    createFieldValidationCheck(fieldName);
    let formFloatingName = createFormFloating("Name", fieldName);

    let fieldTypeSelect = createSelect(`field-type-${index}`, "Type", "selectpicker form-control input-field data-source-field field-type");
    fieldTypeSelect.setAttribute("title", "Select data type...");
    fieldTypeSelect.setAttribute("data-header", "Select data type...");
    let fieldTypeFormGroup = document.createElement("div");
    fieldTypeFormGroup.setAttribute("class", "form-group");
    fieldTypeFormGroup.append(fieldTypeSelect);
    let fieldTypeCol = document.createElement("div");
    fieldTypeCol.setAttribute("class", "col");
    fieldTypeCol.append(fieldTypeFormGroup);

    for (const key of dataTypeOptionsMap.keys()) {
        let selectOption = document.createElement("option");
        selectOption.setAttribute("value", key);
        selectOption.innerText = key;
        fieldTypeSelect.append(selectOption);
    }

    let accordionItem = createAccordionItem(`column-${index}`, `column-${index}`, "", fieldContainer, "show");
    addAccordionCloseButton(accordionItem);
    accordionItem.classList.add("data-field-container");
    // when field name changes, update the accordion header
    fieldName.addEventListener("input", (event) => {
        let accordionButton = $(accordionItem).find(".accordion-button");
        accordionButton[0].innerText = event.target.value;
    });

    fieldContainer.append(formFloatingName, fieldTypeCol);
    $(fieldTypeSelect).selectpicker();
    createDataOrValidationTypeAttributes(fieldContainer, "data-type");
    return accordionItem;
}


function createGenerationFields(dataSource, manualSchema) {
    let allCollapsedAccordionButton = $(document).find(".accordion-button.collapsed");
    allCollapsedAccordionButton.click();
    for (const field of dataSource.fields.optFields) {
        numFields += 1;
        let newField = createSchemaField(numFields);
        $(manualSchema).find(".accordion").first().append(newField);
        $(newField).find("[id^=field-name]").val(field.name)[0].dispatchEvent(new Event("input"));
        $(newField).find("select[class~=field-type]").selectpicker("val", field.type)[0].dispatchEvent(new Event("change"));

        if (field.options) {
            for (const [optKey, optVal] of Object.entries(field.options)) {
                let baseOptions = Object.create(dataTypeOptionsMap.get(field.type)[optKey]);
                baseOptions["default"] = optVal;
                let mainContainer = $(newField).find(".accordion-body")[0];
                addNewDataTypeAttribute(optKey, baseOptions, `data-field-container-${numFields}-${optKey}`, "data-source-field", mainContainer);
                document.getElementById(`data-field-container-${numFields}-${optKey}`).dispatchEvent(new Event("input"));
            }
        }
        // there are nested fields
        if (field.nested && field.nested.fields) {
            let newFieldBox = createManualSchema(numFields, "struct-schema");
            $(newField).find(".accordion-body").append(newFieldBox);
            createGenerationFields(field.nested, newFieldBox);
        }
    }
    let collapseShow = $(document).find(".accordion-button.collapse.show");
    collapseShow.click();
}

async function createAutoGenerationSchema(autoFromMetadataSchema, dataSource) {
    $(autoFromMetadataSchema).find(".metadata-connection-name").selectpicker("val", dataSource.fields.optMetadataSource.name)[0].dispatchEvent(new Event("change"));
    // takes some time to get the override options
    await wait(100);
    for (let [key, value] of Object.entries(dataSource.fields.optMetadataSource.overrideOptions)) {
        $(autoFromMetadataSchema).find(`input[aria-label="${key}"]`).val(value);
    }
}

export async function createGenerationElements(dataSource, newDataSource, numDataSources) {
    let dataSourceGenContainer = $(newDataSource).find("#data-source-generation-config-container");
    // check if there is auto schema defined
    // check if there is auto schema from metadata source defined
    if (dataSource.fields && dataSource.fields.optMetadataSource) {
        $(dataSourceGenContainer).find("[id^=auto-from-metadata-source-generation-checkbox]").prop("checked", true);
        let autoFromMetadataSchema = await createAutoSchema(numDataSources);
        $(dataSourceGenContainer).find(".manual").after(autoFromMetadataSchema);

        await createAutoGenerationSchema(autoFromMetadataSchema, dataSource);
    }
    // check if there is manual schema defined
    if (dataSource.fields && dataSource.fields.optFields && dataSource.fields.optFields.length > 0) {
        let manualSchema = createManualSchema(numDataSources);
        dataSourceGenContainer[0].insertBefore(manualSchema, dataSourceGenContainer[0].lastElementChild);
        $(dataSourceGenContainer).find("[id^=manual-generation-checkbox]").prop("checked", true);

        createGenerationFields(dataSource, manualSchema);
    }
}

function getGenerationSchema(dataSourceSchemaContainer) {
    let dataSourceFields = findNextLevelNodesByClass($(dataSourceSchemaContainer), ["data-field-container"]);
    // get name, type and options applied to each field
    return dataSourceFields.map(field => {
        // need to only get first level of data-source-fields, get nested fields later
        let fieldAttributes = findNextLevelNodesByClass($(field), "data-source-field", ["data-field-container"]);

        return fieldAttributes
            .map(attr => attr.getAttribute("aria-label") ? attr : $(attr).find(".data-source-field")[0])
            .reduce((options, attr) => {
                let label = camelize(attr.getAttribute("aria-label"));
                let fieldValue = attr.value;
                if (label === "type" && fieldValue === "struct") {
                    // nested fields can be defined
                    let innerStructSchema = field.querySelector(".data-source-schema-container-struct-schema");
                    options[label] = fieldValue;
                    options["nested"] = {fields: getGenerationSchema(innerStructSchema)};
                } else if (label === "name" || label === "type") {
                    options[label] = fieldValue;
                } else {
                    let currOpts = (options["options"] || new Map());
                    currOpts.set(label, fieldValue);
                    options["options"] = currOpts;
                }
                return options;
            }, {});
    });
}

export function getGeneration(dataSource, currentDataSource) {
    let dataGenerationInfo = {};
    // check which checkboxes are enabled: auto, auto with external, manual
    let isAutoChecked = $(dataSource).find("[id^=auto-generation-checkbox]").is(":checked");
    let isAutoFromMetadataChecked = $(dataSource).find("[id^=auto-from-metadata-source-generation-checkbox]").is(":checked");
    let isManualChecked = $(dataSource).find("[id^=manual-generation-checkbox]").is(":checked");

    if (isAutoChecked) {
        // need to enable data generation within data source options
        currentDataSource["options"] = {enableDataGeneration: "true"};
    }

    if (isAutoFromMetadataChecked) {
        let dataSourceAutoSchemaContainer = $(dataSource).find("[class~=data-source-auto-schema-container]")[0];
        let metadataConnectionName = $(dataSourceAutoSchemaContainer).find("select[class~=metadata-connection-name]").val();
        let metadataConnectionOptions = $(dataSourceAutoSchemaContainer).find("input[class~=metadata-source-property]").toArray()
            .reduce(function (map, option) {
                if (option.value !== "") {
                    map[option.getAttribute("aria-label")] = option.value;
                }
                return map;
            }, {});
        dataGenerationInfo["optMetadataSource"] = {
            name: metadataConnectionName,
            overrideOptions: metadataConnectionOptions
        };
    }
    // get top level manual fields
    if (isManualChecked) {
        let dataSourceSchemaContainer = $(dataSource).find("[id^=data-source-schema-container]")[0];
        let dataFieldsWithAttributes = getGenerationSchema(dataSourceSchemaContainer);
        dataGenerationInfo["optFields"] = Object.values(dataFieldsWithAttributes);
    }
    currentDataSource["fields"] = dataGenerationInfo;
}