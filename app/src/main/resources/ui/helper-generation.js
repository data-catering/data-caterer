import {
    addNewDataTypeAttribute,
    camelize,
    createAutoFromMetadata,
    createAutoFromMetadataSourceContainer,
    createManualContainer,
    createNewField,
    dispatchEvent,
    findNextLevelNodesByClass
} from "./shared.js";
import {dataTypeOptionsMap} from "./configuration-data.js";

export let numFields = 0;

export function incFields() {
    numFields++;
}

async function createGenerationFields(dataSourceFields, manualSchema) {
    let allCollapsedAccordionButton = $(document).find(".accordion-button.collapsed");
    allCollapsedAccordionButton.click();
    for (const field of dataSourceFields) {
        numFields += 1;
        let newField = await createNewField(numFields, "generation");
        $(manualSchema).find(".accordion").first().append(newField);
        let updatedFieldName = $(newField).find("[id^=field-name]").val(field.name);
        dispatchEvent(updatedFieldName, "input");
        let updatedFieldType = $(newField).find("select[class~=field-type]").selectpicker("val", field.type);
        dispatchEvent(updatedFieldType, "change");

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
        if (field.fields && field.fields.length > 0) {
            let newFieldBox = $(newField).find(".card");
            // let newFieldBox = createManualContainer(numFields, "generation", "struct-schema");
            // $(newField).find(".accordion-body").append(newFieldBox);
            await createGenerationFields(field.fields, newFieldBox);
        }
    }
    let collapseShow = $(document).find(".accordion-button.collapse.show");
    collapseShow.click();
}


export async function createGenerationElements(dataSource, newDataSource, numDataSources) {
    let dataSourceGenContainer = $(newDataSource).find("#data-source-generation-config-container");
    // TODO check if there is auto schema defined
    // check if there is auto schema from metadata source defined
    if (dataSource.options["metadataSourceName"]) {
        $(dataSourceGenContainer).find("[id^=auto-from-metadata-source-generation-checkbox]").prop("checked", true);
        let autoFromMetadataSchema = await createAutoFromMetadataSourceContainer(numDataSources);
        $(dataSourceGenContainer).find(".manual").after(autoFromMetadataSchema);

        await createAutoFromMetadata(autoFromMetadataSchema, dataSource);
    }
    // check if there is manual schema defined
    if (dataSource.fields && dataSource.fields.length > 0) {
        let manualSchema = createManualContainer(numFields, "generation");
        dataSourceGenContainer[0].insertBefore(manualSchema, dataSourceGenContainer[0].lastElementChild);
        $(dataSourceGenContainer).find("[id^=manual-generation-checkbox]").prop("checked", true);

        await createGenerationFields(dataSource.fields, manualSchema);
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
                    options["nested"] = {optFields: getGenerationSchema(innerStructSchema)};
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
        let dataSourceAutoSchemaContainer = $(dataSource).find("[class~=data-source-auto-from-metadata-container]")[0];
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

export function getGenerationYaml(dataSource, currentTask) {
    // check which checkboxes are enabled: auto, auto with external, manual
    let isAutoChecked = $(dataSource).find("[id^=auto-generation-checkbox]").is(":checked");
    let isAutoFromMetadataChecked = $(dataSource).find("[id^=auto-from-metadata-source-generation-checkbox]").is(":checked");
    let isManualChecked = $(dataSource).find("[id^=manual-generation-checkbox]").is(":checked");
    currentTask["options"] = {};

    if (isAutoChecked) {
        // need to enable data generation within data source options
        currentTask["options"]["enableDataGeneration"] = "true";
    }

    if (isAutoFromMetadataChecked) {
        let dataSourceGenerationContainer = $(dataSource).find("[id^=data-source-generation-config-container]")[0];
        let dataSourceAutoSchemaContainer = $(dataSourceGenerationContainer).find("[class~=data-source-auto-from-metadata-container]")[0];
        let metadataConnectionName = $(dataSourceAutoSchemaContainer).find("select[class~=metadata-connection-name]").val();
        $(dataSourceAutoSchemaContainer).find("input[class~=metadata-source-property]").toArray()
            .forEach(opt => {
                if (opt.value !== "") {
                    currentTask["options"][opt.getAttribute("aria-label")] = opt.value;
                }
            });
        currentTask["options"]["metadataSourceName"] = metadataConnectionName;
    }
    // get top level manual fields
    if (isManualChecked) {
        let dataSourceSchemaContainer = $(dataSource).find("[id^=data-source-schema-container]")[0];
        let dataFieldsWithAttributes = getGenerationSchema(dataSourceSchemaContainer);
        currentTask["fields"] = Object.values(dataFieldsWithAttributes);
    }
}