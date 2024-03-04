import {
    addAccordionCloseButton,
    addNewDataTypeAttribute,
    camelize,
    createAccordionItem,
    createButton,
    createDataOrValidationTypeAttributes,
    createFieldValidationCheck,
    createFormFloating,
    createInput,
    createSelect,
    findNextLevelNodesByClass
} from "./shared.js";
import {dataTypeOptionsMap} from "./configuration-data.js";
import {createRecordCount} from "./helper-record-count.js";

export let numFields = 0;

// Schema can be manually created or override automatic config
export function createManualSchema(index, additionalName) {
    let divName = additionalName ? `data-source-schema-container-${additionalName}` : "data-source-schema-container";
    let divContainer = document.createElement("div");
    divContainer.setAttribute("class", divName);
    divContainer.setAttribute("id", divName);
    let schemaAccordion = document.createElement("div");
    schemaAccordion.setAttribute("class", "accordion m-2");
    schemaAccordion.setAttribute("style", "--bs-accordion-active-bg: lavender");
    // add new fields to schema
    let addFieldButton = createButton(`add-field-btn-${index}`, "add-field", "btn btn-secondary", "+ Field");
    addFieldButton.addEventListener("click", function () {
        numFields += 1;
        let newField = createSchemaField(numFields);
        schemaAccordion.append(newField);
        $(".selectpicker").selectpicker();
    });

    divContainer.append(addFieldButton, schemaAccordion);
    if (!additionalName) {
        divContainer.append(createRecordCount(index));
    }
    return divContainer;
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
    for (const field of dataSource.fields) {
        numFields += 1;
        let newField = createSchemaField(numFields);
        $(manualSchema).find(".accordion").first().append(newField);
        $(newField).find("[id^=field-name]").val(field.name)[0].dispatchEvent(new Event("input"));
        console.log($(newField).find("select[class~=field-type]"));
        $(newField).find("select[class~=field-type]").val(field.type).selectpicker("refresh")[0].dispatchEvent(new Event("change"));

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
            for (const nestedField of field.nested.fields) {
                numFields += 1;
                let newFieldBox = createManualSchema(numFields, "struct-schema");
                console.log($(newField).find(".accordion-body"));
                $(newField).find(".accordion-body").append(newFieldBox);
                createGenerationFields(field.nested, newFieldBox);
            }
        }
    }
    let collapseShow = $(document).find(".accordion-button.collapse.show");
    collapseShow.click();
}

export function createGenerationElements(dataSource, newDataSource, numDataSources) {
    if (dataSource.fields && dataSource.fields.length > 0) {
        let manualSchema = createManualSchema(numDataSources);
        let dataSourceGenContainer = $(newDataSource).find("#data-source-generation-config-container");
        dataSourceGenContainer.append(manualSchema);
        $(dataSourceGenContainer).find("#manual-generation-checkbox").prop("checked", true);

        createGenerationFields(dataSource, manualSchema);
    }
    $(".selectpicker").selectpicker();
}

function getGenerationSchema(dataSourceSchemaContainer) {
    let dataSourceFields = findNextLevelNodesByClass($(dataSourceSchemaContainer), ["data-field-container"]);
    // get name, type and options applied to each field
    return dataSourceFields.map(field => {
        // need to only get first level of data-source-fields, get nested fields later
        let fieldAttributes = findNextLevelNodesByClass($(field), "data-source-field", ["data-field-container"]);

        console.log(fieldAttributes);
        return fieldAttributes
            .map(attr => attr.getAttribute("aria-label") ? attr : $(attr).find(".data-source-field")[0])
            .reduce((options, attr) => {
                console.log(attr);
                let label = camelize(attr.getAttribute("aria-label"));
                let fieldValue = attr.value;
                if (label === "type" && fieldValue === "struct") {
                    // nested fields can be defined
                    let innerStructSchema = field.querySelector(".data-source-schema-container-struct-schema");
                    console.log(innerStructSchema);
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
    // get top level fields
    let dataSourceSchemaContainer = dataSource.querySelector(".data-source-schema-container");
    let dataFieldsWithAttributes = getGenerationSchema(dataSourceSchemaContainer);
    currentDataSource["fields"] = Object.values(dataFieldsWithAttributes);
}