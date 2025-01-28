/*
Different types of validation:
- Basic field
- Dataset (field names, row count)
- Group by/aggregate
- Upstream
- External source (great expectations)
 */
import {
    addItemsToAttributeMenu,
    addNewDataTypeAttribute,
    camelize,
    createAutoFromMetadata,
    createAutoFromMetadataSourceContainer,
    createButtonWithMenu,
    createCloseButton,
    createManualContainer,
    createNewField,
    dispatchEvent,
    findNextLevelNodesByClass,
} from "./shared.js";
import {validationTypeOptionsMap} from "./configuration-data.js";

export let numValidations = 0;

export function incValidations() {
    numValidations++;
}

function createGroupByValidationFromPlan(newValidation, validationOpts, validation) {
    let updatedGroupByCols = $(newValidation).find("[aria-label=GroupByFields]").val(validation.groupByFields)
    dispatchEvent(updatedGroupByCols, "input");
    // can be nested validations

    let dataValidationContainer = $(newValidation).find("[id^=data-validation-container]")[0];
    let metadata = Object.create(validationTypeOptionsMap.get("groupBy")[validation.aggType]);
    metadata["default"] = validation.aggField;
    addNewDataTypeAttribute(validation.aggType, metadata, `groupBy-validation-${numValidations}`, "data-validation-field", dataValidationContainer);
    let aggregationRow = $(dataValidationContainer).find(".data-source-validation-container-nested-validation").last().find(".row").first();

    addFieldValidations(validation, aggregationRow);
}

function createNewValidateAttribute(optKey, validationType, optVal, checked, mainContainer) {
    numValidations += 1;
    let baseOptions = Object.create(validationTypeOptionsMap.get(validationType)[optKey]);
    if (checked) baseOptions.group.checked = "true";
    if (optVal) baseOptions["default"] = optVal;
    addNewDataTypeAttribute(optKey, baseOptions, `data-validation-container-${numValidations}-${optKey}`, "data-validation-field", mainContainer);
    document.getElementById(`data-validation-container-${numValidations}-${optKey}`).dispatchEvent(new Event("input"));
}

async function createValidationsFromDataSource(validations, validationOpts, manualValidation) {
    for (const validation of validations) {
        numValidations += 1;
        let newValidation = await createNewField(numValidations, "validation");
        $(manualValidation).children(".accordion").append(newValidation);
        let mainContainer = $(newValidation).find("[id^=data-validation-container]")[0];

        if (validation.field && validation.validation) {
            let updatedValidationType = $(newValidation).find("select[class~=validation-type]").selectpicker("val", "field");
            dispatchEvent(updatedValidationType, "change");
            let updatedValidationCol = $(newValidation).find("[aria-label=Field]").val(validation.field);
            dispatchEvent(updatedValidationCol, "input");
            addFieldValidations(validation, mainContainer);
        } else if (validation.groupByFields && validation.validation) {
            let updatedValidationType = $(newValidation).find("select[class~=validation-type]").selectpicker("val", "groupBy");
            dispatchEvent(updatedValidationType, "change");
            createGroupByValidationFromPlan(newValidation, validationOpts, validation);
        } else if (validation.upstreamTaskName && validation.validation) {
            let updatedValidationType = $(newValidation).find("select[class~=validation-type]").selectpicker("val", "upstream");
            dispatchEvent(updatedValidationType, "change");
            let updatedUpstreamTaskName = $(newValidation).find("[aria-label=UpstreamTaskName]").val(validation.upstreamTaskName);
            dispatchEvent(updatedUpstreamTaskName, "input");
            //update joinFields, joinType or joinEpr
            createNewValidateAttribute("joinFields", "upstream", validation.joinFields, false, mainContainer);
            createNewValidateAttribute("joinType", "upstream", validation.joinType, false, mainContainer);
            if (validation.joinExpr) {
                createNewValidateAttribute("joinExpr", "upstream", validation.joinExpr, false, mainContainer);
            }

            if (validation.validation && validation.validation.length > 0) {
                let nestedManualValidation = $(newValidation).find(".data-source-validation-container-nested-validation").first();
                await createValidationsFromDataSource(validation.validation, validationOpts, nestedManualValidation);
            }
        }
        //otherwise it is field name validation which doesn't have any default options
    }
}

export async function createValidationFromPlan(dataSource, newDataSource, numDataSources) {
    let dataSourceValidationContainer = $(newDataSource).find("#data-source-validation-config-container");

    if (dataSource.validations && dataSource.options["metadataSourceName"]) {
        $(dataSourceValidationContainer).find("[id^=auto-from-metadata-source-validation-checkbox]").prop("checked", true);
        let autoFromMetadataSchema = await createAutoFromMetadataSourceContainer(numDataSources);
        $(dataSourceValidationContainer).find(".manual").after(autoFromMetadataSchema);

        await createAutoFromMetadata(autoFromMetadataSchema, dataSource);
    }

    if (dataSource.validations && dataSource.validations.length > 0) {
        let manualValidation = createManualContainer(numValidations, "validation");
        let dataSourceGenContainer = $(newDataSource).find("#data-source-validation-config-container");
        dataSourceGenContainer.append(manualValidation);
        $(dataSourceGenContainer).find("[id^=manual-validation-checkbox]").prop("checked", true);

        await createValidationsFromDataSource(dataSource.validations, dataSource.options, manualValidation);
    }
}

function addFieldValidations(validation, container) {
    if (validation.validation && validation.validation.length > 0) {
        for (const valid of validation.validation) {
            let key = valid.type;
            let value = valid.value;
            let checked = false;
            if (valid.negate) {
                checked = true;
            } else if (!valid.strictly) {
                checked = true;
            }
            createNewValidateAttribute(key, "field", value, checked, container);
        }
    }
}

function getValidationsFromContainer(dataSourceValidations, visitedValidations) {
    let aggLabels = Array("sum", "average", "max", "min", "standardDeviation", "median", "mode", "count");
    let dataValidationContainers = findNextLevelNodesByClass($(dataSourceValidations), ["data-validation-container"]);
    return dataValidationContainers.map(validation => {
        let validationAttributes = findNextLevelNodesByClass($(validation), "data-validation-field", ["card", "data-validation-container", "data-source-validation-container-nested-validation"]);

        return validationAttributes
            .map(attr => attr.getAttribute("aria-label") ? attr : $(attr).find(".data-validation-field")[0])
            .reduce((options, attr) => {
                let validationId = attr.getAttribute("id");
                if (!visitedValidations.has(validationId)) {
                    visitedValidations.add(validationId);
                    let label = camelize(attr.getAttribute("aria-label"));
                    let fieldValue = attr.value;
                    if (label === "type" && (fieldValue === "upstream" || fieldValue === "groupBy")) {
                        // nested fields can be defined for upstream and groupBy
                        let nestedValidations = Array.from(validation.querySelectorAll(".data-source-validation-container-nested-validation").values());
                        let allNestedValidations = [];

                        for (let nestedValidation of nestedValidations) {
                            let currNested = getValidationsFromContainer(nestedValidation, visitedValidations);
                            if (!jQuery.isEmptyObject(currNested)) {
                                currNested.forEach(n => {
                                    if (!jQuery.isEmptyObject(n)) {
                                        if (fieldValue === "upstream") {
                                            if (options["validation"]) {
                                                options["validation"].push(n);
                                            } else {
                                                options["validation"] = [n];
                                            }
                                        } else if (fieldValue === "groupBy") {
                                            Object.entries(n).forEach(o => options[o[0]] = o[1]);
                                        }
                                    }
                                });
                            }
                            allNestedValidations.push(currNested);
                        }
                        options[label] = fieldValue;
                        // options["validation"] = allNestedValidations.flat().filter(o => !jQuery.isEmptyObject(o));
                    } else if (aggLabels.includes(label)) {
                        options["aggType"] = label;
                        options["aggField"] = fieldValue;
                    } else if (label === "name" || label === "field" || label === "upstreamTaskName" || label === "joinType" || label === "joinExpr" || label === "description" || label === "errorThreshold") {
                        options[label] = fieldValue;
                    } else if (label === "joinFields" || label === "groupByFields") {
                        options[label] = fieldValue.includes(",") ? fieldValue.split(",") : [fieldValue];
                    } else if (label === "type") {

                    } else {
                        let currOpts = (options["options"] || new Map());
                        currOpts.set("type", label);
                        currOpts.set("value", fieldValue);
                        //TODO need to map the validation type params to key -> value pairs
                        // need to check if it is part of input group
                        let checkbox = $(attr).closest(".input-group").find(".form-check-input");
                        if (checkbox && checkbox.length > 0) {
                            if (checkbox[0].checked) {
                                switch (label) {
                                    case "lessThan":
                                    case "greaterThan":
                                    case "lessThanSize":
                                    case "greaterThanSize":
                                    case "isDecreasing":
                                    case "isIncreasing":
                                        currOpts.set("strictly", "false");
                                        break;
                                    default:
                                        currOpts.set("negate", "true");
                                        break;
                                }
                            }
                        }
                        if (currOpts.size > 0) {
                            if (options["validation"]) {
                                options["validation"].push(currOpts);
                            } else {
                                options["validation"] = [currOpts];
                            }
                        }
                    }
                    return options;
                } else {
                    return {};
                }
            }, {});
    });
}

export function getValidations(dataSource, currentValidation) {
    // check which checkboxes are enabled: auto, auto with external, manual
    let isAutoChecked = $(dataSource).find("[id^=auto-validation-checkbox]").is(":checked");
    let isAutoFromMetadataChecked = $(dataSource).find("[id^=auto-from-metadata-source-validation-checkbox]").is(":checked");
    let isManualChecked = $(dataSource).find("[id^=manual-validation-checkbox]").is(":checked");
    currentValidation["options"] = {};

    if (isAutoChecked) {
        // need to enable data generation within data source options
        currentValidation["options"]["enableDataValidation"] = "true";
    } else if (isAutoFromMetadataChecked) {
        let dataSourceValidationContainer = $(dataSource).find("[id^=data-source-validation-config-container]")[0];
        let dataSourceAutoSchemaContainer = $(dataSourceValidationContainer).find("[class~=data-source-auto-from-metadata-container]")[0];
        let metadataConnectionName = $(dataSourceAutoSchemaContainer).find("select[class~=metadata-connection-name]").val();
        $(dataSourceAutoSchemaContainer).find("input[class~=metadata-source-property]").toArray()
            .reduce(function (map, option) {
                if (option.value !== "") {
                    currentValidation["options"][option.getAttribute("aria-label")] = option.value;
                }
                return map;
            }, {});
        currentValidation["options"]["metadataSourceName"] = metadataConnectionName;
    } else if (isManualChecked) {
        // get top level validation container
        let dataSourceValidations = $(dataSource).find("[id^=data-source-validation-container]")[0];
        let visitedValidations = new Set();
        let dataValidationsWithAttributes = getValidationsFromContainer(dataSourceValidations, visitedValidations);
        currentValidation["validations"] = Object.values(dataValidationsWithAttributes);
    }
}

export function addFieldValidationBlock(newAttributeRow, mainContainer, attributeContainerId, inputClass) {
    numValidations += 1;
    let cardDiv = document.createElement("div");
    cardDiv.setAttribute("class", "card m-1 data-source-validation-container-nested-validation");
    let cardBody = document.createElement("div");
    cardBody.setAttribute("class", "card-body data-validation-container");
    cardBody.append(newAttributeRow);
    cardDiv.append(cardBody);
    mainContainer.append(cardDiv);

    // field validation applied after group by
    let {buttonWithMenuDiv, addAttributeButton, menu} = createButtonWithMenu(mainContainer);
    addItemsToAttributeMenu(validationTypeOptionsMap.get("field"), menu);
    newAttributeRow.append(buttonWithMenuDiv);
    let closeButton = createCloseButton(cardDiv);
    newAttributeRow.append(closeButton);
    menu.addEventListener("click", (event) => {
        let attribute = event.target.getAttribute("value");
        // check if attribute already exists
        if ($(newAttributeRow).find(`[aria-label=${attribute}]`).length === 0) {
            let validationMetadata = validationTypeOptionsMap.get("field")[attribute];
            addNewDataTypeAttribute(attribute, validationMetadata, `${attributeContainerId}-${attribute}`, inputClass, newAttributeRow);
        }
    });
}

export async function updateAccordionHeaderOnInputAndSelect(validationContainerHeadRow, accordionButton) {
    let defaultAttribute = $(validationContainerHeadRow).find(".default-attribute")[0];
    let defaultValidationInput = $(defaultAttribute).find(".input-field");
    let input = defaultValidationInput.length > 0 ? defaultValidationInput[0] : $(defaultValidationInput).find("select")[0];
    if (input) {
        input.addEventListener("input", (event) => {
            if (accordionButton.innerText.indexOf("-") === -1) {
                accordionButton.innerText = `${accordionButton.innerText} - ${event.target.value}`;
            } else {
                let selectText = accordionButton.innerText.split("-")[0];
                accordionButton.innerText = `${selectText} - ${event.target.value}`;
            }
        });
    }
}
