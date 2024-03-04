/*
Different types of validation:
- Basic column
- Dataset (column names, row count)
- Group by/aggregate
- Upstream
- External source (great expectations)
 */
import {
    addAccordionCloseButton,
    addItemsToAttributeMenu,
    addNewDataTypeAttribute,
    camelize,
    createAccordionItem,
    createButton,
    createButtonWithMenu,
    createCloseButton,
    createDataOrValidationTypeAttributes,
    createSelect,
    findNextLevelNodesByClass
} from "./shared.js";
import {validationTypeOptionsMap} from "./configuration-data.js";

export let numValidations = 0;

export function createManualValidation(index, additionalName) {
    let divName = additionalName ? `data-source-validation-container-${additionalName}` : "data-source-validation-container";
    let divContainer = document.createElement("div");
    divContainer.setAttribute("class", divName);
    divContainer.setAttribute("id", divName);
    let validationAccordion = document.createElement("div");
    validationAccordion.setAttribute("class", "accordion m-2");
    validationAccordion.setAttribute("style", "--bs-accordion-active-bg: blanchedalmond");
    // add new validations
    let addValidationButton = createButton(`add-validation-btn-${index}`, "add-validation", "btn btn-secondary", "+ Validation");
    addValidationButton.addEventListener("click", function () {
        numValidations += 1;
        let newValidation = createValidation(numValidations);
        validationAccordion.append(newValidation);
        $(".selectpicker").selectpicker();
    });

    divContainer.append(addValidationButton, validationAccordion);
    return divContainer;
}

function createGroupByValidationFromPlan(newValidation, validationOpts, validation) {
    $(newValidation).find("[aria-label=GroupByColumns]").val(validationOpts.groupByColumns)[0].dispatchEvent(new Event("input"));
    // can be nested validations

    if (validation.nested && validation.nested.validations) {
        for (let nestedValidation of validation.nested.validations) {
            numValidations += 1;
            let dataValidationContainer = $(newValidation).find(".data-validation-container")[0];
            let metadata = Object.create(validationTypeOptionsMap.get("groupBy")[nestedValidation.options["aggType"]]);
            metadata["default"] = nestedValidation.options["aggCol"];
            addNewDataTypeAttribute(nestedValidation.options["aggType"], metadata, `groupBy-validation-${numValidations}`, "data-validation-field", dataValidationContainer);
            let aggregationRow = $(dataValidationContainer).find(".data-source-validation-container-nested-validation").last().find(".row").first();

            if (nestedValidation.options) {
                for (let [optKey, optVal] of Object.entries(nestedValidation.options)) {
                    if (optKey !== "aggType" && optKey !== "aggCol") {
                        createNewValidateAttribute(optKey, "column", optVal, aggregationRow);
                    }
                }
            }
        }
    }
}

function createNewValidateAttribute(optKey, validationType, optVal, mainContainer) {
    numValidations += 1;
    let baseKey = optKey;
    if (optKey.startsWith("not")) {
        baseKey = optKey.charAt(3).toLowerCase() + optKey.slice(4);
    } else if (optKey.startsWith("equalOr")) {
        baseKey = optKey.charAt(7).toLowerCase() + optKey.slice(8);
    }
    let baseOptions = Object.create(validationTypeOptionsMap.get(validationType)[baseKey]);
    // if it is 'notEqual' or `equalOrLessThan`, need to ensure checkbox is checked
    if (baseKey !== optKey) baseOptions.group.checked = "true";
    baseOptions["default"] = optVal;
    addNewDataTypeAttribute(baseKey, baseOptions, `data-validation-container-${numValidations}-${optKey}`, "data-validation-field", mainContainer);
    document.getElementById(`data-validation-container-${numValidations}-${optKey}`).dispatchEvent(new Event("input"));
}

async function createNestedValidations(dataSource, manualValidation) {
    for (const validation of dataSource.validations) {
        numValidations += 1;
        let newValidation = await createValidation(numValidations);
        $(manualValidation).children(".accordion").append(newValidation);
        $(newValidation).find(".validation-type").val(validation.type)[0].dispatchEvent(new Event("change"));
        let validationOpts = validation.options;
        let mainContainer = $(newValidation).find(".data-validation-container")[0];

        if (validation.type === "column" && validationOpts.column) {
            $(newValidation).find("[aria-label=Column]").val(validationOpts.column)[0].dispatchEvent(new Event("input"));
        } else if (validation.type === "groupBy" && validationOpts.groupByColumns) {
            createGroupByValidationFromPlan(newValidation, validationOpts, validation);
        } else if (validation.type === "upstream" && validationOpts.upstreamTaskName) {
            $(newValidation).find("[aria-label=UpstreamTaskName]").val(validationOpts.upstreamTaskName)[0].dispatchEvent(new Event("input"));
            // can be nested validations

            if (validation.nested && validation.nested.validations) {
                let nestedManualValidation = $(newValidation).find(".data-source-validation-container-nested-validation").first();
                await createNestedValidations(validation.nested, nestedManualValidation);
            }
        }
        //otherwise it is column name validation which doesn't have any default options

        for (const [optKey, optVal] of Object.entries(validationOpts)) {
            if (optKey !== "groupByColumns" && optKey !== "column" && optKey !== "upstreamTaskName") {
                createNewValidateAttribute(optKey, validation.type, optVal, mainContainer);
            }
        }
    }
}

export async function createValidationFromPlan(dataSource, newDataSource) {
    if (dataSource.validations) {
        let manualValidation = createManualValidation(numValidations);
        let dataSourceGenContainer = $(newDataSource).find("#data-source-validation-config-container");
        dataSourceGenContainer.append(manualValidation);
        $(dataSourceGenContainer).find("#manual-validation-checkbox").prop("checked", true);

        await createNestedValidations(dataSource, manualValidation);
    }
}

function getValidationsFromContainer(dataSourceValidations, visitedValidations) {
    let dataValidationContainers = findNextLevelNodesByClass($(dataSourceValidations), ["data-validation-container"]);
    return dataValidationContainers.map(validation => {
        let validationAttributes = findNextLevelNodesByClass($(validation), "data-validation-field", ["card", "data-validation-container", "data-source-validation-container-nested-validation"]);

        return validationAttributes
            .filter(attr => attr.getAttribute("aria-label"))
            .reduce((options, attr) => {
                let validationId = attr.getAttribute("id");
                if (!visitedValidations.has(validationId)) {
                    visitedValidations.add(validationId);
                    let label = camelize(attr.getAttribute("aria-label"));
                    let fieldValue = attr.value;
                    if (label === "type" && (fieldValue === "upstream" || fieldValue === "groupBy")) {
                        // nested fields can be defined
                        let nestedValidations = Array.from(validation.querySelectorAll(".data-source-validation-container-nested-validation").values());
                        let allNestedValidations = [];
                        for (let nestedValidation of nestedValidations) {
                            let currNested = getValidationsFromContainer(nestedValidation, visitedValidations);
                            allNestedValidations.push(currNested);
                        }
                        options[label] = fieldValue;
                        options["nested"] = {validations: allNestedValidations.flat().filter(o => !jQuery.isEmptyObject(o))};
                    } else if (label === "sum" || label === "average" || label === "max" || label === "min" || label === "standardDeviation" || label === "count") {
                        // then we need to set the type as column and set the column name
                        options["type"] = "column";
                        let currOpts = (options["options"] || new Map());
                        currOpts.set("aggType", label);
                        currOpts.set("aggCol", fieldValue);
                        options["options"] = currOpts;
                    } else if (label === "name" || label === "type") {
                        options[label] = fieldValue;
                    } else {
                        let currOpts = (options["options"] || new Map());
                        // need to check if it is part of input group
                        let checkbox = $(attr).closest(".input-group").find(".form-check-input");
                        if (checkbox && checkbox.length > 0) {
                            if (checkbox[0].checked) {
                                // then we need to get the opposite of the label (i.e. equal -> notEqual)
                                switch (label) {
                                    case "equal":
                                    case "contains":
                                    case "between":
                                    case "in":
                                    case "matches":
                                    case "startsWith":
                                    case "endsWith":
                                    case "null":
                                    case "size":
                                        let oppositeLabel = "not" + label.charAt(0).toUpperCase() + label.slice(1);
                                        currOpts.set(oppositeLabel, fieldValue);
                                        break;
                                    case "lessThan":
                                    case "greaterThan":
                                    case "lessThanSize":
                                    case "greaterThanSize":
                                        let equalLabel = "equalOr" + label.charAt(0).toUpperCase() + label.slice(1);
                                        currOpts.set(equalLabel, fieldValue);
                                        break;
                                }
                            }
                        } else {
                            currOpts.set(label, fieldValue);
                        }
                        options["options"] = currOpts;
                    }
                    return options;
                } else {
                    return {};
                }
            }, {});
    });
}

export function getValidations(dataSource, currentDataSource) {
    // get top level validation container
    let dataSourceValidations = dataSource.querySelector(".data-source-validation-container");
    let visitedValidations = new Set();
    let dataValidationsWithAttributes = getValidationsFromContainer(dataSourceValidations, visitedValidations);
    currentDataSource["validations"] = Object.values(dataValidationsWithAttributes);
}

export function addColumnValidationBlock(newAttributeRow, mainContainer, attributeContainerId, inputClass) {
    numValidations += 1;
    let cardDiv = document.createElement("div");
    cardDiv.setAttribute("class", "card m-1 data-source-validation-container-nested-validation");
    let cardBody = document.createElement("div");
    cardBody.setAttribute("class", "card-body data-validation-container");
    cardBody.append(newAttributeRow);
    let closeButton = createCloseButton(cardDiv);
    newAttributeRow.insertBefore(closeButton, newAttributeRow.firstChild);
    cardDiv.append(cardBody);
    mainContainer.append(cardDiv);

    // column validation applied after group by
    let {buttonWithMenuDiv, addAttributeButton, menu} = createButtonWithMenu(mainContainer);
    addItemsToAttributeMenu(validationTypeOptionsMap.get("column"), menu);
    newAttributeRow.append(buttonWithMenuDiv);
    menu.addEventListener("click", (event) => {
        let attribute = event.target.getAttribute("value");
        // check if attribute already exists
        if ($(newAttributeRow).find(`[aria-label=${attribute}]`).length === 0) {
            let validationMetadata = validationTypeOptionsMap.get("column")[attribute];
            addNewDataTypeAttribute(attribute, validationMetadata, attributeContainerId, inputClass, newAttributeRow);
        }
    });
}

function updateValidationAccordionHeaderOnInput(validationContainerHeadRow, accordionButton) {
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

function createValidation(index) {
    let validationContainer = document.createElement("div");
    validationContainer.setAttribute("class", "data-validation-container");
    validationContainer.setAttribute("id", "data-validation-container-" + index);
    let validationContainerHeadRow = document.createElement("div");
    validationContainerHeadRow.setAttribute("class", "row g-3 m-1 align-items-center");
    validationContainerHeadRow.setAttribute("id", `validation-head-row-${index}`);
    validationContainer.append(validationContainerHeadRow);

    let validationTypeSelect = createSelect(`validation-type-${index}`, "Type", "selectpicker form-control input-field data-validation-field validation-type");
    validationTypeSelect.setAttribute("title", "Select validation type...");
    validationTypeSelect.setAttribute("data-header", "Select validation type...");
    let validationTypeCol = document.createElement("div");
    validationTypeCol.setAttribute("class", "col");
    validationTypeCol.append(validationTypeSelect);

    for (const key of validationTypeOptionsMap.keys()) {
        let selectOption = document.createElement("option");
        selectOption.setAttribute("value", key);
        selectOption.innerText = key;
        validationTypeSelect.append(selectOption);
    }

    let accordionItem = createAccordionItem(`validation-${index}`, `validation-${index}`, "", validationContainer, "show");
    addAccordionCloseButton(accordionItem);

    accordionItem.classList.add("accordion-data-validation-container");
    validationContainerHeadRow.append(validationTypeCol);
    createDataOrValidationTypeAttributes(validationContainerHeadRow, "validation-type");
    // on select change and input of default-attribute, update accordion button
    let accordionButton = $(accordionItem).find(".accordion-button")[0];
    validationTypeSelect.addEventListener("change", (event) => {
        accordionButton.innerText = event.target.value;
        updateValidationAccordionHeaderOnInput(validationContainerHeadRow, accordionButton);
    });
    updateValidationAccordionHeaderOnInput(validationContainerHeadRow, accordionButton);
    return accordionItem;
}