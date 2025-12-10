import {
    createButton,
    createFormFloating,
    createFormText,
    createInput,
    createRadioButtons,
    createSelect,
    dispatchEvent
} from "./shared.js";
import {
    buildCountFromUIState,
    extractUIStateFromCount,
    estimateRecordCountFromConfig
} from "./data-transformers.js";


export function createRecordCount(index) {
    let recordCountContainer = document.createElement("div");
    recordCountContainer.setAttribute("id", "record-count-container");
    recordCountContainer.setAttribute("class", "card card-body")
    let recordCountHeader = document.createElement("h5");
    recordCountHeader.innerText = "Record count";
    let recordCountRow = document.createElement("div");
    recordCountRow.setAttribute("class", "record-count-row");
    // have 3 fields
    // - total      -> number or random between min max
    // - per field -> number or random between min max
    // - estimated number of record
    let estimatedRecordCountContainer = document.createElement("div");
    estimatedRecordCountContainer.setAttribute("class", "col");
    let estimatedRecordCount = document.createElement("p");
    estimatedRecordCount.innerHTML = "<strong>Estimate number of records: 1000</strong>";
    estimatedRecordCountContainer.append(estimatedRecordCount);
    let baseRecordRadio = createBaseRecordCountContainer(index);
    let perFieldContainer = createPerFieldCountContainer(index, estimatedRecordCountContainer);
    let advancedButton = createButton("record-count-advanced-" + index, "Advanced", "btn btn-secondary m-1", "Advanced");
    advancedButton.setAttribute("data-bs-toggle", "collapse");
    advancedButton.setAttribute("data-bs-target", "#" + perFieldContainer.getAttribute("id"));
    advancedButton.setAttribute("aria-expanded", "false");
    advancedButton.setAttribute("aria-controls", perFieldContainer.getAttribute("id"));

    recordCountRow.append(baseRecordRadio, advancedButton, perFieldContainer);
    $(recordCountRow).find("input[type=radio].base-record-count-radio,input[type=radio].per-field-record-count-radio").change(function () {
        let newEstimate = estimateRecordCount(recordCountRow)["estimateRecords"];
        estimatedRecordCount.innerHTML = "<strong>Estimate number of records: " + newEstimate + "</strong>";
    });
    estimateRecordCount(recordCountRow);
    recordCountContainer.append(recordCountHeader, recordCountRow);
    return recordCountContainer;
}

export function createCountElementsFromPlan(dataSource, newDataSource) {
    if (dataSource.count) {
        // Use pure function to extract UI state from backend count format
        const uiState = extractUIStateFromCount(dataSource.count);
        
        // Apply UI state to DOM elements
        if (uiState.isRecordsBetween) {
            $(newDataSource).find(".generated-records-between").prop("checked", true);
            $(newDataSource).find("[id^=min-gen-record-count]").val(uiState.recordsMin);
            $(newDataSource).find("[id^=max-gen-record-count]").val(uiState.recordsMax);
        } else if (uiState.records !== undefined) {
            $(newDataSource).find(".records").prop("checked", true);
            $(newDataSource).find("[id^=base-record-count]").val(uiState.records);
        }

        // Apply per-field UI state
        if (uiState.perFieldNames && uiState.perFieldNames.length > 0) {
            $(newDataSource).find("[id^=per-field-names]").val(uiState.perFieldNames.join(","));
            
            if (uiState.isPerFieldBetween) {
                $(newDataSource).find(".per-unique-set-of-values-between").prop("checked", true);
                $(newDataSource).find("[id^=per-field-min-record-count]").val(uiState.perFieldMin);
                $(newDataSource).find("[id^=per-field-max-record-count]").val(uiState.perFieldMax);
            } else if (uiState.perFieldCount !== undefined) {
                $(newDataSource).find(".per-unique-set-of-values").prop("checked", true);
                $(newDataSource).find("[id^=per-field-record-count]").val(uiState.perFieldCount);
            }
            
            // Handle distribution settings
            if (uiState.distribution) {
                $(newDataSource).find("[id^=per-field-distribution-select]").selectpicker("val", uiState.distribution);
                let updatedPerFieldDistribution = $(newDataSource).find("[id^=per-field-distribution-select]");
                dispatchEvent(updatedPerFieldDistribution, "change");
                if (uiState.distribution === "exponential" && uiState.rateParam !== undefined) {
                    $(newDataSource).find("[id^=per-field-distribution-rate]").val(uiState.rateParam);
                }
            }
        }
    }
}

export function getRecordCount(dataSource, currentDataSource) {
    let recordCountRow = dataSource.querySelector(".record-count-row");
    let recordCountSummary = estimateRecordCount(recordCountRow);
    delete recordCountSummary.estimateRecords;
    currentDataSource["count"] = recordCountSummary;
}

function createPerFieldCountContainer(index, estimatedRecordCountContainer) {
    let perFieldRecordCol = createRecordCountInput(index, "per-field-record-count", "Records", "2");
    let perFieldMinCol = createRecordCountInput(index, "per-field-min-record-count", "Min", "1");
    let perFieldMaxCol = createRecordCountInput(index, "per-field-max-record-count", "Max", "2");
    let perFieldBetweenContainer = document.createElement("div");
    perFieldBetweenContainer.setAttribute("class", "row g-1");
    perFieldBetweenContainer.append(perFieldMinCol, perFieldMaxCol);
    let perFieldOptions = [{text: "None"}, {
        text: "Per unique set of values",
        child: perFieldRecordCol
    }, {text: "Per unique set of values between", child: perFieldBetweenContainer}];
    let perFieldRadio = createRadioButtons(index, "per-field-record-count-radio", perFieldOptions, "col-6");
    // above per field radio is choice of fields
    let perFieldText = createInput(`per-field-names-${index}`, "Field(s)", "form-control input-field record-count-field", "text", "");
    let perFieldFormFloating = createFormFloating("Field(s)", perFieldText);
    // per field distribution alongside radio buttons
    let perFieldDistributionSelect = createSelect(`per-field-distribution-select-${index}`, "Distribution", "selectpicker form-control input-field record-count-distribution-field col", "Select data distribution...");
    ["Uniform", "Exponential", "Normal"].forEach(dist => {
        let option = document.createElement("option");
        option.setAttribute("value", dist.toLowerCase());
        option.innerText = dist;
        perFieldDistributionSelect.append(option);
    });

    let perFieldOptionsRow = document.createElement("div");
    perFieldOptionsRow.setAttribute("class", "row g-3 m-1 align-items-center");
    perFieldOptionsRow.append(perFieldRadio, perFieldDistributionSelect);
    $(perFieldDistributionSelect).selectpicker("val", "uniform");

    let perFieldDistributionRateParam = createInput(`per-field-distribution-rate-param-${index}`, "Rate Parameter", "form-control input-field record-count-distribution-field", "number", "1.0");
    perFieldDistributionRateParam.setAttribute("min", "0");
    perFieldDistributionRateParam.setAttribute("step", "0.00000001");
    let formFloatingRate = createFormFloating("Rate Parameter", perFieldDistributionRateParam);
    perFieldDistributionSelect.addEventListener("change", (event) => {
        if (event.target.value === "exponential") {
            // add extra input for rate parameter
            perFieldOptionsRow.append(formFloatingRate);
        } else if (perFieldOptionsRow.contains(formFloatingRate)) {
            // check if rate parameter exists, if it does, remove it
            perFieldOptionsRow.removeChild(formFloatingRate);
        }
    });

    let fieldInputRow = document.createElement("div");
    fieldInputRow.setAttribute("class", "row g-3 m-1 align-items-center");
    let fieldInputHelpDiv = createFormText(perFieldFormFloating.getAttribute("id"), "Choose which field(s) to use for creating multiple records each unique group of values.", "span");
    fieldInputHelpDiv.setAttribute("class", "col-6");
    fieldInputRow.append(perFieldFormFloating, fieldInputHelpDiv);

    let perFieldInnerContainer = document.createElement("div");
    perFieldInnerContainer.setAttribute("class", "card card-body");
    if (index === 1 || perFieldInnerContainer.childElementCount === 0) {   //TODO should only put if first task in UI
        let perFieldExampleButton = createButton("per-field-example-button", "per-field-example", "btn btn-info", "Example");
        perFieldExampleButton.setAttribute("data-bs-toggle", "modal");
        perFieldExampleButton.setAttribute("data-bs-target", "#perFieldExampleModal");
        let perFieldHelpText = document.createElement("div");
        perFieldHelpText.innerHTML = "Generate multiple records per set of unique field value(s). " + perFieldExampleButton.outerHTML;
        perFieldInnerContainer.append(perFieldHelpText);
    }

    // TODO when perFieldText is empty, disable checkbox for per field
    let perFieldContainer = document.createElement("div");
    perFieldContainer.setAttribute("id", "count-advanced-collapse-" + index);
    perFieldContainer.setAttribute("class", "collapse");
    perFieldInnerContainer.append(fieldInputRow, perFieldOptionsRow, estimatedRecordCountContainer);
    perFieldContainer.append(perFieldInnerContainer);
    return perFieldContainer;
}

function createBaseRecordCountContainer(index) {
    let baseRecordCol = createRecordCountInput(index, "base-record-count", "Records", "1000");
    let baseRecordMinInput = createRecordCountInput(index, "min-gen-record-count", "Min", "1000");
    let baseRecordMaxInput = createRecordCountInput(index, "max-gen-record-count", "Max", "2000");
    let baseRecordBetweenContainer = document.createElement("div");
    baseRecordBetweenContainer.setAttribute("class", "row g-1");
    baseRecordBetweenContainer.append(baseRecordMinInput, baseRecordMaxInput);
    let baseRecordOptions = [{text: "Records", child: baseRecordCol}, {
        text: "Generated records between",
        child: baseRecordBetweenContainer
    }];
    return createRadioButtons(index, "base-record-count-radio", baseRecordOptions);
}

/**
 * Extracts UI state from DOM and builds backend-compatible count object.
 * Uses pure function buildCountFromUIState for the data transformation.
 */
function estimateRecordCount(recordCountRow) {
    // Extract UI state from DOM
    const uiState = extractUIStateFromDOM(recordCountRow);
    
    // Use pure function to build backend-compatible count object
    const recordCountSummary = buildCountFromUIState(uiState);
    
    // Calculate estimate using pure function
    recordCountSummary.estimateRecords = estimateRecordCountFromConfig(recordCountSummary);
    
    return recordCountSummary;
}

/**
 * Extracts UI state from DOM elements.
 * This is the DOM-dependent part, separated from the data transformation logic.
 * 
 * @param {HTMLElement} recordCountRow - The record count row element
 * @returns {Object} UI state object for buildCountFromUIState
 */
export function extractUIStateFromDOM(recordCountRow) {
    const uiState = {};
    
    // Extract base record count state
    const baseRecordCheck = $(recordCountRow).find("input.base-record-count-radio:checked").parent().find(".record-count-field");
    if (baseRecordCheck.length > 1) {
        // "Generated records between" is selected
        uiState.isRecordsBetween = true;
        uiState.recordsMin = Number($(baseRecordCheck).filter("input[aria-label=Min]").val());
        uiState.recordsMax = Number($(baseRecordCheck).filter("input[aria-label=Max]").val());
    } else {
        // Simple "Records" is selected
        uiState.isRecordsBetween = false;
        uiState.records = Number(baseRecordCheck.val());
    }
    
    // Extract per-field state
    const perFieldColumnNames = $(recordCountRow).find("[id^=per-field-names]").val();
    if (perFieldColumnNames && perFieldColumnNames.trim().length > 0) {
        uiState.perFieldNames = perFieldColumnNames.split(",").map(s => s.trim()).filter(s => s.length > 0);
        
        const perFieldCheck = $(recordCountRow).find("input.per-field-record-count-radio:checked").parent().find(".record-count-field");
        if (perFieldCheck.length > 1) {
            // "Per unique set of values between" is selected
            uiState.isPerFieldBetween = true;
            uiState.perFieldMin = Number($(perFieldCheck).filter("input[aria-label=Min]").val());
            uiState.perFieldMax = Number($(perFieldCheck).filter("input[aria-label=Max]").val());
        } else if (perFieldCheck.length === 1) {
            // Simple "Per unique set of values" is selected
            uiState.isPerFieldBetween = false;
            uiState.perFieldCount = Number(perFieldCheck.val());
        }
        
        // Extract distribution settings
        const perFieldDist = $(recordCountRow).find("[id^=per-field-distribution-select]").val();
        if (perFieldDist) {
            uiState.distribution = perFieldDist;
        }
        
        const rateParam = $(recordCountRow).find("[id^=per-field-distribution-rate-param]").val();
        if (perFieldDist === "exponential" && rateParam) {
            uiState.rateParam = Number(rateParam);
        }
    }
    
    return uiState;
}

function createRecordCountInput(index, name, label, value) {
    let recordCountInput = createInput(`${name}-${index}`, label, "form-control input-field record-count-field", "number", value);
    let radioGroup = name.startsWith("per-field") ? `per-field-count-${index}` : `base-record-count-${index}`;
    recordCountInput.setAttribute("radioGroup", radioGroup);
    recordCountInput.setAttribute("min", "0");
    return createFormFloating(label, recordCountInput);
}