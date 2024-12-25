import {
    createButton,
    createFormFloating,
    createFormText,
    createInput,
    createRadioButtons,
    createSelect,
    dispatchEvent
} from "./shared.js";


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
        let dsCount = dataSource.count;
        if (dsCount.recordsMin && dsCount.recordsMax) {
            $(newDataSource).find(".generated-records-between").prop("checked", true);
            $(newDataSource).find("[id^=min-gen-record-count]").val(dsCount.recordsMin);
            $(newDataSource).find("[id^=max-gen-record-count]").val(dsCount.recordsMax);
        } else {
            $(newDataSource).find(".records").prop("checked", true);
            $(newDataSource).find("[id^=base-record-count]").val(dsCount.records);
        }

        if (dsCount.perFieldNames) {
            $(newDataSource).find("[id^=per-field-names]").val(dsCount.perFieldNames.join(","));
            if (dsCount.perFieldRecordsMin && dsCount.perFieldRecordsMax) {
                $(newDataSource).find(".per-unique-set-of-values-between").prop("checked", true);
                $(newDataSource).find("[id^=per-field-min-record-count]").val(dsCount.perFieldRecordsMin);
                $(newDataSource).find("[id^=per-field-max-record-count]").val(dsCount.perFieldRecordsMax);
            } else {
                $(newDataSource).find(".per-unique-set-of-values").prop("checked", true);
                $(newDataSource).find("[id^=per-field-record-count]").val(dsCount.perFieldRecords);
            }
            $(newDataSource).find("[id^=per-field-distribution-select]").selectpicker("val", dsCount.perFieldRecordsDistribution);
            let updatedPerFieldDistribution = $(newDataSource).find("[id^=per-field-distribution-select]");
            dispatchEvent(updatedPerFieldDistribution, "change");
            if (dsCount.perFieldRecordsDistribution === "exponential") {
                $(newDataSource).find("[id^=per-field-distribution-rate]").val(dsCount.perFieldRecordsDistributionRateParam);
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

function estimateRecordCount(recordCountRow) {
    let recordCountSummary = {};
    let baseRecordCheck = $(recordCountRow).find("input.base-record-count-radio:checked").parent().find(".record-count-field");
    let baseRecordCount;
    if (baseRecordCheck.length > 1) {
        let minBase = Number($(baseRecordCheck).filter("input[aria-label=Min]").val());
        let maxBase = Number($(baseRecordCheck).filter("input[aria-label=Max]").val());
        baseRecordCount = (maxBase + minBase) / 2;
        recordCountSummary["recordsMin"] = minBase;
        recordCountSummary["recordsMax"] = maxBase;
    } else {
        baseRecordCount = Number(baseRecordCheck.val());
        recordCountSummary["records"] = baseRecordCount;
    }

    let perFieldCheck = $(recordCountRow).find("input.per-field-record-count-radio:checked").parent().find(".record-count-field");
    let perFieldCount;
    if (perFieldCheck.length > 1) {
        let minPerCol = Number($(perFieldCheck).filter("input[aria-label=Min]").val());
        let maxPerCol = Number($(perFieldCheck).filter("input[aria-label=Max]").val());
        perFieldCount = (maxPerCol + minPerCol) / 2;
        recordCountSummary["perFieldRecordsMin"] = minPerCol;
        recordCountSummary["perFieldRecordsMax"] = maxPerCol;
    } else if (perFieldCheck.length === 1) {
        perFieldCount = Number(perFieldCheck.val());
        recordCountSummary["perFieldRecords"] = perFieldCount;
    } else {
        perFieldCount = 1;
    }
    if (perFieldCheck.length >= 1) {
        let perColumNames = $(recordCountRow).find("[id^=per-field-names]").val();
        recordCountSummary["perFieldNames"] = perColumNames ? perColumNames.split(",") : [];
    }
    let perFieldDist = $(recordCountRow).find("[id^=per-field-distribution-select]").val();
    if (perFieldDist !== "uniform") {
        recordCountSummary["perFieldRecordsDistribution"] = perFieldDist;
    }
    recordCountSummary["perFieldRecordsDistributionRateParam"] = $(recordCountRow).find("[id^=per-field-distribution-rate-param]").val();

    recordCountSummary["estimateRecords"] = baseRecordCount * perFieldCount;
    return recordCountSummary;
}

function createRecordCountInput(index, name, label, value) {
    let recordCountInput = createInput(`${name}-${index}`, label, "form-control input-field record-count-field", "number", value);
    let radioGroup = name.startsWith("per-field") ? `per-field-count-${index}` : `base-record-count-${index}`;
    recordCountInput.setAttribute("radioGroup", radioGroup);
    recordCountInput.setAttribute("min", "0");
    return createFormFloating(label, recordCountInput);
}