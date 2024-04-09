import {
    createButton,
    createFormFloating,
    createFormText,
    createInput,
    createRadioButtons,
    createSelect
} from "./shared.js";


export function createRecordCount(index) {
    let recordCountContainer = document.createElement("div");
    recordCountContainer.setAttribute("id", "record-count-container");
    recordCountContainer.setAttribute("class", "card card-body")
    let recordCountHeader = document.createElement("h5");
    recordCountHeader.innerText = "Record count";
    let recordCountRow = document.createElement("div");
    recordCountRow.setAttribute("class", "record-count-row");
    // have 3 columns
    // - total      -> number or random between min max
    // - per column -> number or random between min max
    // - estimated number of record
    let estimatedRecordCountContainer = document.createElement("div");
    estimatedRecordCountContainer.setAttribute("class", "col");
    let estimatedRecordCount = document.createElement("p");
    estimatedRecordCount.innerHTML = "<strong>Estimate number of records: 1000</strong>";
    estimatedRecordCountContainer.append(estimatedRecordCount);
    let baseRecordRadio = createBaseRecordCountContainer(index);
    let perColumnContainer = createPerColumnCountContainer(index, estimatedRecordCountContainer);
    let advancedButton = createButton("record-count-advanced-" + index, "Advanced", "btn btn-secondary m-1", "Advanced");
    advancedButton.setAttribute("data-bs-toggle", "collapse");
    advancedButton.setAttribute("data-bs-target", "#" + perColumnContainer.getAttribute("id"));
    advancedButton.setAttribute("aria-expanded", "false");
    advancedButton.setAttribute("aria-controls", perColumnContainer.getAttribute("id"));

    recordCountRow.append(baseRecordRadio, advancedButton, perColumnContainer);
    $(recordCountRow).find("input[type=radio].base-record-count-radio,input[type=radio].per-column-record-count-radio").change(function () {
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

        if (dsCount.perColumnNames) {
            $(newDataSource).find("[id^=per-column-names]").val(dsCount.perColumnNames.join(","));
            if (dsCount.perColumnRecordsMin && dsCount.perColumnRecordsMax) {
                $(newDataSource).find(".per-unique-set-of-values-between").prop("checked", true);
                $(newDataSource).find("[id^=per-column-min-record-count]").val(dsCount.perColumnRecordsMin);
                $(newDataSource).find("[id^=per-column-max-record-count]").val(dsCount.perColumnRecordsMax);
            } else {
                $(newDataSource).find(".per-unique-set-of-values").prop("checked", true);
                $(newDataSource).find("[id^=per-column-record-count]").val(dsCount.perColumnRecords);
            }
            $(newDataSource).find("[id^=per-column-distribution-select]").selectpicker("val", dsCount.perColumnRecordsDistribution)[0].dispatchEvent(new Event("change"));
            if (dsCount.perColumnRecordsDistribution === "exponential") {
                $(newDataSource).find("[id^=per-column-distribution-rate]").val(dsCount.perColumnRecordsDistributionRateParam);
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

function createPerColumnCountContainer(index, estimatedRecordCountContainer) {
    let perColumnRecordCol = createRecordCountInput(index, "per-column-record-count", "Records", "2");
    let perColumnMinCol = createRecordCountInput(index, "per-column-min-record-count", "Min", "1");
    let perColumnMaxCol = createRecordCountInput(index, "per-column-max-record-count", "Max", "2");
    let perColumnBetweenContainer = document.createElement("div");
    perColumnBetweenContainer.setAttribute("class", "row g-1");
    perColumnBetweenContainer.append(perColumnMinCol, perColumnMaxCol);
    let perColumnOptions = [{text: "None"}, {
        text: "Per unique set of values",
        child: perColumnRecordCol
    }, {text: "Per unique set of values between", child: perColumnBetweenContainer}];
    let perColumnRadio = createRadioButtons(index, "per-column-record-count-radio", perColumnOptions, "col-6");
    // above per column radio is choice of columns
    let perColumnText = createInput(`per-column-names-${index}`, "Column(s)", "form-control input-field record-count-field", "text", "");
    let perColumnFormFloating = createFormFloating("Column(s)", perColumnText);
    // per column distribution alongside radio buttons
    let perColumnDistributionSelect = createSelect(`per-column-distribution-select-${index}`, "Distribution", "selectpicker form-control input-field record-count-distribution-field col")
    perColumnDistributionSelect.setAttribute("title", "Select data distribution...");
    perColumnDistributionSelect.setAttribute("data-header", "Select data distribution...");
    ["Uniform", "Exponential", "Normal"].forEach(dist => {
        let option = document.createElement("option");
        option.setAttribute("value", dist.toLowerCase());
        option.innerText = dist;
        perColumnDistributionSelect.append(option);
    });

    let perColumnOptionsRow = document.createElement("div");
    perColumnOptionsRow.setAttribute("class", "row g-3 m-1 align-items-center");
    perColumnOptionsRow.append(perColumnRadio, perColumnDistributionSelect);
    $(perColumnDistributionSelect).selectpicker("val", "uniform");

    let perColumnDistributionRateParam = createInput(`per-column-distribution-rate-param-${index}`, "Rate Parameter", "form-control input-field record-count-distribution-field", "number", "1.0");
    perColumnDistributionRateParam.setAttribute("min", "0");
    perColumnDistributionRateParam.setAttribute("step", "0.00000001");
    let formFloatingRate = createFormFloating("Rate Parameter", perColumnDistributionRateParam);
    perColumnDistributionSelect.addEventListener("change", (event) => {
        if (event.target.value === "exponential") {
            // add extra input for rate parameter
            perColumnOptionsRow.append(formFloatingRate);
        } else {
            // check if rate parameter exists, if it does, remove it
            perColumnOptionsRow.removeChild(formFloatingRate);
        }
    });

    let columnInputRow = document.createElement("div");
    columnInputRow.setAttribute("class", "row g-3 m-1 align-items-center");
    let columnInputHelpDiv = createFormText(perColumnFormFloating.getAttribute("id"), "Choose which column(s) to use for creating multiple records each unique group of values.", "span");
    columnInputHelpDiv.setAttribute("class", "col-6");
    columnInputRow.append(perColumnFormFloating, columnInputHelpDiv);

    let perColumnInnerContainer = document.createElement("div");
    perColumnInnerContainer.setAttribute("class", "card card-body");
    if (index === 1 || perColumnInnerContainer.childElementCount === 0) {   //TODO should only put if first task in UI
        let perColumnExampleButton = createButton("per-column-example-button", "per-column-example", "btn btn-info", "Example");
        perColumnExampleButton.setAttribute("data-bs-toggle", "modal");
        perColumnExampleButton.setAttribute("data-bs-target", "#perColumnExampleModal");
        let perColumnHelpText = document.createElement("div");
        perColumnHelpText.innerHTML = "Generate multiple records per set of unique column value(s). " + perColumnExampleButton.outerHTML;
        perColumnInnerContainer.append(perColumnHelpText);
    }

    // TODO when perColumnText is empty, disable checkbox for per column
    let perColumnContainer = document.createElement("div");
    perColumnContainer.setAttribute("id", "count-advanced-collapse-" + index);
    perColumnContainer.setAttribute("class", "collapse");
    perColumnInnerContainer.append(columnInputRow, perColumnOptionsRow, estimatedRecordCountContainer);
    perColumnContainer.append(perColumnInnerContainer);
    return perColumnContainer;
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

    let perColumnCheck = $(recordCountRow).find("input.per-column-record-count-radio:checked").parent().find(".record-count-field");
    let perColumnCount;
    if (perColumnCheck.length > 1) {
        let minPerCol = Number($(perColumnCheck).filter("input[aria-label=Min]").val());
        let maxPerCol = Number($(perColumnCheck).filter("input[aria-label=Max]").val());
        perColumnCount = (maxPerCol + minPerCol) / 2;
        recordCountSummary["perColumnRecordsMin"] = minPerCol;
        recordCountSummary["perColumnRecordsMax"] = maxPerCol;
    } else if (perColumnCheck.length === 1) {
        perColumnCount = Number(perColumnCheck.val());
        recordCountSummary["perColumnRecords"] = perColumnCount;
    } else {
        perColumnCount = 1;
    }
    if (perColumnCheck.length >= 1) {
        let perColumNames = $(recordCountRow).find("[id^=per-column-names]").val();
        recordCountSummary["perColumnNames"] = perColumNames ? perColumNames.split(",") : [];
    }
    recordCountSummary["perColumnRecordsDistribution"] = $(recordCountRow).find("[id^=per-column-distribution-select]").val();
    recordCountSummary["perColumnRecordsDistributionRateParam"] = $(recordCountRow).find("[id^=per-column-distribution-rate-param]").val();

    recordCountSummary["estimateRecords"] = baseRecordCount * perColumnCount;
    return recordCountSummary;
}

function createRecordCountInput(index, name, label, value) {
    let recordCountInput = createInput(`${name}-${index}`, label, "form-control input-field record-count-field", "number", value);
    let radioGroup = name.startsWith("per-column") ? `per-column-count-${index}` : `base-record-count-${index}`;
    recordCountInput.setAttribute("radioGroup", radioGroup);
    recordCountInput.setAttribute("min", "0");
    return createFormFloating(label, recordCountInput);
}