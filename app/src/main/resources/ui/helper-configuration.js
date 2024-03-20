/*
Configuration for job execution. Includes:
- Flags for enabling/disabling features
- Metadata
- Generation
- Validation
- Folder pathways
- Alerts
- Runtime
 */
import {createAccordionItem, createFormFloating, createFormText, createInput, createSelect} from "./shared.js";
import {configurationOptionsMap, reportOptionsMap} from "./configuration-data.js";

export function createConfiguration() {
    let configurationContainer = document.createElement("div");
    configurationContainer.setAttribute("class", "accordion mt-2");
    let configIndex = 0;
    for (let [configKey, configOptions] of configurationOptionsMap.entries()) {
        configIndex += 1;
        // create accordion item with configKey header and configOptions as grid content
        let header = configKey.charAt(0).toUpperCase() + configKey.slice(1);
        let configOptionsContainer = document.createElement("div");
        configOptionsContainer.setAttribute("class", "m-1 configuration-options-container");
        for (let [optKey, options] of Object.entries(configOptions)) {
            // check if config is already included in the alert section, if not, add it
            if (!reportOptionsMap.has(optKey)) {
                let newConfigOptionRow = createNewConfigRow(configKey, optKey, options);
                configOptionsContainer.append(newConfigOptionRow);
            }
        }
        let accordionItem = createAccordionItem(`config-${configIndex}`, header, "", configOptionsContainer);
        configurationContainer.append(accordionItem);
    }
    return configurationContainer;
}

export function createConfigurationFromPlan(respJson) {
    if (respJson.configuration) {
        let configContainer = $(document).find("#configuration-details-body,#report-details-body");
        for (const [configParent, configValues] of Object.entries(respJson.configuration)) {
            for (const [optKey, optValue] of Object.entries(configValues)) {
                let currentConfigElement = $(configContainer).find(`[configuration-parent="${configParent}"][configuration="${optKey}"]`);
                let typeAttr = currentConfigElement.attr("type");
                if (typeAttr && typeAttr === "checkbox") {
                    currentConfigElement.prop("checked", optValue === "true");
                } else if (optValue !== "") {
                    currentConfigElement.val(optValue);
                }
            }
        }
    }
}

export function getConfiguration() {
    let configurationOptionContainers = Array.from(document.querySelectorAll(".configuration-options-container").values());
    let mappedConfiguration = new Map();
    configurationOptionContainers.forEach(configurationOptionContainer => {
        let inputConfigurations = Array.from(configurationOptionContainer.querySelectorAll(".input-configuration").values());
        for (let option of inputConfigurations) {
            let baseConfig = option.getAttribute("configuration-parent");
            let options = (mappedConfiguration.get(baseConfig) || new Map());
            if (option.getAttribute("type") === "checkbox") {
                let optionEnabled = option.checked ? "true" : "false";
                options.set(option.getAttribute("configuration"), optionEnabled);
            } else {
                options.set(option.getAttribute("configuration"), option.value);
            }
            mappedConfiguration.set(baseConfig, options);
        }
    });
    return mappedConfiguration;
}

export function createNewConfigRow(configKey, optKey, options) {
    let newConfigOptionRow = document.createElement("div");
    newConfigOptionRow.setAttribute("class", "row g-3 m-1 align-items-center user-added-attribute");
    let configOptionInput = createConfigurationOption(configKey, optKey, options);
    newConfigOptionRow.append(configOptionInput);
    if (options.help) {
        let formText = createFormText(optKey, options.help, "span");
        formText.setAttribute("class", "col-8");
        newConfigOptionRow.append(formText);
    }
    return newConfigOptionRow;
}

function createConfigurationOption(configKey, config, options) {
    let configLabel = options["paid"] ? `${options.displayName}*` : options.displayName;
    let colWrapper = document.createElement("div");
    colWrapper.setAttribute("class", "col-4");
    if (options.choice && options.choice.includes("true")) {
        let formSwitch = document.createElement("div");
        formSwitch.setAttribute("class", "form-check form-switch m-1");
        let switchInput = document.createElement("input");
        switchInput.setAttribute("class", "form-check-input input-configuration");
        switchInput.setAttribute("configuration-parent", configKey);
        switchInput.setAttribute("configuration", options.configName);
        switchInput.setAttribute("type", "checkbox");
        switchInput.setAttribute("role", "switch");
        switchInput.setAttribute("id", "switch-" + config);
        if (options.default === "true") {
            switchInput.setAttribute("checked", "");
        }
        let switchLabel = document.createElement("label");
        switchLabel.setAttribute("class", "form-check-label");
        switchLabel.setAttribute("for", "switch-" + config);
        switchLabel.innerText = configLabel;
        formSwitch.append(switchInput, switchLabel);
        colWrapper.append(formSwitch);
    } else if (options.choice) {
        let selectInput = createSelect(`select-${config}`, configLabel, "selectpicker form-control input-field input-configuration m-1");
        selectInput.setAttribute("configuration-parent", configKey);
        selectInput.setAttribute("configuration", options.configName);
        selectInput.setAttribute("title", `Select ${configLabel} option...`);
        selectInput.setAttribute("data-header", `Select ${configLabel} option...`);
        for (let choice of options.choice) {
            let option = document.createElement("option");
            option.setAttribute("value", choice.toString());
            option.innerText = choice.toString();
            if (choice === options["default"]) {
                option.setAttribute("selected", "");
            }
            selectInput.append(option);
        }
        colWrapper.append(selectInput);
        $(selectInput).selectpicker();
    } else {
        let formInput = createInput(`config-${config}`, config, "form-control input-field input-configuration", options.type, options["default"]);
        formInput.setAttribute("configuration-parent", configKey);
        formInput.setAttribute("configuration", options.configName);

        for (const [key, value] of Object.entries(options)) {
            if (key !== "default" && key !== "type" && key !== "choice" && key !== "help") {
                formInput.setAttribute(key, value);
            }
        }
        let formFloatingAttr = createFormFloating(configLabel, formInput);
        colWrapper.append(formFloatingAttr);
    }
    return colWrapper;
}