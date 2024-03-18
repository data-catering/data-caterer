import {dataTypeOptionsMap, validationTypeOptionsMap} from "./configuration-data.js";
import {addColumnValidationBlock, createManualValidation, numValidations} from "./helper-validation.js";
import {createManualSchema, numFields} from "./helper-generation.js";

Map.prototype.toJSON = function () {
    let obj = {}
    for (let [key, value] of this)
        obj[key] = value
    return obj
}

let numAddAttributeButton = 0;
let numMenuButton = 0;

export function camelize(str) {
    return str.replace(/(?:^\w|[A-Z]|\b\w|\s+)/g, function (match, index) {
        if (+match === 0) return "";
        return index === 0 ? match.toLowerCase() : match.toUpperCase();
    });
}

export function createFormFloating(floatingText, inputAttr) {
    let formFloatingAttr = document.createElement("div");
    formFloatingAttr.setAttribute("class", "form-floating");
    let labelAttr = document.createElement("label");
    labelAttr.setAttribute("for", inputAttr.getAttribute("id"));
    labelAttr.innerText = floatingText;
    formFloatingAttr.append(inputAttr, labelAttr);
    let colWrapper = document.createElement("div");
    colWrapper.setAttribute("class", "col");
    colWrapper.append(formFloatingAttr);
    return colWrapper;
}

export function createBadge(text) {
    let badge = document.createElement("span");
    badge.setAttribute("class", "col badge bg-secondary m-2");
    badge.innerText = text;
    return badge;
}

export function createButton(id, label, classes, innerText) {
    let button = document.createElement("button");
    if (id.length > 0) {
        button.setAttribute("id", id);
    }
    button.setAttribute("aria-label", label);
    button.setAttribute("class", classes);
    button.setAttribute("type", "button");
    if (innerText) {
        button.innerText = innerText;
    }
    return button;
}

export function createCloseButton(closeElement) {
    let closeButton = createButton("", "Close", "col-md-auto btn-close");
    closeButton.addEventListener("click", function () {
        closeElement.remove();
    });
    return closeButton;
}

export function createSelect(id, label, classes) {
    let selectInput = document.createElement("select");
    selectInput.setAttribute("id", id);
    selectInput.setAttribute("class", classes);
    selectInput.setAttribute("aria-label", label);
    selectInput.setAttribute("placeholder", "");
    return selectInput;
}

export function createInput(id, label, classes, type, innerText) {
    let formInput = document.createElement("input");
    formInput.setAttribute("id", id);
    formInput.setAttribute("aria-label", label);
    formInput.setAttribute("class", classes);
    formInput.setAttribute("type", type);
    formInput.setAttribute("placeholder", innerText);
    formInput.setAttribute("value", innerText);
    formInput.innerText = innerText;
    return formInput;
}

export function createFormText(id, text, span) {
    let formText = span ? document.createElement("span") : document.createElement("div");
    formText.setAttribute("class", "form-text");
    formText.setAttribute("id", `${id}-form-text`);
    formText.innerHTML = text;
    if (span) {
        let spanCol = document.createElement("div");
        spanCol.setAttribute("class", "col");
        spanCol.append(formText);
        return spanCol;
    } else {
        return formText;
    }
}

export function createFieldValidationCheck(inputField) {
    inputField.addEventListener("input", (event) => {
        if (inputField.classList.contains("is-invalid") && event.target.value !== "") {
            inputField.classList.remove("is-invalid");
        } else if (!inputField.classList.contains("is-invalid") && event.target.value === "") {
            inputField.classList.add("is-invalid");
        }
    });
}

export function createInputGroup(leftElement, rightElement, col) {
    let colWrapper = document.createElement("div");
    colWrapper.setAttribute("class", col);
    let inputGroup = document.createElement("div");
    inputGroup.setAttribute("class", "input-group");
    inputGroup.append(leftElement, rightElement);
    colWrapper.append(inputGroup);
    return colWrapper;
}

export function createAccordionItem(index, buttonText, bodyText, bodyContainer, show) {
    let buttonClass = show ? "accordion-button" : "accordion-button collapsed";
    let ariaExpanded = show ? "true" : "false";
    let collapseClass = show ? "accordion-collapse collapse show" : "accordion-collapse collapse";

    let accordionItem = document.createElement("div");
    accordionItem.setAttribute("class", "accordion-item");
    let accordionHeader = document.createElement("h2");
    accordionHeader.setAttribute("class", "accordion-header");
    accordionHeader.setAttribute("id", "accordion-header-" + index);
    let accordionButton = document.createElement("button");
    accordionButton.setAttribute("class", buttonClass);
    accordionButton.setAttribute("type", "button");
    accordionButton.setAttribute("data-bs-toggle", "collapse");
    accordionButton.setAttribute("data-bs-target", "#accordion-collapse-" + index);
    accordionButton.setAttribute("aria-expanded", ariaExpanded);
    let accordionCollapse = document.createElement("div");
    accordionCollapse.setAttribute("class", collapseClass);
    accordionCollapse.setAttribute("id", "accordion-collapse-" + index);
    accordionCollapse.setAttribute("aria-labelledby", "accordion-header-" + index);
    let accordionBody = document.createElement("div");
    accordionBody.setAttribute("class", "accordion-body");
    accordionButton.innerText = buttonText;
    if (bodyText !== "") {
        accordionBody.innerText = bodyText;
    }
    if (bodyContainer) {
        accordionBody.append(bodyContainer);
    }

    accordionCollapse.append(accordionBody);
    accordionHeader.append(accordionButton);
    accordionItem.append(accordionHeader, accordionCollapse);
    return accordionItem;
}

export function createRadioButtons(index, name, options) {
    let radioButtonContainer = document.createElement("div");
    radioButtonContainer.setAttribute("id", `${name}-${index}`);
    radioButtonContainer.setAttribute("radioGroup", `${name}-${index}`);
    for (const [i, option] of options.entries()) {
        let snakeCaseOption = option.text.toLowerCase().replaceAll(" ", "-");
        let formCheck = document.createElement("div");
        formCheck.setAttribute("class", "form-check form-check-inline ");
        let formInput = document.createElement("input");
        formInput.setAttribute("class", `form-check-input ${name} ${snakeCaseOption}`);
        formInput.setAttribute("type", "radio");
        formInput.setAttribute("name", `${name}-${index}`);
        formInput.setAttribute("id", `radio-${name}-${index}-${i}`);
        if (i === 0) {
            formInput.setAttribute("checked", "");
        }
        let formLabel = document.createElement("label");
        formLabel.setAttribute("class", "form-check-label");
        formLabel.setAttribute("for", `radio-${name}-${index}-${i}`);
        formLabel.innerText = option.text;

        formCheck.append(formInput, formLabel);
        if (option.child) formCheck.append(option.child);
        radioButtonContainer.append(formCheck);
    }
    return radioButtonContainer;
}

export function formatDate(isMin, isTimestamp) {
    let currentDate = new Date();
    if (isMin) {
        currentDate.setDate(currentDate.getDate() - 365);
    }
    return isTimestamp ? currentDate.toISOString() : currentDate.toISOString().split("T")[0];
}

const dataAndValidationTypeBaseOptions = new Map();
dataAndValidationTypeBaseOptions.set("validation-type", {
    elementQuerySelector: ".validation-type",
    menuAttributeName: "current-validation-type",
    inputClass: "data-validation-field",
    optionsMap: validationTypeOptionsMap,
});
dataAndValidationTypeBaseOptions.set("data-type", {
    elementQuerySelector: ".field-type",
    menuAttributeName: "current-data-type",
    inputClass: "data-source-field",
    optionsMap: dataTypeOptionsMap,
});

// Create a button for overriding attributes of the data field based on data type, i.e. set min to 10 for integer
export function createDataOrValidationTypeAttributes(element, elementType) {
    let elementContainer = element.parentElement;
    let defaultAttributeClass = "default-attribute";
    let typeOptions = dataAndValidationTypeBaseOptions.get(elementType);

    //maybe on hover, show defaults
    let containerId = element.getAttribute("id");
    numAddAttributeButton += 1;
    let {buttonWithMenuDiv, addAttributeButton, menu} = createButtonWithMenu(element);
    element.append(buttonWithMenuDiv);
    // element that decides what attributes are available in the menu
    let menuDeciderElement = element.querySelector(typeOptions.elementQuerySelector);

    // when menu decider is changed, previous attributes should be removed from the `element` children
    // there many also be default required children added, 'defaultChild' prefix
    menuDeciderElement.addEventListener("change", (event) => {
        let optionAttributes = typeOptions.optionsMap.get(event.target.value);
        // remove default children from container
        let defaultAddedElements = Array.from(element.querySelectorAll("." + defaultAttributeClass).values());
        for (let defaultAddedElement of defaultAddedElements) {
            // TODO check if userAddedElement is compatible with new 'type' (i.e. change data type from int to long should keep min)
            element.removeChild(defaultAddedElement);
        }
        // remove user added children from container
        let userAddedElements = Array.from(elementContainer.querySelectorAll(".user-added-attribute").values());
        for (let userAddedElement of userAddedElements) {
            elementContainer.removeChild(userAddedElement);
        }

        // add in default attributes that are required
        if (optionAttributes) {
            let defaultChildKeys = Object.keys(optionAttributes).filter(k => k.startsWith("defaultChild"));
            for (let defaultChildKey of defaultChildKeys) {
                let defaultChildAttributes = optionAttributes[defaultChildKey];
                let attribute = defaultChildKey.replace("defaultChild", "");
                let attributeContainerId = `${containerId}-${attribute}`;
                let inputAttribute = createAttributeFormFloating(defaultChildAttributes, attributeContainerId, typeOptions.inputClass, attribute, `col ${defaultAttributeClass}`);
                element.insertBefore(inputAttribute, buttonWithMenuDiv);
            }
            // add in any additional blocks to the main container
            // for example, when validation is upstream, add in block for validations based on upstream
            if (optionAttributes["addBlock"]) {
                if (optionAttributes["addBlock"].type === "validation") {
                    // need to add in new validation button and accordion for upstream validations
                    let newValidationBox = createManualValidation(numValidations, "nested-validation");
                    elementContainer.append(newValidationBox);
                } else if (optionAttributes["addBlock"].type === "field") {
                    // need to add in new field button to allow for nested fields
                    let newFieldBox = createManualSchema(numFields, "struct-schema");
                    elementContainer.append(newFieldBox);
                }
            } else {
                // if there exists additional blocks, remove them
                $(elementContainer).find("[id$=nested-validation],[id$=struct-schema]").remove();
            }
        }
    });
    // trigger when loaded to ensure default child is there at start
    // menuDeciderElement.dispatchEvent(new Event("change"));

    // when click on the add attributes button, show menu of attributes
    addClickListenForAttributeMenu(addAttributeButton, menu, element, typeOptions.elementQuerySelector, typeOptions.menuAttributeName, typeOptions.optionsMap);

    // when attribute in menu is clicked, create text box with that attribute and its default value before the add button
    menu.addEventListener("click", (event) => {
        event.preventDefault();
        let attribute = event.target.getAttribute("value");
        // check if attribute already exists
        let attributeContainerId = containerId + "-" + attribute;
        if ($(elementContainer).find(`[aria-label=${attribute}]`).length === 0) {
            // get default value for data type attribute along with other metadata
            let currentMenuType = menu.getAttribute(typeOptions.menuAttributeName);
            let currentTypeAttributes = typeOptions.optionsMap.get(currentMenuType);
            let attrMetadata = currentTypeAttributes[attribute];
            addNewDataTypeAttribute(attribute, attrMetadata, attributeContainerId, typeOptions.inputClass, elementContainer);
        }
    });
}

export function createButtonWithMenu(element) {
    numMenuButton += 1;
    let buttonWithMenuDiv = document.createElement("div");
    buttonWithMenuDiv.setAttribute("class", "col dropdown");
    let addAttributeId = `${element.getAttribute("id")}-add-attribute-button-${numMenuButton}`;
    let addAttributeButton = createAddAttributeButton(addAttributeId);
    buttonWithMenuDiv.append(addAttributeButton);

    // menu of attribute based on data type
    let menu = document.createElement("ul");
    menu.setAttribute("class", "dropdown-menu");
    menu.setAttribute("aria-labelledby", addAttributeId);
    menu.setAttribute("id", `${element.getAttribute("id")}-add-attribute-menu-${numMenuButton}`);
    buttonWithMenuDiv.append(menu);
    return {buttonWithMenuDiv, addAttributeButton, menu};
}

export function createAddAttributeButton(addAttributeId) {
    let addAttributeButton = createButton(addAttributeId, "add-attribute", "btn btn-secondary dropdown-toggle");
    addAttributeButton.setAttribute("data-bs-toggle", "dropdown");
    addAttributeButton.setAttribute("aria-expanded", "false");
    let addIcon = document.createElement("i");
    addIcon.setAttribute("class", "fa fa-plus");
    addAttributeButton.append(addIcon);
    return addAttributeButton;
}

export function addItemsToAttributeMenu(attributesForDataType, menu) {
    for (let [key] of Object.entries(attributesForDataType)) {
        if (!key.startsWith("defaultChild") && key !== "addBlock") {
            let menuItem = document.createElement("li");
            menuItem.setAttribute("id", key);
            menuItem.setAttribute("value", key);
            let menuItemContent = document.createElement("button");
            menuItemContent.setAttribute("class", "dropdown-item");
            menuItemContent.setAttribute("type", "button");
            menuItemContent.setAttribute("value", key);
            menuItemContent.innerText = key;
            menuItem.append(menuItemContent);
            menu.append(menuItem);
        }
    }
}

export function addNewDataTypeAttribute(attribute, attrMetadata, attributeContainerId, inputClass, elementContainer) {
    let hasAddBlockColumn = attrMetadata["addBlock"] && attrMetadata["addBlock"].type === "column";
    // add attribute field to field container
    let newAttributeRow = document.createElement("div");
    newAttributeRow.setAttribute("class", "row g-3 m-1 align-items-center user-added-attribute");

    if (attrMetadata["type"] === "min-max") {
        let formFloatingAttrMin = createAttributeFormFloating(attrMetadata, attributeContainerId, inputClass, attribute + "Min", "col-4");
        let formFloatingAttrMax = createAttributeFormFloating(attrMetadata, attributeContainerId, inputClass, attribute + "Max", "col-3");
        newAttributeRow.append(formFloatingAttrMin, formFloatingAttrMax);
        if (attrMetadata.help) {
            let helpDiv = createFormText(formFloatingAttrMin.getAttribute("id"), attrMetadata.help, "span");
            formFloatingAttrMin.setAttribute("aria-describedby", helpDiv.getAttribute("id"));
            newAttributeRow.append(helpDiv);
        }
        if (!hasAddBlockColumn) elementContainer.append(newAttributeRow);
    } else {
        let formFloatingAttr = createAttributeFormFloating(attrMetadata, attributeContainerId, inputClass, attribute, "col-7");
        newAttributeRow.append(formFloatingAttr);
        if (attrMetadata.help) {
            let helpDiv = createFormText(formFloatingAttr.getAttribute("id"), attrMetadata.help, "span");
            formFloatingAttr.setAttribute("aria-describedby", helpDiv.getAttribute("id"));
            newAttributeRow.append(helpDiv);
        }
        if (!hasAddBlockColumn) elementContainer.append(newAttributeRow);
    }

    if (hasAddBlockColumn) {
        // need to use card for appending to mainContainer
        addColumnValidationBlock(newAttributeRow, elementContainer, attributeContainerId, inputClass);
    } else {
        let closeButton = createCloseButton(newAttributeRow);
        let closeCol = document.createElement("div");
        closeCol.setAttribute("class", "col-md-auto");
        closeCol.append(closeButton);
        newAttributeRow.append(closeCol);
    }
}

// get first level elements of class
export function findNextLevelNodesByClass($elem, className, notInsideClassName) {
    let nodesArray = [];

    function getRecursiveNextDom($rootNode) {
        $rootNode.children().each((i, element) => {
            if (notInsideClassName && notInsideClassName.some(c => $(element).hasClass(c))) {
                //do nothing
            } else if ($(element).hasClass(className)) {
                nodesArray.push(element)
            } else {
                getRecursiveNextDom($(element));
            }
        })
    }

    getRecursiveNextDom($elem, className);
    return nodesArray;
}

function createAttributeFormFloating(attrMetadata, attributeContainerId, inputClass, attribute, col) {
    let inputAttr;
    if (attrMetadata.choice) {
        let baseInput = createSelect(attributeContainerId, attribute, `selectpicker form-control input-field ${inputClass}`);
        for (let choice of attrMetadata.choice) {
            let option = document.createElement("option");
            option.setAttribute("value", choice.toString());
            option.innerText = choice.toString();
            if (choice === attrMetadata["default"]) {
                option.setAttribute("selected", "");
            }
            baseInput.append(option);
        }
        // doesn't need form floating
        let inputCol = document.createElement("div");
        inputCol.setAttribute("class", "col");
        inputCol.append(baseInput);
        $(baseInput).selectpicker();
        inputAttr = inputCol;
    } else if (attrMetadata["type"] === "badge") {
        return createBadge(attribute);
    } else {
        inputAttr = createInput(attributeContainerId, attribute, `form-control input-field ${inputClass}`, attrMetadata["type"], attrMetadata["default"]);
        if (attrMetadata["disabled"] !== "") {
            if (!attrMetadata["default"] || attrMetadata["default"] === "") {
                inputAttr.classList.add("is-invalid");
                createFieldValidationCheck(inputAttr);
            }
            inputAttr.setAttribute("required", "");
        }
        inputAttr.dispatchEvent(new Event("input"));
    }

    for (const [key, value] of Object.entries(attrMetadata)) {
        if (key !== "default" && key !== "type" && key !== "choice" && key !== "help" && key !== "group" && key !== "addBlock") {
            inputAttr.setAttribute(key, value);
        }
    }
    let formFloatingInput = attrMetadata.choice ? inputAttr : createFormFloating(attribute, inputAttr);
    // if group is defined, there is additional input required
    // if help is defined, add to container
    if (attrMetadata.group) {
        let startInputDiv = document.createElement("div");
        startInputDiv.setAttribute("class", "input-group-text");
        let startInput = createInput(`${attributeContainerId}-option-input`, attribute, "form-check-input mt-0", attrMetadata.group.type);
        if (attrMetadata.group.type === "checkbox" && attrMetadata.group.checked) startInput.setAttribute("checked", "");

        let startInputLabel = document.createElement("label");
        startInputLabel.setAttribute("class", "form-check-label");
        startInputLabel.setAttribute("for", `${attributeContainerId}-option-input`);
        startInputLabel.innerText = attrMetadata.group.innerText;
        startInputDiv.append(startInput, startInputLabel);
        return createInputGroup(startInputDiv, formFloatingInput, col ? col : "col-8");
    } else {
        if (col) {
            formFloatingInput.setAttribute("class", col);
        }
        return formFloatingInput;
    }
}

function addClickListenForAttributeMenu(addAttributeButton, menu, element, elementQuerySelector, menuAttributeName, optionsMap) {
    addAttributeButton.addEventListener("click", (event) => {
        event.preventDefault();
        menu.open = !menu.open;
        // get current value of the data/validation type
        let currentType = $(element).find("select").val();
        let currentMenuType = menu.getAttribute(menuAttributeName);
        if (currentType !== null && (currentMenuType === null || currentMenuType !== currentType)) {
            // clear out menu items
            menu.replaceChildren();
            let attributesForDataType = optionsMap.get(currentType);
            // menu items are the different attribute available for the type selected
            if (attributesForDataType) {
                addItemsToAttributeMenu(attributesForDataType, menu);
            }
            menu.setAttribute(menuAttributeName, currentType);
        }
    });
}

export function createButtonGroup(...args) {
    let divContainer = document.createElement("div");
    divContainer.setAttribute("class", "d-flex position-absolute");
    divContainer.setAttribute("style", "right:10px; z-index:3;");
    let buttonGroup = document.createElement("div");
    buttonGroup.setAttribute("class", "btn-group");
    buttonGroup.setAttribute("role", "group");
    buttonGroup.setAttribute("aria-label", "plan-control-group");

    buttonGroup.append(...args);
    divContainer.append(buttonGroup);
    return divContainer;
}

export function addAccordionCloseButton(accordionItem) {
    let header = accordionItem.querySelector(".accordion-header");
    header.classList.add("position-relative");
    let closeButton = createCloseButton(accordionItem);
    closeButton.setAttribute("style", "width: 3px; height: 3px;");
    let buttonGroup = createButtonGroup(closeButton);
    let divContainer = document.createElement("div");
    divContainer.setAttribute("class", "d-flex align-items-center");
    divContainer.append(header.firstChild, buttonGroup);
    header.replaceChildren(divContainer);
}

export function createToast(header, message, type) {
    let toast = document.createElement("div");
    toast.setAttribute("class", "toast");
    toast.setAttribute("role", "alert");
    toast.setAttribute("aria-live", "assertive");
    toast.setAttribute("aria-atomic", "true");
    let toastHeader = document.createElement("div");
    toastHeader.setAttribute("class", "toast-header mr-2");
    let icon = document.createElement("i");
    if (type === "success") {
        icon.setAttribute("class", "bi bi-check-square-fill");
        icon.setAttribute("style", "color: green");
    } else if (type === "fail") {
        icon.setAttribute("class", "bi bi-exclamation-square-fill");
        icon.setAttribute("style", "color: red");
    } else {
        icon.setAttribute("class", "bi bi-caret-right-square-fill");
        icon.setAttribute("style", "color: orange");
    }
    let strong = document.createElement("strong");
    strong.setAttribute("class", "me-auto");
    strong.innerText = header;
    let small = document.createElement("small");
    small.innerText = "Now";
    let button = createButton("", "Close", "btn-close");
    button.setAttribute("data-bs-dismiss", "toast");
    let toastBody = document.createElement("div");
    toastBody.setAttribute("class", "toast-body");
    toastBody.innerText = message;
    toastHeader.append(icon, strong, small, button);
    toast.append(toastHeader, toastBody);
    document.getElementById("toast-container").append(toast);
    return toast;
}
