Map.prototype.toJSON = function () {
    let obj = {}
    for (let [key, value] of this)
        obj[key] = value
    return obj
}

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
    let closeButton = createButton("","Close", "col-md-auto btn-close");
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
    formText.innerText = text;
    if (span) {
        let spanCol = document.createElement("div");
        spanCol.setAttribute("class", "col-4");
        spanCol.append(formText);
        return spanCol;
    } else {
        return formText;
    }
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

export function createAccordionItem(index, buttonText, bodyText, bodyContainer) {
    let accordionItem = document.createElement("div");
    accordionItem.setAttribute("class", "accordion-item");
    let accordionHeader = document.createElement("h2");
    accordionHeader.setAttribute("class", "accordion-header");
    accordionHeader.setAttribute("id", "accordion-header-" + index);
    let accordionButton = document.createElement("button");
    accordionButton.setAttribute("class", "accordion-button collapsed");
    accordionButton.setAttribute("type", "button");
    accordionButton.setAttribute("data-bs-toggle", "collapse");
    accordionButton.setAttribute("data-bs-target", "#accordion-collapse-" + index);
    accordionButton.setAttribute("aria-expanded", "false");
    let accordionCollapse = document.createElement("div");
    accordionCollapse.setAttribute("class", "accordion-collapse collapse");
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
