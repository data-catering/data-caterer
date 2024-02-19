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
