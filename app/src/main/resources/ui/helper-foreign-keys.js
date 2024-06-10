/*
Foreign keys section based off tasks created.
Ability to choose task name and columns. Define custom relationships.
- One to one
- One to many
- Transformations
 */
import {
    addAccordionCloseButton,
    addConnectionOverrideOptions,
    createAccordionItem,
    createButton,
    createCloseButton,
    createFieldValidationCheck,
    createFormFloating,
    createInput,
    createSelect,
    createTooltip,
    dispatchEvent,
    getOverrideConnectionOptionsAsMap,
    wait
} from "./shared.js";

export let numForeignKeys = 0;
export let numForeignKeysLinks = 0;

export function createForeignKeys() {
    let foreignKeyContainer = document.createElement("div");
    foreignKeyContainer.setAttribute("class", "foreign-keys-container");
    let foreignKeyAccordion = document.createElement("div");
    foreignKeyAccordion.setAttribute("class", "accordion mt-2");
    foreignKeyAccordion.setAttribute("style", "--bs-accordion-active-bg: mistyrose");

    let addForeignKeyButton = createButton("add-foreign-key-btn", "add-foreign-key", "btn btn-secondary", "+ Relationship");
    addForeignKeyButton.addEventListener("click", async function () {
        numForeignKeys += 1;
        let newForeignKey = await createForeignKey(numForeignKeys);
        foreignKeyAccordion.append(newForeignKey);
    });

    foreignKeyContainer.append(addForeignKeyButton, foreignKeyAccordion);
    return foreignKeyContainer;
}

async function createForeignKeyLinksFromPlan(newForeignKey, foreignKey, linkType) {
    // clear out default links
    let foreignKeyLinkSources = newForeignKey.querySelector(`.foreign-key-${linkType}-link-sources`);
    if (foreignKeyLinkSources.length) {
        foreignKeyLinkSources.removeChild(foreignKeyLinkSources.querySelectorAll(`.foreign-key-${linkType}-link-source`)[0]);
    }
    for (const fkLink of Array.from(foreignKey[`${linkType}Links`])) {
        let newForeignKeyLink = await createForeignKeyInput(numForeignKeysLinks, `foreign-key-${linkType}-link`);
        foreignKeyLinkSources.insertBefore(newForeignKeyLink, foreignKeyLinkSources.lastChild);
        let updatedForeignKeyTaskName = $(newForeignKeyLink).find(`select.foreign-key-${linkType}-link`).selectpicker("val", fkLink.taskName);
        dispatchEvent(updatedForeignKeyTaskName, "change");
        let updatedForeignKeyColumns = $(newForeignKeyLink).find(`input.foreign-key-${linkType}-link`).val(fkLink.columns);
        dispatchEvent(updatedForeignKeyColumns, "input");
        //also add in other options
        if (fkLink.options) {
            for (let [key, value] of Object.entries(fkLink.options)) {
                let fkConnectionProperty = $(newForeignKeyLink).find(`input.foreign-key-connection-property[aria-label=${key}]`);
                fkConnectionProperty.val(value);
                if (value && value.length > 0) {
                    fkConnectionProperty.attr("disabled", "");
                }
            }
        }
    }
}

export async function createForeignKeysFromPlan(respJson) {
    if (respJson.foreignKeys) {
        let foreignKeysAccordion = document.querySelector(".foreign-keys-container").querySelector(".accordion");
        for (const foreignKey of respJson.foreignKeys) {
            numForeignKeys += 1;
            let newForeignKey = await createForeignKey(numForeignKeys);
            foreignKeysAccordion.append(newForeignKey);

            if (foreignKey.source) {
                let updatedTaskName = $(newForeignKey).find("select.foreign-key-source").selectpicker("val", foreignKey.source.taskName);
                dispatchEvent(updatedTaskName, "change");
                let updatedColumns = $(newForeignKey).find("input.foreign-key-source").val(foreignKey.source.columns);
                dispatchEvent(updatedColumns, "input");
                //also add in other options
                console.log(foreignKey.source.options);
                if (foreignKey.source.options) {
                    for (let [key, value] of Object.entries(foreignKey.source.options)) {
                        let fkConnectionProperty = $(newForeignKey).find(`input.foreign-key-connection-property[aria-label=${key}]`);
                        fkConnectionProperty.val(value);
                        if (value && value.length > 0) {
                            fkConnectionProperty.attr("disabled", "");
                        }
                    }
                }
            }

            if (foreignKey.generationLinks) {
                await createForeignKeyLinksFromPlan(newForeignKey, foreignKey, "generation");
            }
            if (foreignKey.deleteLinks) {
                await createForeignKeyLinksFromPlan(newForeignKey, foreignKey, "delete");
            }
        }
    }
}

function getForeignKeyLinksToArray(foreignKeyContainer, className) {
    let mainContainer = $(foreignKeyContainer).find(className);
    let foreignKeyLinks = $(mainContainer).find(".foreign-key-input-container");
    let foreignKeyLinksArray = [];
    for (let foreignKeyLink of foreignKeyLinks) {
        let foreignKeyLinkDetails = getForeignKeyDetail(foreignKeyLink);
        if (Object.keys(foreignKeyLinkDetails).length !== 0) {
            foreignKeyLinksArray.push(foreignKeyLinkDetails);
        }
    }
    return foreignKeyLinksArray;
}

export function getForeignKeys() {
    let foreignKeyContainers = Array.from(document.querySelectorAll(".foreign-key-container").values());
    return foreignKeyContainers.map(fkContainer => {
        let fkSource = $(fkContainer).find(".foreign-key-main-source");
        let fkSourceDetails = getForeignKeyDetail(fkSource[0]);
        let fkGenerationLinkArray = getForeignKeyLinksToArray(fkContainer, ".foreign-key-generation-link-sources");
        let fkDeleteLinkArray = getForeignKeyLinksToArray(fkContainer, ".foreign-key-delete-link-sources");
        return {source: fkSourceDetails, generationLinks: fkGenerationLinkArray, deleteLinks: fkDeleteLinkArray};
    });
}

async function createForeignKeyLinks(index, linkType) {
    // links to either data generation link or delete link
    let buttonText = linkType.charAt(0).toUpperCase() + linkType.slice(1);
    let linkSourceFkHeader = document.createElement("h5");
    linkSourceFkHeader.innerText = "Links to";
    let linkSourceForeignKeys = document.createElement("div");
    linkSourceForeignKeys.setAttribute("class", `foreign-key-${linkType}-link-sources`);
    let addLinkForeignKeyButton = createButton(`add-foreign-key-${linkType}-link-btn-${index}`, "add-link", "btn btn-secondary", "+ Link");
    addLinkForeignKeyButton.addEventListener("click", async function () {
        numForeignKeysLinks += 1;
        if (linkSourceForeignKeys.childElementCount > 1) {
            let divider = document.createElement("hr");
            linkSourceForeignKeys.insertBefore(divider, addLinkForeignKeyButton);
        }
        let newForeignKeyLink = await createForeignKeyInput(numForeignKeysLinks, `foreign-key-${linkType}-link`);
        linkSourceForeignKeys.insertBefore(newForeignKeyLink, addLinkForeignKeyButton);
    });

    linkSourceForeignKeys.append(addLinkForeignKeyButton);

    let bodyContainer = document.createElement("div");
    bodyContainer.append(linkSourceFkHeader, linkSourceForeignKeys);
    return createAccordionItem(`foreign-key-${linkType}-${index}`, buttonText, "", bodyContainer);
}

async function createForeignKey(index) {
    let foreignKeyContainer = document.createElement("div");
    foreignKeyContainer.setAttribute("class", "foreign-key-container");
    // main source
    let mainSourceFkHeader = document.createElement("h5");
    mainSourceFkHeader.innerText = "Source";
    let mainSourceForeignKey = document.createElement("div");
    mainSourceForeignKey.setAttribute("class", "foreign-key-main-source");
    let mainForeignKeySource = await createForeignKeyInput(index, "foreign-key-source");
    mainSourceForeignKey.append(mainForeignKeySource);

    let foreignKeyLinkAccordion = document.createElement("div");
    foreignKeyLinkAccordion.setAttribute("class", "accordion mt-2");
    // foreignKeyLinkAccordion.setAttribute("style", "--bs-accordion-active-bg: mistyrose");

    let generationAccordionItem = await createForeignKeyLinks(index, "generation");
    let deleteAccordionItem = await createForeignKeyLinks(index, "delete");
    foreignKeyLinkAccordion.append(generationAccordionItem, deleteAccordionItem);

    let accordionItem = createAccordionItem(`foreign-key-${index}`, `Relationship ${index}`, "", foreignKeyContainer, "show");
    addAccordionCloseButton(accordionItem);
    foreignKeyContainer.append(mainSourceFkHeader, mainSourceForeignKey, foreignKeyLinkAccordion);
    return accordionItem;
}

async function updateForeignKeyTasks(taskNameSelect) {
    await wait(100);
    let previousSelectedVal = $(taskNameSelect).val();
    $(taskNameSelect).empty();
    let taskNames = Array.from(document.querySelectorAll(".task-name-field").values());
    for (const taskName of taskNames) {
        let selectOption = document.createElement("option");
        selectOption.setAttribute("value", taskName.value);
        selectOption.innerText = taskName.value;
        taskNameSelect.append(selectOption);
    }
    $(taskNameSelect).selectpicker("destroy").selectpicker("render");
    let hasPreviousSelectedVal = $(taskNameSelect).find(`[value="${previousSelectedVal}"]`);
    if (previousSelectedVal !== "" && hasPreviousSelectedVal.length) {
        $(previousSelectedVal).selectpicker("val", previousSelectedVal);
    }
}

async function createForeignKeyInput(index, name) {
    let foreignKeyContainer = document.createElement("div");
    foreignKeyContainer.setAttribute("class", "foreign-key-input-container m-1");
    let foreignKey = document.createElement("div");
    foreignKey.setAttribute("class", `row m-1 align-items-center ${name}-source`);
    // input is task name -> column(s)
    let taskNameSelect = createSelect(`${name}-${index}`, "Task", `selectpicker form-control input-field ${name}`, "Select a task...");
    let taskNameCol = document.createElement("div");
    taskNameCol.setAttribute("class", "col");
    taskNameCol.append(taskNameSelect);

    let columnNamesInput = createInput(`${name}-column-${index}`, "Columns", `form-control input-field is-invalid ${name}`, "text", "");
    columnNamesInput.setAttribute("required", "");
    createFieldValidationCheck(columnNamesInput);
    let columnNameFloating = createFormFloating("Column(s)", columnNamesInput);

    foreignKey.append(taskNameCol, columnNameFloating);
    //when task name is selected, offer input to define sub data source if not defined
    //(i.e. schema and table for Postgres task with no schema and table defined, only offer table if schema is defined in data source)
    //for a http data source, endpoint is not part of the data source
    //same logic can be shared for data generation/validation to allow re-use of connection
    let iconDiv = createTooltip();
    let overrideOptionsContainer = document.createElement("div");
    overrideOptionsContainer.setAttribute("class", "foreign-key-connection-container");
    taskNameSelect.addEventListener("change", (event) => {
        let taskName = event.target.value;
        //get the corresponding task data source connection name
        let taskNameInput = $(document).find(`input[class~=task-name-field]`).filter(function () {
            return this.value === taskName
        });
        let connectionName = $(taskNameInput).closest("[class~=row]").find("select[class~=data-connection-name]").val();
        addConnectionOverrideOptions(connectionName, iconDiv, overrideOptionsContainer, "foreign-key-connection-property", index)
            .then(() => {
                //check if override options already exist in tasks
                //TODO do we make a listener between the connection properties and the relationships properties to keep them in sync?
                console.log($(taskNameInput).closest("[class~=data-source-config-container]").find("input[class~=data-source-property]"));
                $(taskNameInput).closest("[class~=data-source-config-container]")
                    .find("input[class~=data-source-property]")
                    .each(function (i, obj) {
                        let fkConnectionProperty = $(overrideOptionsContainer).find(`input.foreign-key-connection-property[aria-label=${obj.getAttribute("aria-label")}]`);
                        fkConnectionProperty.val(obj.value);
                        if (obj.value && obj.value.length > 0) {
                            fkConnectionProperty.attr("disabled", "");
                        }
                    });
            });
    });
    if (name === "foreign-key-generation-link" || name === "foreign-key-delete-link") {
        let closeButton = createCloseButton(foreignKey);
        foreignKey.append(closeButton);
    }
    $(taskNameSelect).selectpicker();
    // get the latest list of task names
    $(document).find(".task-name-field").on("change", function () {
        updateForeignKeyTasks(taskNameSelect);
    });
    $(document).find("#add-task-button").on("click", function () {
        updateForeignKeyTasks(taskNameSelect);
    });
    await updateForeignKeyTasks(taskNameSelect);
    foreignKeyContainer.append(foreignKey, overrideOptionsContainer);
    return foreignKeyContainer;
}

function getForeignKeyDetail(element) {
    let taskName = $(element).find("select[aria-label=Task]").val();
    let columns = $(element).find("input[aria-label=Columns]").val();
    let baseForeignKey = {taskName: taskName, columns: columns};
    let overrideConnectionOptions = getOverrideConnectionOptionsAsMap(element);
    if (Object.keys(overrideConnectionOptions).length > 0) {
        baseForeignKey["options"] = overrideConnectionOptions;
    }
    return baseForeignKey;
}
