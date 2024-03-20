/*
Foreign keys section based off tasks created.
Ability to choose task name and columns. Define custom relationships.
- One to one
- One to many
- Transformations
 */
import {
    addAccordionCloseButton,
    createAccordionItem,
    createButton,
    createCloseButton,
    createFieldValidationCheck,
    createFormFloating,
    createInput,
    createSelect
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
    addForeignKeyButton.addEventListener("click", function () {
        numForeignKeys += 1;
        let newForeignKey = createForeignKey(numForeignKeys);
        foreignKeyAccordion.append(newForeignKey);
    });

    foreignKeyContainer.append(addForeignKeyButton, foreignKeyAccordion);
    return foreignKeyContainer;
}

export function createForeignKeysFromPlan(respJson) {
    if (respJson.foreignKeys) {
        let foreignKeysAccordion = document.querySelector(".foreign-keys-container").querySelector(".accordion");
        for (const foreignKey of respJson.foreignKeys) {
            numForeignKeys += 1;
            let newForeignKey = createForeignKey(numForeignKeys);
            foreignKeysAccordion.append(newForeignKey);

            if (foreignKey.source) {
                $(newForeignKey).find("select.foreign-key-source").selectpicker("val", foreignKey.source.taskName);
                $(newForeignKey).find("input.foreign-key-source").val(foreignKey.source.columns)[0].dispatchEvent(new Event("input"));
            }

            if (foreignKey.links) {
                // clear out default links
                let foreignKeyLinkSources = newForeignKey.querySelector(".foreign-key-link-sources");
                foreignKeyLinkSources.removeChild(foreignKeyLinkSources.querySelectorAll(".foreign-key-link-source")[0]);
                for (const fkLink of foreignKey.links) {
                    let newForeignKeyLink = createForeignKeyInput(numForeignKeysLinks, "foreign-key-link");
                    foreignKeyLinkSources.insertBefore(newForeignKeyLink, foreignKeyLinkSources.lastChild);
                    $(newForeignKeyLink).find("select.foreign-key-link").selectpicker("val", fkLink.taskName);
                    $(newForeignKeyLink).find("input.foreign-key-link").val(fkLink.columns)[0].dispatchEvent(new Event("input"));
                }
            }
        }
    }
}

export function getForeignKeys() {
    let foreignKeyContainers = Array.from(document.querySelectorAll(".foreign-key-container").values());
    return foreignKeyContainers.map(fkContainer => {
        let fkSource = $(fkContainer).find(".foreign-key-main-source");
        let fkSourceDetails = getForeignKeyDetail(fkSource[0]);
        let fkLinks = $(fkContainer).find(".foreign-key-link-source");
        let fkLinkArray = [];
        for (let fkLink of fkLinks) {
            let fkLinkDetails = getForeignKeyDetail(fkLink);
            fkLinkArray.push(fkLinkDetails);
        }
        return {source: fkSourceDetails, links: fkLinkArray};
    });
}

function createForeignKey(index) {
    let foreignKeyContainer = document.createElement("div");
    foreignKeyContainer.setAttribute("class", "foreign-key-container");
    // main source
    let mainSourceFkHeader = document.createElement("h5");
    mainSourceFkHeader.innerText = "Source";
    let mainSourceForeignKey = document.createElement("div");
    mainSourceForeignKey.setAttribute("class", "foreign-key-main-source");
    let mainForeignKeySource = createForeignKeyInput(index, "foreign-key-source");
    mainSourceForeignKey.append(mainForeignKeySource);
    // links to...
    let linkSourceFkHeader = document.createElement("h5");
    linkSourceFkHeader.innerText = "Links to";
    let linkSourceForeignKeys = document.createElement("div");
    linkSourceForeignKeys.setAttribute("class", "foreign-key-link-sources");
    let addLinkForeignKeyButton = createButton("add-foreign-key-link-btn", "add-link", "btn btn-secondary", "+ Link");
    addLinkForeignKeyButton.addEventListener("click", function () {
        numForeignKeysLinks += 1;
        let newForeignKeyLink = createForeignKeyInput(numForeignKeysLinks, "foreign-key-link");
        linkSourceForeignKeys.insertBefore(newForeignKeyLink, addLinkForeignKeyButton);
    });

    linkSourceForeignKeys.append(addLinkForeignKeyButton);
    numForeignKeysLinks += 1;
    let newForeignKeyLink = createForeignKeyInput(numForeignKeysLinks, "foreign-key-link");

    linkSourceForeignKeys.insertBefore(newForeignKeyLink, addLinkForeignKeyButton);
    let accordionItem = createAccordionItem(`foreign-key-${index}`, `Relationship ${index}`, "", foreignKeyContainer, "show");
    addAccordionCloseButton(accordionItem);
    foreignKeyContainer.append(mainSourceFkHeader, mainSourceForeignKey, linkSourceFkHeader, linkSourceForeignKeys);
    return accordionItem;
}

function updateForeignKeyTasks(taskNameSelect) {
    taskNameSelect.replaceChildren();
    let taskNames = Array.from(document.querySelectorAll(".task-name-field").values());
    for (const taskName of taskNames) {
        let selectOption = document.createElement("option");
        selectOption.setAttribute("value", taskName.value);
        selectOption.innerText = taskName.value;
        taskNameSelect.append(selectOption);
    }
    $(taskNameSelect).selectpicker("destroy").selectpicker("render");
}

function createForeignKeyInput(index, name) {
    let foreignKey = document.createElement("div");
    foreignKey.setAttribute("class", `row m-1 align-items-center ${name}-source`);
    // input is task name -> column(s)
    let taskNameSelect = createSelect(`${name}-${index}`, "Task", `selectpicker form-control input-field ${name}`);
    taskNameSelect.setAttribute("title", "Select a task...");
    taskNameSelect.setAttribute("data-header", "Select a task...");
    let taskNameCol = document.createElement("div");
    taskNameCol.setAttribute("class", "col");
    taskNameCol.append(taskNameSelect);

    let columnNamesInput = createInput(`${name}-column-${index}`, "Columns", `form-control input-field is-invalid ${name}`, "text", "");
    columnNamesInput.setAttribute("required", "");
    createFieldValidationCheck(columnNamesInput);
    let columnNameFloating = createFormFloating("Column(s)", columnNamesInput);

    foreignKey.append(taskNameCol, columnNameFloating);
    if (name === "foreign-key-link") {
        let closeButton = createCloseButton(foreignKey);
        foreignKey.append(closeButton);
    }
    $(taskNameSelect).selectpicker();
    // get the latest list of task names
    $(document).find(".task-name-field").on("change", function () {
        updateForeignKeyTasks(taskNameSelect);
    });
    updateForeignKeyTasks(taskNameSelect);
    // $(document).find(".task-name-field")[0].dispatchEvent(new Event("change"));
    return foreignKey;
}

function getForeignKeyDetail(element) {
    let taskName = $(element).find("select[aria-label=Task]").val();
    let columns = $(element).find("input[aria-label=Columns]").val();
    return {taskName: taskName, columns: columns};
}
