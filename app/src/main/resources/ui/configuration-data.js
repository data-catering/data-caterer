import {formatDate} from "./shared.js";


const baseDataTypes = ["string", "integer", "long", "short", "decimal", "double", "float", "date", "timestamp", "binary", "array", "struct"];
export const dataTypeOptionsMap = new Map();
const defaultDataTypeOptions = {
    enableEdgeCases: {
        default: "false",
        type: "text",
        choice: ["true", "false"],
        help: "Enable generating edge case values for data type."
    },
    edgeCaseProbability: {
        default: 0.0,
        type: "number",
        min: 0.0,
        max: 1.0,
        step: 0.001,
        help: "Probability of generating edge case values. Range from 0-1."
    },
    isUnique: {default: "false", type: "text", choice: ["true", "false"], help: "Generate only unique values."},
    seed: {
        default: -1,
        type: "number",
        min: -1,
        max: 9223372036854775807,
        help: "Seed for generating consistent random values."
    },
    sql: {default: "", type: "text", help: "SQL expression for generating data."},
    oneOf: {default: [], type: "text", help: "Generated values will be one of the defined values. Comma separated."},
    omit: {
        default: "false",
        type: "text",
        choice: ["true", "false"],
        help: "Exclude the column from the final output. Can be used for intermediate data generation."
    },
};

function getNumberOptions(min, max) {
    let minMaxOpt = min && max ? {min: min, max: max} : {};
    return {
        min: {default: 0, type: "number", ...minMaxOpt, help: "Minimum generated value."},
        max: {default: 1000, type: "number", ...minMaxOpt, help: "Maximum generated value."},
        stddev: {
            default: 1.0,
            type: "number",
            min: 0.0,
            max: 100000000.0,
            help: "Standard deviation of generated values."
        },
        mean: {default: 500, type: "number", ...minMaxOpt, help: "Mean of generated values."}
    };
}

dataTypeOptionsMap.set("string", {
    ...defaultDataTypeOptions,
    minLen: {default: 1, type: "number", min: 0, max: 1000, help: "Minimum length of generated values."},
    maxLen: {default: 10, type: "number", min: 0, max: 1000, help: "Maximum length of generated values."},
    expression: {default: "", type: "text", help: "Faker expression to generate values."},
    enableNull: {default: "false", type: "text", choice: ["true", "false"], help: "Enable generation of null values."},
    nullProbability: {
        default: 0.0,
        type: "number",
        min: 0.0,
        max: 1.0,
        step: 0.001,
        help: "Probability of generating null values. Range from 0-1."
    },
    regex: {default: "", type: "text", help: "Regex for generating values."}
});
dataTypeOptionsMap.set("integer", {...defaultDataTypeOptions, ...getNumberOptions(-2147483648, 2147483647)});
dataTypeOptionsMap.set("long", {...defaultDataTypeOptions, ...getNumberOptions(-9223372036854775808, 9223372036854775807)});
dataTypeOptionsMap.set("short", {...defaultDataTypeOptions, ...getNumberOptions(-32768, 32767)});
dataTypeOptionsMap.set("decimal", {
    ...defaultDataTypeOptions,
    ...getNumberOptions(),
    numericPrecision: {
        default: 10,
        type: "number",
        min: 0,
        max: 2147483647,
        help: "Precision for generated decimal values."
    },
    numericScale: {default: 0, type: "number", min: 0, max: 2147483647, help: "Scale for geneated decimal values."}
});
dataTypeOptionsMap.set("double", {...defaultDataTypeOptions, ...getNumberOptions()});
dataTypeOptionsMap.set("float", {...defaultDataTypeOptions, ...getNumberOptions()});
dataTypeOptionsMap.set("date", {
    ...defaultDataTypeOptions,
    min: {
        default: formatDate(true),
        type: "date",
        min: "0001-01-01",
        max: "9999-12-31",
        help: "Minimum date of generated values. Expected format 'yyyy-MM-dd'."
    },
    max: {
        default: formatDate(false),
        type: "date",
        min: "0001-01-01",
        max: "9999-12-31",
        help: "Maximum date of generated values. Expected format 'yyyy-MM-dd'."
    }
});
dataTypeOptionsMap.set("timestamp", {
    ...defaultDataTypeOptions,
    min: {
        default: formatDate(true, true),
        type: "datetime-local",
        min: "0001-01-01 00:00:00",
        max: "9999-12-31 23:59:59",
        help: "Minimum timestamp of generated values. Expected format 'yyyy-MM-dd HH:mm:ss'."
    },
    max: {
        default: formatDate(false, true),
        type: "datetime-local",
        min: "0001-01-01 00:00:00",
        max: "9999-12-31 23:59:59",
        help: "Maximum timestamp of generated values. Expected format 'yyyy-MM-dd HH:mm:ss'."
    }
});
dataTypeOptionsMap.set("binary", {
    ...defaultDataTypeOptions,
    minLen: {default: 1, type: "number", min: 0, max: 2147483647, help: "Minimum length of generated values."},
    maxLen: {default: 20, type: "number", min: 0, max: 2147483647, help: "Maximum length of generated values."},
});
dataTypeOptionsMap.set("array", {
    ...defaultDataTypeOptions,
    arrayMinLen: {default: 0, type: "number", min: 0, max: 2147483647, help: "Minimum generated array length."},
    arrayMaxLen: {default: 5, type: "number", min: 0, max: 2147483647, help: "Maximum generated array length."},
    arrayType: {default: "string", type: "text", choice: baseDataTypes, help: "Data type of array values."}
});
dataTypeOptionsMap.set("struct", {...defaultDataTypeOptions, addBlock: {type: "field"}});


export const validationTypeOptionsMap = new Map();
const defaultValidationOptions = {
    description: {default: "", type: "text", help: "Description of validation. Used in report."},
    errorThreshold: {
        default: 0.0,
        type: "number",
        min: 0.0,
        help: "Number or percentage (0.0 to 1.0) of errors before marking validation as failed."
    },
}
validationTypeOptionsMap.set("column", {
    ...defaultValidationOptions,
    defaultChildColumn: {default: "", type: "text", required: "", help: "Column to validate."},
    equal: {
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Not"},
        help: "Equal to value. Select 'Not' for not equals."
    },
    null: {default: "", type: "text", disabled: ""},
    notNull: {default: "", type: "text", disabled: ""},
    contains: {
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Not"},
        help: "Contains value. Select 'Not' for not contains."
    },
    unique: {default: "", type: "text"},
    lessThan: {
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Equal"},
        help: "Less than value. Select 'Equal' for less than or equal to."
    },
    greaterThan: {
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Equal"},
        help: "Greater than value. Select 'Equal' for greater than or equal to."
    },
    between: {
        default: "",
        type: "min-max",
        group: {type: "checkbox", innerText: "Not"},
        help: "Between values. Select 'Not' for not between."
    },
    in: {default: "", type: "text", group: {type: "checkbox", innerText: "Not"}, help: "In set of values. Select 'Not' for not in set."},
    matches: {
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Not"},
        help: "Matches regex. Select 'Not' for not matches regex."
    },
    startsWith: {
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Not"},
        help: "Starts with value. Select 'Not' for not starts with."
    },
    endsWith: {
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Not"},
        help: "Ends with value. Select 'Not' for not ends with."
    },
    size: {
        default: 0,
        type: "number",
        group: {type: "checkbox", innerText: "Not"},
        help: "Equal to size. Select 'Not' for not equal to size."
    },
    lessThanSize: {
        default: 0,
        type: "number",
        group: {type: "checkbox", innerText: "Equal"},
        help: "Less than size. Select 'Equal' for less than or equal to size."
    },
    greaterThanSize: {
        default: 0,
        type: "number",
        group: {type: "checkbox", innerText: "Equal"},
        help: "Greater than size. Select 'Equal' for greater than or equal to size."
    },
    luhnCheck: {default: "", type: "text", disabled: ""},
    hasType: {default: "string", type: "text", choice: baseDataTypes},
    sql: {default: "", type: "text"},
});
validationTypeOptionsMap.set("groupBy", {
    ...defaultValidationOptions,
    defaultChildGroupByColumns: {
        default: "",
        type: "text",
        required: "",
        help: "Column name(s) to group by. Comma separated."
    },
    count: {
        default: "",
        type: "text",
        help: "Column name to count number of groups after group by.",
        addBlock: {type: "column"}
    },
    sum: {default: "", type: "text", help: "Column name of values to sum after group by.", addBlock: {type: "column"}},
    min: {default: "", type: "text", help: "Column name to find minimum value after group by.", addBlock: {type: "column"}},
    max: {default: "", type: "text", help: "Column name to find maximum value after group by.", addBlock: {type: "column"}},
    average: {default: "", type: "text", help: "Column name to find average value after group by.", addBlock: {type: "column"}},
    standardDeviation: {
        default: "",
        type: "text",
        help: "Column name to find standard deviation value after group by.",
        addBlock: {type: "column"}
    },
});
validationTypeOptionsMap.set("upstream", {
    ...defaultValidationOptions,
    defaultChildUpstreamTaskName: {
        default: "",
        type: "text",
        required: "",
        selector: ".task-name-field",
        help: "Name of upstream data generation task."
    },
    addBlock: {type: "validation"},
    joinColumns: {default: "", type: "text", help: "Column name(s) to join by."},
    joinType: {
        default: "outer",
        type: "text",
        choice: ["inner", "outer", "left_outer", "right_outer", "left_semi", "anti", "cross"],
        help: "Type of join."
    },
    joinExpr: {default: "", type: "text", help: "Custom join SQL expression."}
});
validationTypeOptionsMap.set("columnNames", {
    ...defaultValidationOptions,
    countEqual: {default: 0, type: "number", help: "Number of columns has to equal value."},
    countBetween: {
        default: 0,
        type: "min-max",
        help: "Number of columns has to be between min and max value (inclusive)."
    },
    matchOrder: {
        default: "",
        type: "text",
        help: "All column names match particular ordering and is complete. Comma separated."
    },
    matchSet: {
        default: "",
        type: "text",
        help: "Column names contains set of expected names. Order is not checked. Comma separated."
    },
});


export const configurationOptionsMap = new Map();
configurationOptionsMap.set("flag", {
    "enableCount": {
        configName: "enableCount",
        displayName: "Count",
        default: "true",
        type: "text",
        choice: ["true", "false"],
        help: "Count the number of records generated. Can be disabled to improve performance."
    },
    "enableGenerateData": {
        configName: "enableGenerateData",
        displayName: "Generate Data",
        default: "true",
        type: "text",
        choice: ["true", "false"],
        help: "Enable/disable data generation."
    },
    "enableFailOnError": {
        configName: "enableFailOnError",
        displayName: "Fail On Error",
        default: "true",
        type: "text",
        choice: ["true", "false"],
        help: "Whilst saving generated data, if there is an error, it will stop any further data from being generated."
    },
    "enableUniqueCheck": {
        configName: "enableUniqueCheck",
        displayName: "Unique Check",
        default: "false",
        type: "text",
        choice: ["true", "false"],
        help: "Enable/disable generating unique values for columns marked as unique. Can be disabled to improve performance but not guarantee uniqueness."
    },
    "enableSinkMetadata": {
        configName: "enableSinkMetadata",
        displayName: "Sink Metadata",
        default: "false",
        type: "text",
        choice: ["true", "false"],
        help: "Run data profiling for the generated data. Shown in HTML reports if enabled."
    },
    "enableSaveReports": {
        configName: "enableSaveReports",
        displayName: "Save Report",
        default: "true",
        type: "text",
        choice: ["true", "false"],
        help: "Enable/disable HTML report summarising data generated, metadata of data generated (if <code>Sink Metadata</code> is enabled) and validation results (if <code>Validation</code> is enabled)."
    },
    "enableValidation": {
        configName: "enableValidation",
        displayName: "Validation",
        default: "true",
        type: "text",
        choice: ["true", "false"],
        help: "Run validations as described in plan. Results can be viewed from logs or from HTML report if <code>Sink Metadata</code> is enabled."
    },
    "enableAlerts": {
        configName: "enableAlerts",
        displayName: "Alerts",
        default: "true",
        type: "text",
        choice: ["true", "false"],
        help: "Enable/disable alerts being sent when plan execution is finished (can be configured for success/failure)."
    },
    "enableGenerateValidations": {
        configName: "enableGenerateValidations",
        displayName: "Generate Validations",
        default: "false",
        type: "text",
        choice: ["true", "false"],
        paid: "true",
        help: "Enable/disable automatically generating validations based on the data sources defined."
    },
    "enableRecordTracking": {
        configName: "enableRecordTracking",
        displayName: "Record Tracking",
        default: "false",
        type: "text",
        choice: ["true", "false"],
        paid: "true",
        help: "Enable/disable tracking of data records generated."
    },
    "enableDeleteGeneratedRecords": {
        configName: "enableDeleteGeneratedRecords",
        displayName: "Delete Generated Records",
        default: "false",
        type: "text",
        choice: ["true", "false"],
        paid: "true",
        help: "Delete all generated records based off record tracking (if <code>Record Tracking</code> has been set to true whilst generating)."
    },
    "enableGeneratePlanAndTasks": {
        configName: "enableGeneratePlanAndTasks",
        displayName: "Generate Plan And Tasks",
        default: "false",
        type: "text",
        choice: ["true", "false"],
        paid: "true",
        help: "Enable/disable plan and task automatic generation based off data source connections."
    },
});
configurationOptionsMap.set("folder", {
    "generatedReportsFolderPath": {
        configName: "generatedReportsFolderPath",
        displayName: "Generated Reports Folder Path",
        default: "",
        type: "text",
        help: "Folder path where generated HTML reports will be saved."
    },
    "validationFolderPath": {
        configName: "validationFolderPath",
        displayName: "Validation Folder Path",
        default: "",
        type: "text",
        help: "If using YAML validation file(s), folder path that contains all validation files (can have nested directories)."
    },
    "planFilePath": {
        configName: "planFilePath",
        displayName: "Plan File Path",
        default: "",
        type: "text",
        help: "If using YAML plan file, path to use when generating and/or validating data."
    },
    "taskFolderPath": {
        configName: "taskFolderPath",
        displayName: "Task Folder Path",
        default: "",
        type: "text",
        help: "If using YAML task file(s), folder path that contains all the task files (can have nested directories)."
    },
    "generatedPlanAndTasksFolderPath": {
        configName: "generatedPlanAndTasksFolderPath",
        displayName: "Generated Plan And Tasks Folder Path",
        default: "",
        type: "text",
        paid: "true",
        help: "Folder path where generated plan and task files will be saved."
    },
    "recordTrackingFolderPath": {
        configName: "recordTrackingFolderPath",
        displayName: "Record Tracking Folder Path",
        default: "",
        type: "text",
        paid: "true",
        help: "Folder path where record tracking files will be saved."
    },
    "recordTrackingForValidationFolderPath": {
        configName: "recordTrackingForValidationFolderPath",
        displayName: "Record Tracking For Validation Folder Path",
        default: "",
        type: "text",
        paid: "true",
        help: "Folder path where record tracking for validation files will be saved."
    },
});
configurationOptionsMap.set("metadata", {
    "numGeneratedSamples": {
        configName: "numGeneratedSamples",
        displayName: "Generated Samples",
        default: 10,
        type: "number",
        min: 0,
        help: "Number of sample records from generated data to take. Shown in HTML report."
    },
    "numRecordsFromDataSource": {
        configName: "numRecordsFromDataSource",
        displayName: "Records From Data Source",
        default: 10000,
        type: "number",
        paid: "true",
        min: 0,
        help: "Number of records read in from the data source that could be used for data profiling."
    },
    "numRecordsForAnalysis": {
        configName: "numRecordsForAnalysis",
        displayName: "Records For Analysis",
        default: 10000,
        type: "number",
        paid: "true",
        min: 0,
        help: "Number of records used for data profiling from the records gathered in <code>Records From Data Source</code>."
    },
    "oneOfDistinctCountVsCountThreshold": {
        configName: "oneOfDistinctCountVsCountThreshold",
        displayName: "One-of Distinct Count vs Count Threshold",
        default: 0.2,
        type: "number",
        paid: "true",
        min: 0.0,
        max: 1.0,
        step: 0.001,
        help: "Threshold ratio to determine if a field is of type oneOf (i.e. a field called status that only contains open or closed. Distinct count = 2, total count = 10, ratio = 2 / 10 = 0.2 therefore marked as oneOf)."
    },
    "oneOfMinCount": {
        configName: "oneOfMinCount",
        displayName: "One-of Min Count",
        default: 1000,
        type: "number",
        paid: "true",
        min: 0,
        help: "Minimum number of records required before considering if a field can be of type oneOf."
    },
});
configurationOptionsMap.set("generation", {
    "numRecordsPerBatch": {
        configName: "numRecordsPerBatch",
        displayName: "Records Per Batch",
        default: 100000,
        type: "number",
        min: 0,
        help: "Number of records across all data sources to generate per batch."
    },
    "numRecordsPerStep": {
        configName: "numRecordsPerStep",
        displayName: "Records Per Step",
        default: -1,
        type: "number",
        help: "Overrides the count defined in each step with this value if defined (i.e. if set to 1000, for each step, 1000 records will be generated)."
    },
});
configurationOptionsMap.set("validation", {
    "numSampleErrorRecords": {
        configName: "numSampleErrorRecords",
        displayName: "Error Samples",
        default: 5,
        type: "number",
        help: "Number of sample error records to show in HTML report. Useful for debugging."
    },
    "enableDeleteRecordTrackingFiles": {
        configName: "enableDeleteRecordTrackingFiles",
        displayName: "Delete Record Tracking Files",
        default: "true",
        type: "text",
        choice: ["true", "false"],
        paid: "true",
        help: "Enable/disable to delete record tracking files at end of execution."
    },
});
configurationOptionsMap.set("alert", {
    "triggerOn": {
        configName: "triggerOn",
        displayName: "Trigger On",
        default: "all",
        type: "text",
        choice: ["all", "failure", "success", "generation_failure", "validation_failure", "generation_success", "validation_success"],
        help: "Condition for triggering alert."
    },
    "slackToken": {
        configName: "slackToken",
        displayName: "Slack Token",
        default: "",
        type: "password",
        help: "Slack token to connect to Slack. Check <a href=\"https://api.slack.com/authentication/token-types\">here</a> for more details."
    },
    "slackChannels": {
        configName: "slackChannels",
        displayName: "Slack Channels",
        default: "",
        type: "text",
        help: "Define one or more Slack channels to send alerts to. Comma separated."
    },
});

export const reportOptionsMap = new Map();
const reportConfigKeys = [["flag", "enableSaveReports"],
    ["flag", "enableValidation"],
    ["flag", "enableSinkMetadata"],
    ["flag", "enableAlerts"],
    ["metadata", "numGeneratedSamples"],
    ["validation", "numSampleErrorRecords"],
    ["alert", "triggerOn"]];
reportConfigKeys.forEach(key => reportOptionsMap.set(key[1], configurationOptionsMap.get(key[0])[key[1]]));
