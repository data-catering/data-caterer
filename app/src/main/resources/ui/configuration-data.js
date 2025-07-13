import {formatDate} from "./shared.js";


const baseDataTypes = ["string", "integer", "long", "short", "decimal", "double", "float", "date", "timestamp", "binary", "array", "struct"];
export const dataTypeOptionsMap = new Map();
const defaultDataTypeOptions = {
    enableEdgeCases: {
        displayName: "Enable Edge Cases",
        default: "false",
        type: "text",
        choice: ["true", "false"],
        help: "Enable generating edge case values for data type."
    },
    edgeCaseProbability: {
        displayName: "Edge Case Probability",
        default: 0.0,
        type: "number",
        min: 0.0,
        max: 1.0,
        step: 0.001,
        help: "Probability of generating edge case values. Range from 0-1.",
        required: ""
    },
    isUnique: {
        displayName: "Unique",
        default: "false",
        type: "text",
        choice: ["true", "false"],
        help: "Generate only unique values."
    },
    seed: {
        displayName: "Seed",
        default: -1,
        type: "number",
        min: -1,
        max: 9223372036854775807,
        help: "Seed for generating consistent random values.",
        required: ""
    },
    sql: {
        displayName: "SQL",
        default: "",
        type: "text",
        help: "<a href='https://spark.apache.org/docs/latest/api/sql/' target='_blank' rel='noopener noreferrer'>Spark SQL</a> expression for generating data.",
        required: ""
    },
    oneOf: {
        displayName: "One Of",
        default: [],
        type: "text",
        help: "Generated values will be one of the defined values. Comma separated.",
        required: ""
    },
    oneOfWeighted: {
        displayName: "One Of Weighted",
        default: [],
        type: "text",
        help: "Generated values will be one of the defined values by weight (i.e. open->0.8, closed->0.1, suspended->0.1). Comma separated.",
        required: ""
    },
    omit: {
        displayName: "Omit",
        default: "false",
        type: "text",
        choice: ["true", "false"],
        help: "Exclude the field from the final output. Can be used for intermediate data generation."
    },
};

function getNumberOptions(min, max) {
    let minMaxOpt = min && max ? {min: min, max: max} : {};
    return {
        min: {
            displayName: "Min",
            default: 0,
            type: "number", ...minMaxOpt,
            help: "Minimum generated value.",
            required: ""
        },
        max: {
            displayName: "Max",
            default: 1000,
            type: "number", ...minMaxOpt,
            help: "Maximum generated value.",
            required: ""
        },
        stddev: {
            displayName: "Standard Deviation",
            default: 1.0,
            type: "number",
            min: 0.0,
            max: 100000000.0,
            help: "Standard deviation of generated values.",
            required: ""
        },
        mean: {
            displayName: "Mean",
            default: 500,
            type: "number", ...minMaxOpt,
            help: "Mean of generated values.",
            required: ""
        },
        round: {
            displayName: "Round",
            default: 2,
            type: "integer",
            help: "Round to number of decimal places.",
            required: ""
        },
        distribution: {
            displayName: "Distribution",
            default: "normal",
            type: "text",
            choice: ["normal", "exponential"],
            help: "Type of distribution values should follow.",
            required: ""
        },
        incremental: {
            displayName: "Incremental",
            default: "",
            type: "text",
            disabled: "",
            help: "Create incremental integer values. Starts from 1 until number of records generated.",
            required: ""
        }
    };
}

dataTypeOptionsMap.set("string", {
    ...defaultDataTypeOptions,
    minLen: {
        displayName: "Min Length",
        default: 1,
        type: "number",
        min: 0,
        max: 1000,
        help: "Minimum length of generated values.",
        required: ""
    },
    maxLen: {
        displayName: "Max Length",
        default: 10,
        type: "number",
        min: 0,
        max: 1000,
        help: "Maximum length of generated values.",
        required: ""
    },
    expression: {
        displayName: "Faker Expression",
        default: "",
        type: "text",
        help: "<a href='https://www.datafaker.net/documentation/providers/' target='_blank' rel='noopener noreferrer'>Faker expression</a> to generate values.",
        required: ""
    },
    enableNull: {
        displayName: "Enable Null",
        default: "false",
        type: "text",
        choice: ["true", "false"],
        help: "Enable generation of null values."
    },
    nullProbability: {
        displayName: "Null Probability",
        default: 0.0,
        type: "number",
        min: 0.0,
        max: 1.0,
        step: 0.001,
        help: "Probability of generating null values. Range from 0-1.",
        required: ""
    },
    regex: {displayName: "Regex", default: "", type: "text", help: "Regex for generating values.", required: ""},
    uuid: {
        displayName: "UUID",
        default: "",
        type: "text",
        help: "Generate UUID values. Optionally, define a field name to generate consistent UUID values from",
        required: ""
    }
});
dataTypeOptionsMap.set("integer", {...defaultDataTypeOptions, ...getNumberOptions(-2147483648, 2147483647)});
dataTypeOptionsMap.set("long", {...defaultDataTypeOptions, ...getNumberOptions(-9223372036854775808, 9223372036854775807)});
dataTypeOptionsMap.set("short", {...defaultDataTypeOptions, ...getNumberOptions(-32768, 32767)});
dataTypeOptionsMap.set("decimal", {
    ...defaultDataTypeOptions,
    ...getNumberOptions(),
    numericPrecision: {
        displayName: "Precision",
        default: 10,
        type: "number",
        min: 0,
        max: 2147483647,
        help: "Precision for generated decimal values."
    },
    numericScale: {
        displayName: "Scale",
        default: 0,
        type: "number",
        min: 0,
        max: 2147483647,
        help: "Scale for generated decimal values."
    }
});
dataTypeOptionsMap.set("double", {...defaultDataTypeOptions, ...getNumberOptions()});
dataTypeOptionsMap.set("float", {...defaultDataTypeOptions, ...getNumberOptions()});
dataTypeOptionsMap.set("date", {
    ...defaultDataTypeOptions,
    min: {
        displayName: "Min",
        default: formatDate(true),
        type: "date",
        min: "0001-01-01",
        max: "9999-12-31",
        help: "Minimum date of generated values. Expected format 'yyyy-MM-dd'.",
        required: ""
    },
    max: {
        displayName: "Max",
        default: formatDate(false),
        type: "date",
        min: "0001-01-01",
        max: "9999-12-31",
        help: "Maximum date of generated values. Expected format 'yyyy-MM-dd'.",
        required: ""
    }
});
dataTypeOptionsMap.set("timestamp", {
    ...defaultDataTypeOptions,
    min: {
        displayName: "Min",
        default: formatDate(true, true),
        type: "datetime-local",
        min: "0001-01-01 00:00:00",
        max: "9999-12-31 23:59:59",
        help: "Minimum timestamp of generated values. Expected format 'yyyy-MM-dd HH:mm:ss'.",
        required: ""
    },
    max: {
        displayName: "Max",
        default: formatDate(false, true),
        type: "datetime-local",
        min: "0001-01-01 00:00:00",
        max: "9999-12-31 23:59:59",
        help: "Maximum timestamp of generated values. Expected format 'yyyy-MM-dd HH:mm:ss'.",
        required: ""
    }
});
dataTypeOptionsMap.set("binary", {
    ...defaultDataTypeOptions,
    minLen: {
        displayName: "Min Length",
        default: 1,
        type: "number",
        min: 0,
        max: 2147483647,
        help: "Minimum length of generated values.",
        required: ""
    },
    maxLen: {
        displayName: "Max Length",
        default: 20,
        type: "number",
        min: 0,
        max: 2147483647,
        help: "Maximum length of generated values.",
        required: ""
    },
});
dataTypeOptionsMap.set("array", {
    ...defaultDataTypeOptions,
    arrayMinLen: {
        displayName: "Min Length",
        default: 0,
        type: "number",
        min: 0,
        max: 2147483647,
        help: "Minimum generated array length.",
        required: ""
    },
    arrayMaxLen: {
        displayName: "Max Length",
        default: 5,
        type: "number",
        min: 0,
        max: 2147483647,
        help: "Maximum generated array length.",
        required: ""
    },
    arrayType: {
        displayName: "Type",
        default: "string",
        type: "text",
        choice: baseDataTypes,
        help: "Data type of array values.",
        required: ""
    }
});
dataTypeOptionsMap.set("struct", {...defaultDataTypeOptions, addBlock: {type: "field"}});

export const validationTypeDisplayNameMap = new Map();
validationTypeDisplayNameMap.set("field", "Field");
validationTypeDisplayNameMap.set("groupBy", "Group By/Aggregate");
validationTypeDisplayNameMap.set("upstream", "Upstream");
validationTypeDisplayNameMap.set("fieldNames", "Field Names");
export const validationTypeOptionsMap = new Map();
const defaultValidationOptions = {
    description: {
        displayName: "Description",
        default: "",
        type: "text",
        help: "Description of validation. Used in report."
    },
    errorThreshold: {
        displayName: "Error Threshold",
        default: 0.0,
        type: "number",
        min: 0.0,
        help: "Number or percentage (0.0 to 1.0) of errors before marking validation as failed."
    },
}
validationTypeOptionsMap.set("field", {
    ...defaultValidationOptions,
    defaultChildField: {displayName: "Field", default: "", type: "text", required: "", help: "Field to validate."},
    equal: {
        displayName: "Equal",
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Not"},
        help: "Equal to value. Select 'Not' for not equals."
    },
    null: {
        displayName: "Null",
        default: "",
        type: "text",
        disabled: "",
        group: {type: "checkbox", innerText: "Not"},
        help: "Values are null."
    },
    contains: {
        displayName: "Contains",
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Not"},
        help: "Contains value. Select 'Not' for not contains.",
        required: ""
    },
    unique: {displayName: "Unique", default: "", type: "text", disabled: "", help: "Values are unique."},
    lessThan: {
        displayName: "Less Than",
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Equal"},
        help: "Less than value. Select 'Equal' for less than or equal to.",
        required: ""
    },
    greaterThan: {
        displayName: "Greater Than",
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Equal"},
        help: "Greater than value. Select 'Equal' for greater than or equal to.",
        required: ""
    },
    between: {
        displayName: "Between",
        default: "",
        type: "min-max",
        group: {type: "checkbox", innerText: "Not"},
        help: "Between values. Select 'Not' for not between."
    },
    in: {
        displayName: "In",
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Not"},
        help: "In set of values. Select 'Not' for not in set."
    },
    matches: {
        displayName: "Matches",
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Not"},
        help: "Matches regex. Select 'Not' for not matches regex.",
        required: ""
    },
    startsWith: {
        displayName: "Starts With",
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Not"},
        help: "Starts with value. Select 'Not' for not starts with.",
        required: ""
    },
    endsWith: {
        displayName: "Ends With",
        default: "",
        type: "text",
        group: {type: "checkbox", innerText: "Not"},
        help: "Ends with value. Select 'Not' for not ends with.",
        required: ""
    },
    size: {
        displayName: "Size",
        default: 0,
        type: "number",
        group: {type: "checkbox", innerText: "Not"},
        help: "Equal to size. Select 'Not' for not equal to size.",
        required: ""
    },
    lessThanSize: {
        displayName: "Less Than Size",
        default: 0,
        type: "number",
        group: {type: "checkbox", innerText: "Equal"},
        help: "Less than size. Select 'Equal' for less than or equal to size.",
        required: ""
    },
    greaterThanSize: {
        displayName: "Greater Than Size",
        default: 0,
        type: "number",
        group: {type: "checkbox", innerText: "Equal"},
        help: "Greater than size. Select 'Equal' for greater than or equal to size.",
        required: ""
    },
    luhnCheck: {
        displayName: "Luhn Check",
        default: "",
        type: "text",
        disabled: "",
        group: {type: "checkbox", innerText: "Not"},
        help: "Values are valid credit card or identification numbers according to Luhn Algorithm.",
        required: ""
    },
    hasType: {
        displayName: "Type",
        default: "string",
        type: "text",
        choice: baseDataTypes,
        group: {type: "checkbox", innerText: "Not"},
        help: "Values are of data type."
    },
    sql: {
        displayName: "SQL",
        default: "",
        type: "text",
        help: "<a href='https://spark.apache.org/docs/latest/api/sql/' target='_blank' rel='noopener noreferrer'>Spark SQL</a> statement, returning boolean, for custom validation.",
        required: ""
    },
});
validationTypeOptionsMap.set("groupBy", {
    ...defaultValidationOptions,
    defaultChildGroupByFields: {
        displayName: "Group By Field(s)",
        default: "",
        type: "text",
        required: "",
        help: "Field name(s) to group by. Comma separated."
    },
    count: {
        displayName: "Count",
        default: "",
        type: "text",
        help: "Field name to count number of groups after group by.",
        addBlock: {type: "field"}
    },
    sum: {
        displayName: "Sum",
        default: "",
        type: "text",
        help: "Field name of values to sum after group by.",
        addBlock: {type: "field"},
        required: ""
    },
    min: {
        displayName: "Min",
        default: "",
        type: "text",
        help: "Field name to find minimum value after group by.",
        addBlock: {type: "field"},
        required: ""
    },
    max: {
        displayName: "Max",
        default: "",
        type: "text",
        help: "Field name to find maximum value after group by.",
        addBlock: {type: "field"},
        required: ""
    },
    average: {
        displayName: "Average",
        default: "",
        type: "text",
        help: "Field name to find average value after group by.",
        addBlock: {type: "field"},
        required: ""
    },
    standardDeviation: {
        displayName: "Standard Deviation",
        default: "",
        type: "text",
        help: "Field name to find standard deviation value after group by.",
        addBlock: {type: "field"},
        required: ""
    },
});
validationTypeOptionsMap.set("upstream", {
    ...defaultValidationOptions,
    defaultChildUpstreamTaskName: {
        displayName: "Upstream Task Name",
        default: "",
        type: "text",
        required: "",
        selector: ".task-name-field",
        help: "Name of upstream data generation task."
    },
    addBlock: {type: "validation"},
    joinFields: {
        displayName: "Join Field(s)",
        default: "",
        type: "text",
        help: "Field name(s) to join by.",
        required: ""
    },
    joinType: {
        displayName: "Join Type",
        default: "outer",
        type: "text",
        choice: ["inner", "outer", "left_outer", "right_outer", "left_semi", "anti", "cross"],
        help: "Type of join."
    },
    joinExpr: {
        displayName: "Join Expression",
        default: "",
        type: "text",
        help: "Custom join SQL expression.",
        required: ""
    }
});
validationTypeOptionsMap.set("fieldNames", {
    ...defaultValidationOptions,
    countEqual: {
        displayName: "Count Equal",
        default: 0,
        type: "number",
        help: "Number of fields has to equal value.",
        required: ""
    },
    countBetween: {
        displayName: "Count Between",
        default: 0,
        type: "min-max",
        help: "Number of field has to be between min and max value (inclusive).",
        required: ""
    },
    matchOrder: {
        displayName: "Match Order",
        default: "",
        type: "text",
        help: "All field names match particular ordering and is complete. Comma separated.",
        required: ""
    },
    matchSet: {
        displayName: "Match Set",
        default: "",
        type: "text",
        help: "Field names contains set of expected names. Order is not checked. Comma separated.",
        required: ""
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
    "enableReferenceMode": {
        configName: "enableReferenceMode",
        displayName: "Reference Mode",
        default: "false",
        type: "text",
        choice: ["true", "false"],
        help: "Enable/disable reference mode. When enabled, reads existing data from the data source instead of generating new data. Useful for using existing datasets as reference data in foreign key relationships. Cannot be used together with data generation."
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
        help: "Enable/disable generating unique values for fields marked as unique. Can be disabled to improve performance but not guarantee uniqueness."
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
        help: "Run validations as described in plan. Results can be viewed from logs or from HTML report."
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
        default: "true",
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
    "enableFastGeneration": {
        configName: "enableFastGeneration",
        displayName: "Fast Generation",
        default: "false",
        type: "text",
        choice: ["true", "false"],
        help: "Enable/disable fast generation mode. When enabled, disables certain features to improve performance."
    },
});
configurationOptionsMap.set("generation", {
    "numRecordsPerBatch": {
        configName: "numRecordsPerBatch",
        displayName: "Records Per Batch",
        default: 100000,
        type: "number",
        min: 0,
        help: "Number of records across all data sources to generate per batch.",
        required: ""
    },
    "numRecordsPerStep": {
        configName: "numRecordsPerStep",
        displayName: "Records Per Step",
        default: -1,
        type: "number",
        help: "Overrides the count defined in each step with this value if defined (i.e. if set to 1000, for each step, 1000 records will be generated).",
        required: ""
    },
});
configurationOptionsMap.set("validation", {
    "numSampleErrorRecords": {
        configName: "numSampleErrorRecords",
        displayName: "Error Samples",
        default: 5,
        type: "number",
        help: "Number of sample error records to show in HTML report. Useful for debugging.",
        required: ""
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
configurationOptionsMap.set("metadata", {
    "numGeneratedSamples": {
        configName: "numGeneratedSamples",
        displayName: "Generated Samples",
        default: 10,
        type: "number",
        min: 0,
        help: "Number of sample records from generated data to take. Shown in HTML report.",
        required: ""
    },
    "numRecordsFromDataSource": {
        configName: "numRecordsFromDataSource",
        displayName: "Records From Data Source",
        default: 10000,
        type: "number",
        paid: "true",
        min: 0,
        help: "Number of records read in from the data source that could be used for data profiling.",
        required: ""
    },
    "numRecordsForAnalysis": {
        configName: "numRecordsForAnalysis",
        displayName: "Records For Analysis",
        default: 10000,
        type: "number",
        paid: "true",
        min: 0,
        help: "Number of records used for data profiling from the records gathered in <code>Records From Data Source</code>.",
        required: ""
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
        help: "Threshold ratio to determine if a field is of type oneOf (i.e. a field called status that only contains open or closed. Distinct count = 2, total count = 10, ratio = 2 / 10 = 0.2 therefore marked as oneOf).",
        required: ""
    },
    "oneOfMinCount": {
        configName: "oneOfMinCount",
        displayName: "One-of Min Count",
        default: 1000,
        type: "number",
        paid: "true",
        min: 0,
        help: "Minimum number of records required before considering if a field can be of type oneOf.",
        required: ""
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
        help: "Slack token to connect to Slack. Check <a href='https://api.slack.com/authentication/token-types' target='_blank' rel='noopener noreferrer'>here</a> for more details."
    },
    "slackChannels": {
        configName: "slackChannels",
        displayName: "Slack Channels",
        default: "",
        type: "text",
        help: "Define one or more Slack channels to send alerts to. Comma separated."
    },
});
configurationOptionsMap.set("folder", {
    "generatedReportsFolderPath": {
        configName: "generatedReportsFolderPath",
        displayName: "Generated Reports Folder Path",
        default: "/opt/app/report",
        type: "text",
        help: "Folder path where generated HTML reports will be saved.",
        required: ""
    },
    "validationFolderPath": {
        configName: "validationFolderPath",
        displayName: "Validation Folder Path",
        default: "/opt/app/validation",
        type: "text",
        help: "If using YAML validation file(s), folder path that contains all validation files (can have nested directories).",
        required: ""
    },
    "planFilePath": {
        configName: "planFilePath",
        displayName: "Plan File Path",
        default: "/opt/app/plan/customer-create-plan.yaml",
        type: "text",
        help: "If using YAML plan file, path to use when generating and/or validating data.",
        required: ""
    },
    "taskFolderPath": {
        configName: "taskFolderPath",
        displayName: "Task Folder Path",
        default: "/opt/app/task",
        type: "text",
        help: "If using YAML task file(s), folder path that contains all the task files (can have nested directories).",
        required: ""
    },
    "generatedPlanAndTaskFolderPath": {
        configName: "generatedPlanAndTaskFolderPath",
        displayName: "Generated Plan And Tasks Folder Path",
        default: "/tmp",
        type: "text",
        paid: "true",
        help: "Folder path where generated plan and task files will be saved.",
        required: ""
    },
    "recordTrackingFolderPath": {
        configName: "recordTrackingFolderPath",
        displayName: "Record Tracking Folder Path",
        default: "/opt/app/record-tracking",
        type: "text",
        paid: "true",
        help: "Folder path where record tracking files will be saved.",
        required: ""
    },
    "recordTrackingForValidationFolderPath": {
        configName: "recordTrackingForValidationFolderPath",
        displayName: "Record Tracking For Validation Folder Path",
        default: "/opt/app/record-tracking-validation",
        type: "text",
        paid: "true",
        help: "Folder path where record tracking for validation files will be saved.",
        required: ""
    },
});

export const reportOptionsMap = new Map();
export const reportConfigKeys = [["flag", "enableSaveReports"],
    ["flag", "enableValidation"],
    ["flag", "enableSinkMetadata"],
    ["flag", "enableAlerts"],
    ["alert", "triggerOn"],
    ["metadata", "numGeneratedSamples"],
    ["validation", "numSampleErrorRecords"]];
reportConfigKeys.forEach(key => reportOptionsMap.set(key[1], configurationOptionsMap.get(key[0])[key[1]]));


export const dataSourcePropertiesMap = new Map();
// Data Source
dataSourcePropertiesMap.set("bigquery", {
    optGroupLabel: "Data Source",
    Name: "BigQuery",
    properties: {
        temporaryGcsBucket: {
            displayName: "Temporary GCS Bucket",
            default: "",
            type: "text",
            help: "Temporary Google Cloud Storage bucket to store data."
        },
        table: {
            displayName: "Table",
            default: "",
            type: "text",
            help: "Table to generate/validate data to/from. Format: [project name].[dataset name].[table name].",
            override: "true",
        }
    }
})
dataSourcePropertiesMap.set("cassandra", {
    optGroupLabel: "Data Source",
    Name: "Cassandra",
    properties: {
        url: {
            displayName: "URL",
            default: "localhost:9042",
            type: "text",
            help: "Hostname and port to connect to Cassandra.",
            required: ""
        },
        user: {
            displayName: "Username",
            default: "cassandra",
            type: "text",
            help: "Username to connect to Cassandra.",
            required: ""
        },
        password: {
            displayName: "Password",
            default: "cassandra",
            type: "password",
            help: "Password to connect to Cassandra.",
            required: ""
        },
        keyspace: {
            displayName: "Keyspace",
            default: "",
            type: "text",
            help: "Keyspace to generate/validate data to/from.",
            override: "true",
        },
        table: {
            displayName: "Table",
            default: "",
            type: "text",
            help: "Table to generate/validate data to/from.",
            override: "true",
        }
    }
});
dataSourcePropertiesMap.set("csv", {
    optGroupLabel: "Data Source",
    Name: "CSV",
    properties: {
        path: {
            displayName: "Path",
            default: "/tmp/generated-data/csv",
            type: "text",
            help: "File pathway to save CSV.",
            required: ""
        },
        partitions: {
            displayName: "Num Partitions",
            default: "1",
            type: "number",
            help: "Number of file partitions.",
            override: "true"
        },
        partitionBy: {
            displayName: "Partition By",
            default: "",
            type: "text",
            help: "Field name(s) to partition by (comma separated).",
            override: "true"
        }
    }
});
dataSourcePropertiesMap.set("delta", {
    optGroupLabel: "Data Source",
    Name: "Delta Lake",
    properties: {
        path: {
            displayName: "Path",
            default: "/tmp/generated-data/delta",
            type: "text",
            help: "File pathway to save Delta Lake.",
            required: ""
        },
        partitions: {
            displayName: "Num Partitions",
            default: "1",
            type: "number",
            help: "Number of file partitions.",
            override: "true"
        },
        partitionBy: {
            displayName: "Partition By",
            default: "",
            type: "text",
            help: "Field name(s) to partition by (comma separated).",
            override: "true"
        }
    }
});
dataSourcePropertiesMap.set("http", {
    optGroupLabel: "Data Source",
    Name: "HTTP",
    properties: {
        user: {
            displayName: "Username",
            default: "",
            type: "text",
            help: "Username to connect to HTTP API."
        },
        password: {
            displayName: "Password",
            default: "",
            type: "password",
            help: "Password to connect to HTTP API."
        },
    }
});
dataSourcePropertiesMap.set("iceberg", {
    optGroupLabel: "Data Source",
    Name: "Iceberg",
    properties: {
        catalogType: {
            displayName: "Catalog Type",
            default: "hadoop",
            type: "text",
            choice: ["hadoop", "glue", "hive", "jdbc", "nessie", "rest"],
            help: "Catalog Type for Iceberg metadata.",
            required: ""
        },
        path: {
            displayName: "Warehouse Path",
            default: "/tmp/generated-data/iceberg",
            type: "text",
            help: "File pathway to Iceberg warehouse.",
        },
        catalogUri: {
            displayName: "Catalog URI",
            default: "",
            type: "text",
            help: "Catalog URI for Iceberg metadata."
        },
        table: {
            displayName: "Table Name",
            default: "",
            type: "text",
            help: "Table name for Iceberg dataset (format: [database name].[table name]).",
            override: "true"
        },
        partitions: {
            displayName: "Num Partitions",
            default: "1",
            type: "number",
            help: "Number of file partitions.",
            override: "true"
        },
        partitionBy: {
            displayName: "Partition By",
            default: "",
            type: "text",
            help: "Field name(s) to partition by (comma separated).",
            override: "true"
        }
    }
});
dataSourcePropertiesMap.set("json", {
    optGroupLabel: "Data Source",
    Name: "JSON",
    properties: {
        path: {
            displayName: "Path",
            default: "/tmp/generated-data/json",
            type: "text",
            help: "File pathway to save JSON.",
            required: ""
        },
        partitions: {
            displayName: "Num Partitions",
            default: "1",
            type: "number",
            help: "Number of file partitions.",
            override: "true"
        },
        partitionBy: {
            displayName: "Partition By",
            default: "",
            type: "text",
            help: "Field name(s) to partition by (comma separated).",
            override: "true"
        }
    }
});
dataSourcePropertiesMap.set("kafka", {
    optGroupLabel: "Data Source",
    Name: "Kafka",
    properties: {
        url: {
            displayName: "URL",
            default: "localhost:9092",
            type: "text",
            help: "URL to connect to Kafka.",
            required: ""
        },
        topic: {
            displayName: "Topic",
            default: "",
            type: "text",
            help: "Topic to generate/validate data to/from.",
            required: "",
            override: "true",
        },
    }
});
dataSourcePropertiesMap.set("mysql", {
    optGroupLabel: "Data Source",
    Name: "MySQL",
    properties: {
        url: {
            displayName: "URL",
            default: "jdbc:mysql://localhost:3306/customer",
            type: "text",
            help: "URL to connect to MySQL.",
            required: ""
        },
        user: {
            displayName: "Username",
            default: "root",
            type: "text",
            help: "Username to connect to MySQL.",
            required: ""
        },
        password: {
            displayName: "Password",
            default: "root",
            type: "password",
            help: "Password to connect to MySQL.",
            required: ""
        },
        schema: {
            displayName: "Schema",
            default: "",
            type: "text",
            help: "Schema to generate/validate data to/from.",
            override: "true",
        },
        table: {
            displayName: "Table",
            default: "",
            type: "text",
            help: "Table to generate/validate data to/from.",
            override: "true",
        }
    }
});
dataSourcePropertiesMap.set("orc", {
    optGroupLabel: "Data Source",
    Name: "ORC",
    properties: {
        path: {
            displayName: "Path",
            default: "/tmp/generated-data/orc",
            type: "text",
            help: "File pathway to save ORC.",
            required: ""
        },
        partitions: {
            displayName: "Num Partitions",
            default: "1",
            type: "number",
            help: "Number of file partitions.",
            override: "true"
        },
        partitionBy: {
            displayName: "Partition By",
            default: "",
            type: "text",
            help: "Field name(s) to partition by (comma separated).",
            override: "true"
        }
    }
});
dataSourcePropertiesMap.set("parquet", {
    optGroupLabel: "Data Source",
    Name: "Parquet",
    properties: {
        path: {
            displayName: "Path",
            default: "/tmp/generated-data/parquet",
            type: "text",
            help: "File pathway to save Parquet.",
            required: ""
        },
        partitions: {
            displayName: "Num Partitions",
            default: "1",
            type: "number",
            help: "Number of file partitions.",
            override: "true"
        },
        partitionBy: {
            displayName: "Partition By",
            default: "",
            type: "text",
            help: "Field name(s) to partition by (comma separated).",
            override: "true"
        }
    }
});
dataSourcePropertiesMap.set("postgres", {
    optGroupLabel: "Data Source",
    Name: "Postgres",
    properties: {
        url: {
            displayName: "URL",
            default: "jdbc:postgresql://localhost:5432/customer",
            type: "text",
            help: "URL to connect to Postgres.",
            required: ""
        },
        user: {
            displayName: "Username",
            default: "postgres",
            type: "text",
            help: "Username to connect to Postgres.",
            required: ""
        },
        password: {
            displayName: "Password",
            default: "postgres",
            type: "password",
            help: "Password to connect to Postgres.",
            required: ""
        },
        schema: {
            displayName: "Schema",
            default: "",
            type: "text",
            help: "Schema to generate/validate data to/from.",
            override: "true",
        },
        table: {
            displayName: "Table",
            default: "",
            type: "text",
            help: "Table to generate/validate data to/from.",
            override: "true",
        }
    }
});
dataSourcePropertiesMap.set("rabbitmq", {
    optGroupLabel: "Data Source",
    Name: "RabbitMQ",
    properties: {
        url: {
            displayName: "URL",
            default: "ampq://host.docker.internal:5672",
            type: "text",
            help: "URL to connect to RabbitMQ.",
            required: ""
        },
        user: {
            displayName: "Username",
            default: "guest",
            type: "text",
            help: "Username to connect to RabbitMQ.",
            required: ""
        },
        password: {
            displayName: "Password",
            default: "guest",
            type: "password",
            help: "Password to connect to RabbitMQ.",
            required: ""
        },
        destination: {
            displayName: "Destination",
            default: "accounts",
            type: "text",
            help: "JMS destination (queue or topic) to generate data to.",
            required: "",
            override: "true",
        }
    }
});
dataSourcePropertiesMap.set("solace", {
    optGroupLabel: "Data Source",
    Name: "Solace",
    properties: {
        url: {
            displayName: "URL",
            default: "smf://host.docker.internal:55554",
            type: "text",
            help: "URL to connect to Solace.",
            required: ""
        },
        destination: {
            displayName: "Destination",
            default: "/JNDI/Q/test_queue",
            type: "text",
            help: "JNDI JMS destination to generate data to.",
            required: "",
            override: "true",
        }
    }
});

// Metadata Source
dataSourcePropertiesMap.set("amundsen", {
    optGroupLabel: "Metadata Source",
    Name: "Amundsen",
    properties: {
        url: {
            displayName: "URL",
            default: "",
            type: "text",
            help: "URL to connect to Amundsen API.",
            required: ""
        }
    }
});
dataSourcePropertiesMap.set("datahub", {
    optGroupLabel: "Metadata Source",
    Name: "Datahub",
    properties: {
        url: {
            displayName: "URL",
            default: "",
            type: "text",
            help: "URL to connect to Datahub API.",
            required: ""
        }
    }
});
dataSourcePropertiesMap.set("dataContractCli", {
    optGroupLabel: "Metadata Source",
    Name: "Data Contract CLI",
    properties: {
        path: {
            displayName: "Contract File",
            default: "",
            type: "text",
            help: "File path to data contract file.",
            required: ""
        }
    }
});
dataSourcePropertiesMap.set("greatExpectations", {
    optGroupLabel: "Metadata Source",
    Name: "Great Expectations",
    properties: {
        path: {
            displayName: "Expectations File",
            default: "",
            type: "text",
            help: "File path to expectations file.",
            required: ""
        }
    }
});
dataSourcePropertiesMap.set("marquez", {
    optGroupLabel: "Metadata Source",
    Name: "Marquez",
    properties: {
        url: {
            displayName: "URL",
            default: "http://localhost:5001",
            type: "text",
            help: "API URL to connect to Marquez.",
            required: ""
        },
        namespace: {
            displayName: "Namespace",
            default: "",
            type: "text",
            help: "Namespace to gather metadata from.",
            override: "true"
        },
        dataset: {
            displayName: "Dataset",
            default: "",
            type: "text",
            help: "Dataset to gather metadata from.",
            override: "true"
        }
    }
});
dataSourcePropertiesMap.set("openApi", {
    optGroupLabel: "Metadata Source",
    Name: "OpenAPI/Swagger",
    properties: {
        schemaLocation: {
            displayName: "Schema Location",
            default: "",
            type: "text",
            help: "Path/URL to gather metadata from.",
            required: ""
        }
    }
});
dataSourcePropertiesMap.set("openDataContractStandard", {
    optGroupLabel: "Metadata Source",
    Name: "Open Data Contract Standard (ODCS)",
    properties: {
        path: {
            displayName: "Contract File",
            default: "",
            type: "text",
            help: "File path to ODCS file.",
            required: ""
        }
    }
});
dataSourcePropertiesMap.set("openMetadata", {
    optGroupLabel: "Metadata Source",
    Name: "Open Metadata",
    properties: {
        url: {
            displayName: "URL",
            default: "http://localhost:8585/api",
            type: "text",
            help: "API URL to connect to OpenMetadata.",
            required: ""
        },
        authType: {
            displayName: "Auth Type",
            default: "openmetadata",
            type: "text",
            help: "Authentication mechanism used to connect to OpenMetadata.",
            required: ""
        },
        jwt: {
            displayName: "JWT",
            default: "",
            type: "password",
            help: "JWT token."
        },
        tableFQN: {
            displayName: "Table FQN",
            default: "",
            type: "text",
            help: "Table FQN to gather metadata from.",
            override: "true"
        }
    }
});

// Alert
dataSourcePropertiesMap.set("slack", {
    optGroupLabel: "Alert",
    Name: "Slack",
    properties: {
        token: {
            displayName: "Token",
            default: "",
            type: "password",
            help: "Token to authenticate with Slack.",
            required: ""
        },
        channels: {
            displayName: "Channels",
            default: "",
            type: "text",
            help: "Channel(s) to send alerts to (comma separated).",
            required: ""
        }
    }
});

export let subDataSourceConfigMap = new Map();
subDataSourceConfigMap.set("http", {
    method: {
        displayName: "Method",
        default: "",
        type: "text",
        help: "HTTP method.",
        choice: ["", "GET", "POST", "PUT", "DELETE", "PATCH", "CONNECT", "OPTIONS", "TRACE", "HEAD"],
        override: "true"
    },
    endpoint: {
        displayName: "Endpoint",
        default: "",
        type: "text",
        help: "Endpoint pathway (i.e. '/my-path/data').",
        override: "true"
    }
});
