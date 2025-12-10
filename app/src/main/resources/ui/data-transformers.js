/**
 * Pure data transformation functions for save/load operations.
 * These functions contain no DOM manipulation and can be easily unit tested.
 * 
 * The UI helper files (helper-generation.js, helper-validation.js, helper-record-count.js)
 * should use these functions for the actual data transformation logic.
 */

/**
 * Converts a Map to a plain object for JSON serialization.
 * @param {Map} map - The map to convert
 * @returns {Object} Plain object representation
 */
export function mapToObject(map) {
    if (!(map instanceof Map)) return map;
    const obj = {};
    for (const [key, value] of map) {
        obj[key] = value;
    }
    return obj;
}

/**
 * Converts camelCase to display name (e.g., "fieldName" -> "Field Name")
 * @param {string} str - The camelCase string
 * @returns {string} Display name
 */
export function camelToDisplayName(str) {
    return str.replace(/([A-Z])/g, ' $1').replace(/^./, s => s.toUpperCase()).trim();
}

/**
 * Converts a string to camelCase.
 * @param {string} str - The string to convert
 * @returns {string} camelCase string
 */
export function camelize(str) {
    return str.replace(/(?:^\w|[A-Z]|\b\w|\s+)/g, function (match, index) {
        if (+match === 0) return "";
        return index === 0 ? match.toLowerCase() : match.toUpperCase();
    });
}

// ============================================================================
// Record Count Transformations
// ============================================================================

/**
 * Builds a count object in backend-compatible format from UI state.
 * 
 * Backend format:
 * {
 *   records: number (optional),
 *   options: { min: number, max: number } (for random between),
 *   perField: {
 *     fieldNames: string[],
 *     count: number (optional),
 *     options: { min: number, max: number, distribution: string, rateParam: number }
 *   }
 * }
 * 
 * @param {Object} uiState - The UI state object
 * @param {boolean} uiState.isRecordsBetween - Whether "Generated records between" is selected
 * @param {number} [uiState.records] - Simple record count (when isRecordsBetween is false)
 * @param {number} [uiState.recordsMin] - Min records (when isRecordsBetween is true)
 * @param {number} [uiState.recordsMax] - Max records (when isRecordsBetween is true)
 * @param {string[]} [uiState.perFieldNames] - Field names for per-field count
 * @param {boolean} [uiState.isPerFieldBetween] - Whether per-field uses min/max
 * @param {number} [uiState.perFieldCount] - Simple per-field count
 * @param {number} [uiState.perFieldMin] - Min per-field count
 * @param {number} [uiState.perFieldMax] - Max per-field count
 * @param {string} [uiState.distribution] - Distribution type (uniform, exponential, normal)
 * @param {number} [uiState.rateParam] - Rate parameter for exponential distribution
 * @returns {Object} Backend-compatible count object
 */
export function buildCountFromUIState(uiState) {
    const result = {};
    
    // Handle base record count
    if (uiState.isRecordsBetween) {
        result.options = {
            min: uiState.recordsMin,
            max: uiState.recordsMax
        };
    } else if (uiState.records !== undefined && uiState.records !== null) {
        result.records = uiState.records;
    }
    
    // Handle per-field count
    if (uiState.perFieldNames && uiState.perFieldNames.length > 0) {
        const perField = {
            fieldNames: uiState.perFieldNames
        };
        const perFieldOptions = {};
        
        if (uiState.isPerFieldBetween) {
            perFieldOptions.min = uiState.perFieldMin;
            perFieldOptions.max = uiState.perFieldMax;
        } else if (uiState.perFieldCount !== undefined && uiState.perFieldCount !== null) {
            perField.count = uiState.perFieldCount;
        }
        
        if (uiState.distribution && uiState.distribution !== 'uniform') {
            perFieldOptions.distribution = uiState.distribution;
        }
        
        if (uiState.distribution === 'exponential' && uiState.rateParam !== undefined) {
            perFieldOptions.rateParam = uiState.rateParam;
        }
        
        if (Object.keys(perFieldOptions).length > 0) {
            perField.options = perFieldOptions;
        }
        
        result.perField = perField;
    }
    
    return result;
}

/**
 * Extracts UI state from a backend count object.
 * This is the inverse of buildCountFromUIState.
 * 
 * @param {Object} count - Backend count object
 * @returns {Object} UI state object
 */
export function extractUIStateFromCount(count) {
    if (!count) return {};
    
    const result = {};
    
    // Handle base record count
    if (count.options && count.options.min !== undefined && count.options.max !== undefined) {
        result.isRecordsBetween = true;
        result.recordsMin = count.options.min;
        result.recordsMax = count.options.max;
    } else if (count.records !== undefined) {
        result.isRecordsBetween = false;
        result.records = count.records;
    }
    
    // Handle per-field count
    if (count.perField && count.perField.fieldNames && count.perField.fieldNames.length > 0) {
        result.perFieldNames = count.perField.fieldNames;
        
        if (count.perField.options && count.perField.options.min !== undefined && count.perField.options.max !== undefined) {
            result.isPerFieldBetween = true;
            result.perFieldMin = count.perField.options.min;
            result.perFieldMax = count.perField.options.max;
        } else if (count.perField.count !== undefined) {
            result.isPerFieldBetween = false;
            result.perFieldCount = count.perField.count;
        }
        
        if (count.perField.options && count.perField.options.distribution) {
            result.distribution = count.perField.options.distribution;
        }
        
        if (count.perField.options && count.perField.options.rateParam !== undefined) {
            result.rateParam = count.perField.options.rateParam;
        }
    }
    
    return result;
}

/**
 * Estimates the total record count from a count configuration.
 * 
 * @param {Object} count - Backend count object
 * @returns {number} Estimated record count
 */
export function estimateRecordCountFromConfig(count) {
    if (!count) return 1000; // default
    
    let baseCount;
    if (count.options && count.options.min !== undefined && count.options.max !== undefined) {
        baseCount = (count.options.min + count.options.max) / 2;
    } else {
        baseCount = count.records || 1000;
    }
    
    let perFieldMultiplier = 1;
    if (count.perField && count.perField.fieldNames && count.perField.fieldNames.length > 0) {
        if (count.perField.options && count.perField.options.min !== undefined && count.perField.options.max !== undefined) {
            perFieldMultiplier = (count.perField.options.min + count.perField.options.max) / 2;
        } else if (count.perField.count !== undefined) {
            perFieldMultiplier = count.perField.count;
        }
    }
    
    return baseCount * perFieldMultiplier;
}

// ============================================================================
// Generation Configuration Transformations
// ============================================================================

/**
 * Determines which generation checkboxes should be checked based on task data.
 * 
 * @param {Object} taskData - The task data from backend
 * @param {Object} [taskData.options] - Task options
 * @param {Array} [taskData.fields] - Manual field definitions
 * @returns {Object} Checkbox states
 */
export function getGenerationCheckboxStates(taskData) {
    const result = {
        auto: false,
        autoFromMetadata: false,
        manual: false
    };
    
    if (taskData.options && taskData.options.enableDataGeneration === 'true') {
        result.auto = true;
    }
    
    if (taskData.options && taskData.options.metadataSourceName) {
        result.autoFromMetadata = true;
    }
    
    if (taskData.fields && taskData.fields.length > 0) {
        result.manual = true;
    }
    
    return result;
}

/**
 * Builds task options from generation checkbox states.
 * 
 * @param {Object} checkboxStates - The checkbox states
 * @param {boolean} checkboxStates.auto - Auto generation enabled
 * @param {boolean} checkboxStates.autoFromMetadata - Auto from metadata enabled
 * @param {string} [checkboxStates.metadataSourceName] - Metadata source name
 * @param {Object} [checkboxStates.metadataOptions] - Additional metadata options
 * @returns {Object} Task options
 */
export function buildGenerationOptions(checkboxStates) {
    const options = {};
    
    if (checkboxStates.auto) {
        options.enableDataGeneration = 'true';
    }
    
    if (checkboxStates.autoFromMetadata && checkboxStates.metadataSourceName) {
        options.metadataSourceName = checkboxStates.metadataSourceName;
        if (checkboxStates.metadataOptions) {
            Object.assign(options, checkboxStates.metadataOptions);
        }
    }
    
    return options;
}

// ============================================================================
// Validation Configuration Transformations
// ============================================================================

/**
 * Determines which validation checkboxes should be checked based on validation data.
 * 
 * @param {Object} validationData - The validation data from backend
 * @param {Object} [validationData.options] - Validation options
 * @param {Array} [validationData.validations] - Manual validation definitions
 * @returns {Object} Checkbox states
 */
export function getValidationCheckboxStates(validationData) {
    const result = {
        auto: false,
        autoFromMetadata: false,
        manual: false
    };
    
    if (validationData.options && validationData.options.enableDataValidation === 'true') {
        result.auto = true;
    }
    
    if (validationData.options && validationData.options.metadataSourceName) {
        result.autoFromMetadata = true;
    }
    
    if (validationData.validations && validationData.validations.length > 0) {
        result.manual = true;
    }
    
    return result;
}

/**
 * Builds validation options from checkbox states.
 * 
 * @param {Object} checkboxStates - The checkbox states
 * @param {boolean} checkboxStates.auto - Auto validation enabled
 * @param {boolean} checkboxStates.autoFromMetadata - Auto from metadata enabled
 * @param {string} [checkboxStates.metadataSourceName] - Metadata source name
 * @param {Object} [checkboxStates.metadataOptions] - Additional metadata options
 * @returns {Object} Validation options
 */
export function buildValidationOptions(checkboxStates) {
    const options = {};
    
    if (checkboxStates.auto) {
        options.enableDataValidation = 'true';
    }
    
    if (checkboxStates.autoFromMetadata && checkboxStates.metadataSourceName) {
        options.metadataSourceName = checkboxStates.metadataSourceName;
        if (checkboxStates.metadataOptions) {
            Object.assign(options, checkboxStates.metadataOptions);
        }
    }
    
    return options;
}

// ============================================================================
// Field Configuration Transformations
// ============================================================================

/**
 * Builds a field object from UI field state.
 * 
 * @param {Object} fieldState - The field UI state
 * @param {string} fieldState.name - Field name
 * @param {string} fieldState.type - Field type
 * @param {Object} [fieldState.options] - Field options (as Map or Object)
 * @param {Array} [fieldState.nestedFields] - Nested fields for struct type
 * @returns {Object} Backend-compatible field object
 */
export function buildFieldFromUIState(fieldState) {
    const result = {
        name: fieldState.name,
        type: fieldState.type
    };
    
    if (fieldState.options) {
        result.options = mapToObject(fieldState.options);
    }
    
    if (fieldState.type === 'struct' && fieldState.nestedFields && fieldState.nestedFields.length > 0) {
        result.fields = fieldState.nestedFields.map(buildFieldFromUIState);
    }
    
    return result;
}

/**
 * Extracts UI field state from a backend field object.
 * 
 * @param {Object} field - Backend field object
 * @returns {Object} UI field state
 */
export function extractUIStateFromField(field) {
    const result = {
        name: field.name,
        type: field.type
    };
    
    if (field.options && Object.keys(field.options).length > 0) {
        result.options = field.options;
    }
    
    if (field.type === 'struct' && field.fields && field.fields.length > 0) {
        result.nestedFields = field.fields.map(extractUIStateFromField);
    }
    
    return result;
}

// ============================================================================
// Configuration Transformations
// ============================================================================

/**
 * Extracts configuration values from a configuration object.
 * 
 * @param {Object} config - Configuration object
 * @param {string} parentKey - Parent key (e.g., 'flag', 'generation')
 * @param {string} key - Configuration key
 * @returns {*} Configuration value or undefined
 */
export function getConfigValue(config, parentKey, key) {
    if (!config || !config[parentKey]) return undefined;
    return config[parentKey][key];
}

/**
 * Sets a configuration value in a configuration object.
 * 
 * @param {Object} config - Configuration object
 * @param {string} parentKey - Parent key (e.g., 'flag', 'generation')
 * @param {string} key - Configuration key
 * @param {*} value - Configuration value
 */
export function setConfigValue(config, parentKey, key, value) {
    if (!config[parentKey]) {
        config[parentKey] = {};
    }
    config[parentKey][key] = value;
}

