/**
 * Tests for pure data transformation functions.
 * 
 * These tests verify the actual implementation code in data-transformers.js,
 * which contains the core logic for save/load operations without DOM dependencies.
 */

import {
    buildCountFromUIState,
    extractUIStateFromCount,
    estimateRecordCountFromConfig,
    getGenerationCheckboxStates,
    buildGenerationOptions,
    getValidationCheckboxStates,
    buildValidationOptions,
    buildFieldFromUIState,
    extractUIStateFromField,
    mapToObject,
    camelize
} from './data-transformers';

describe('Record Count Transformations', () => {
    
    describe('buildCountFromUIState', () => {
        
        test('should build simple records count', () => {
            const uiState = {
                isRecordsBetween: false,
                records: 1000
            };
            
            const result = buildCountFromUIState(uiState);
            
            expect(result.records).toBe(1000);
            expect(result.options).toBeUndefined();
        });
        
        test('should build records between with min/max in options', () => {
            const uiState = {
                isRecordsBetween: true,
                recordsMin: 500,
                recordsMax: 1500
            };
            
            const result = buildCountFromUIState(uiState);
            
            expect(result.records).toBeUndefined();
            expect(result.options).toBeDefined();
            expect(result.options.min).toBe(500);
            expect(result.options.max).toBe(1500);
        });
        
        test('should build per-field count with simple count', () => {
            const uiState = {
                isRecordsBetween: false,
                records: 1000,
                perFieldNames: ['account_id', 'user_id'],
                isPerFieldBetween: false,
                perFieldCount: 5
            };
            
            const result = buildCountFromUIState(uiState);
            
            expect(result.records).toBe(1000);
            expect(result.perField).toBeDefined();
            expect(result.perField.fieldNames).toEqual(['account_id', 'user_id']);
            expect(result.perField.count).toBe(5);
            expect(result.perField.options).toBeUndefined();
        });
        
        test('should build per-field count with min/max in options', () => {
            const uiState = {
                isRecordsBetween: false,
                records: 1000,
                perFieldNames: ['account_id'],
                isPerFieldBetween: true,
                perFieldMin: 2,
                perFieldMax: 10
            };
            
            const result = buildCountFromUIState(uiState);
            
            expect(result.perField.fieldNames).toEqual(['account_id']);
            expect(result.perField.count).toBeUndefined();
            expect(result.perField.options).toBeDefined();
            expect(result.perField.options.min).toBe(2);
            expect(result.perField.options.max).toBe(10);
        });
        
        test('should build per-field count with exponential distribution', () => {
            const uiState = {
                isRecordsBetween: false,
                records: 1000,
                perFieldNames: ['account_id'],
                isPerFieldBetween: true,
                perFieldMin: 2,
                perFieldMax: 10,
                distribution: 'exponential',
                rateParam: 0.5
            };
            
            const result = buildCountFromUIState(uiState);
            
            expect(result.perField.options.distribution).toBe('exponential');
            expect(result.perField.options.rateParam).toBe(0.5);
        });
        
        test('should not include uniform distribution in options', () => {
            const uiState = {
                isRecordsBetween: false,
                records: 1000,
                perFieldNames: ['account_id'],
                isPerFieldBetween: false,
                perFieldCount: 5,
                distribution: 'uniform'
            };
            
            const result = buildCountFromUIState(uiState);
            
            expect(result.perField.options).toBeUndefined();
        });
        
        test('should not include perField if no field names', () => {
            const uiState = {
                isRecordsBetween: false,
                records: 1000,
                perFieldNames: []
            };
            
            const result = buildCountFromUIState(uiState);
            
            expect(result.perField).toBeUndefined();
        });
    });
    
    describe('extractUIStateFromCount', () => {
        
        test('should extract simple records count', () => {
            const count = { records: 1000 };
            
            const result = extractUIStateFromCount(count);
            
            expect(result.isRecordsBetween).toBe(false);
            expect(result.records).toBe(1000);
        });
        
        test('should extract records between from options', () => {
            const count = { options: { min: 500, max: 1500 } };
            
            const result = extractUIStateFromCount(count);
            
            expect(result.isRecordsBetween).toBe(true);
            expect(result.recordsMin).toBe(500);
            expect(result.recordsMax).toBe(1500);
        });
        
        test('should extract per-field count with simple count', () => {
            const count = {
                records: 1000,
                perField: {
                    fieldNames: ['account_id', 'user_id'],
                    count: 5
                }
            };
            
            const result = extractUIStateFromCount(count);
            
            expect(result.perFieldNames).toEqual(['account_id', 'user_id']);
            expect(result.isPerFieldBetween).toBe(false);
            expect(result.perFieldCount).toBe(5);
        });
        
        test('should extract per-field count with min/max', () => {
            const count = {
                records: 1000,
                perField: {
                    fieldNames: ['account_id'],
                    options: { min: 2, max: 10 }
                }
            };
            
            const result = extractUIStateFromCount(count);
            
            expect(result.perFieldNames).toEqual(['account_id']);
            expect(result.isPerFieldBetween).toBe(true);
            expect(result.perFieldMin).toBe(2);
            expect(result.perFieldMax).toBe(10);
        });
        
        test('should extract distribution settings', () => {
            const count = {
                records: 1000,
                perField: {
                    fieldNames: ['account_id'],
                    options: { min: 2, max: 10, distribution: 'exponential', rateParam: 0.5 }
                }
            };
            
            const result = extractUIStateFromCount(count);
            
            expect(result.distribution).toBe('exponential');
            expect(result.rateParam).toBe(0.5);
        });
        
        test('should return empty object for null/undefined count', () => {
            expect(extractUIStateFromCount(null)).toEqual({});
            expect(extractUIStateFromCount(undefined)).toEqual({});
        });
    });
    
    describe('estimateRecordCountFromConfig', () => {
        
        test('should estimate from simple records count', () => {
            const count = { records: 1000 };
            expect(estimateRecordCountFromConfig(count)).toBe(1000);
        });
        
        test('should estimate average from min/max', () => {
            const count = { options: { min: 500, max: 1500 } };
            expect(estimateRecordCountFromConfig(count)).toBe(1000);
        });
        
        test('should multiply by per-field count', () => {
            const count = {
                records: 1000,
                perField: {
                    fieldNames: ['account_id'],
                    count: 5
                }
            };
            expect(estimateRecordCountFromConfig(count)).toBe(5000);
        });
        
        test('should multiply by per-field average', () => {
            const count = {
                records: 1000,
                perField: {
                    fieldNames: ['account_id'],
                    options: { min: 2, max: 8 }
                }
            };
            expect(estimateRecordCountFromConfig(count)).toBe(5000);
        });
        
        test('should return default for null count', () => {
            expect(estimateRecordCountFromConfig(null)).toBe(1000);
        });
    });
    
    describe('Round-trip: buildCountFromUIState -> extractUIStateFromCount', () => {
        
        test('simple records should round-trip correctly', () => {
            const original = {
                isRecordsBetween: false,
                records: 5000
            };
            
            const backendFormat = buildCountFromUIState(original);
            const restored = extractUIStateFromCount(backendFormat);
            
            expect(restored.isRecordsBetween).toBe(false);
            expect(restored.records).toBe(5000);
        });
        
        test('records between should round-trip correctly', () => {
            const original = {
                isRecordsBetween: true,
                recordsMin: 500,
                recordsMax: 1500
            };
            
            const backendFormat = buildCountFromUIState(original);
            const restored = extractUIStateFromCount(backendFormat);
            
            expect(restored.isRecordsBetween).toBe(true);
            expect(restored.recordsMin).toBe(500);
            expect(restored.recordsMax).toBe(1500);
        });
        
        test('per-field count should round-trip correctly', () => {
            const original = {
                isRecordsBetween: false,
                records: 1000,
                perFieldNames: ['account_id', 'user_id'],
                isPerFieldBetween: false,
                perFieldCount: 5
            };
            
            const backendFormat = buildCountFromUIState(original);
            const restored = extractUIStateFromCount(backendFormat);
            
            expect(restored.perFieldNames).toEqual(['account_id', 'user_id']);
            expect(restored.isPerFieldBetween).toBe(false);
            expect(restored.perFieldCount).toBe(5);
        });
        
        test('per-field with min/max and distribution should round-trip correctly', () => {
            const original = {
                isRecordsBetween: false,
                records: 1000,
                perFieldNames: ['account_id'],
                isPerFieldBetween: true,
                perFieldMin: 2,
                perFieldMax: 10,
                distribution: 'exponential',
                rateParam: 0.5
            };
            
            const backendFormat = buildCountFromUIState(original);
            const restored = extractUIStateFromCount(backendFormat);
            
            expect(restored.isPerFieldBetween).toBe(true);
            expect(restored.perFieldMin).toBe(2);
            expect(restored.perFieldMax).toBe(10);
            expect(restored.distribution).toBe('exponential');
            expect(restored.rateParam).toBe(0.5);
        });
    });
});

describe('Generation Configuration Transformations', () => {
    
    describe('getGenerationCheckboxStates', () => {
        
        test('should detect auto generation enabled', () => {
            const taskData = {
                options: { enableDataGeneration: 'true' }
            };
            
            const result = getGenerationCheckboxStates(taskData);
            
            expect(result.auto).toBe(true);
            expect(result.autoFromMetadata).toBe(false);
            expect(result.manual).toBe(false);
        });
        
        test('should detect auto from metadata enabled', () => {
            const taskData = {
                options: { metadataSourceName: 'my-metadata-source' }
            };
            
            const result = getGenerationCheckboxStates(taskData);
            
            expect(result.auto).toBe(false);
            expect(result.autoFromMetadata).toBe(true);
            expect(result.manual).toBe(false);
        });
        
        test('should detect manual fields', () => {
            const taskData = {
                fields: [
                    { name: 'id', type: 'string' },
                    { name: 'name', type: 'string' }
                ]
            };
            
            const result = getGenerationCheckboxStates(taskData);
            
            expect(result.auto).toBe(false);
            expect(result.autoFromMetadata).toBe(false);
            expect(result.manual).toBe(true);
        });
        
        test('should detect multiple options enabled', () => {
            const taskData = {
                options: {
                    enableDataGeneration: 'true',
                    metadataSourceName: 'my-metadata-source'
                },
                fields: [{ name: 'id', type: 'string' }]
            };
            
            const result = getGenerationCheckboxStates(taskData);
            
            expect(result.auto).toBe(true);
            expect(result.autoFromMetadata).toBe(true);
            expect(result.manual).toBe(true);
        });
        
        test('should return all false for empty task', () => {
            const taskData = {};
            
            const result = getGenerationCheckboxStates(taskData);
            
            expect(result.auto).toBe(false);
            expect(result.autoFromMetadata).toBe(false);
            expect(result.manual).toBe(false);
        });
    });
    
    describe('buildGenerationOptions', () => {
        
        test('should build options for auto generation', () => {
            const checkboxStates = { auto: true };
            
            const result = buildGenerationOptions(checkboxStates);
            
            expect(result.enableDataGeneration).toBe('true');
        });
        
        test('should build options for auto from metadata', () => {
            const checkboxStates = {
                autoFromMetadata: true,
                metadataSourceName: 'my-metadata-source',
                metadataOptions: { tableFQN: 'db.schema.table' }
            };
            
            const result = buildGenerationOptions(checkboxStates);
            
            expect(result.metadataSourceName).toBe('my-metadata-source');
            expect(result.tableFQN).toBe('db.schema.table');
        });
        
        test('should not include metadata without source name', () => {
            const checkboxStates = {
                autoFromMetadata: true
                // no metadataSourceName
            };
            
            const result = buildGenerationOptions(checkboxStates);
            
            expect(result.metadataSourceName).toBeUndefined();
        });
    });
    
    describe('Round-trip: buildGenerationOptions -> getGenerationCheckboxStates', () => {
        
        test('auto generation should round-trip correctly', () => {
            const original = { auto: true };
            
            const options = buildGenerationOptions(original);
            const restored = getGenerationCheckboxStates({ options });
            
            expect(restored.auto).toBe(true);
        });
        
        test('auto from metadata should round-trip correctly', () => {
            const original = {
                autoFromMetadata: true,
                metadataSourceName: 'my-source'
            };
            
            const options = buildGenerationOptions(original);
            const restored = getGenerationCheckboxStates({ options });
            
            expect(restored.autoFromMetadata).toBe(true);
        });
    });
});

describe('Validation Configuration Transformations', () => {
    
    describe('getValidationCheckboxStates', () => {
        
        test('should detect auto validation enabled', () => {
            const validationData = {
                options: { enableDataValidation: 'true' }
            };
            
            const result = getValidationCheckboxStates(validationData);
            
            expect(result.auto).toBe(true);
            expect(result.autoFromMetadata).toBe(false);
            expect(result.manual).toBe(false);
        });
        
        test('should detect auto from metadata enabled', () => {
            const validationData = {
                options: { metadataSourceName: 'my-metadata-source' }
            };
            
            const result = getValidationCheckboxStates(validationData);
            
            expect(result.auto).toBe(false);
            expect(result.autoFromMetadata).toBe(true);
            expect(result.manual).toBe(false);
        });
        
        test('should detect manual validations', () => {
            const validationData = {
                validations: [
                    { field: 'account_id', validation: [{ type: 'null', negate: true }] }
                ]
            };
            
            const result = getValidationCheckboxStates(validationData);
            
            expect(result.auto).toBe(false);
            expect(result.autoFromMetadata).toBe(false);
            expect(result.manual).toBe(true);
        });
    });
    
    describe('buildValidationOptions', () => {
        
        test('should build options for auto validation', () => {
            const checkboxStates = { auto: true };
            
            const result = buildValidationOptions(checkboxStates);
            
            expect(result.enableDataValidation).toBe('true');
        });
        
        test('should build options for auto from metadata', () => {
            const checkboxStates = {
                autoFromMetadata: true,
                metadataSourceName: 'my-metadata-source'
            };
            
            const result = buildValidationOptions(checkboxStates);
            
            expect(result.metadataSourceName).toBe('my-metadata-source');
        });
    });
    
    describe('Round-trip: buildValidationOptions -> getValidationCheckboxStates', () => {
        
        test('auto validation should round-trip correctly', () => {
            const original = { auto: true };
            
            const options = buildValidationOptions(original);
            const restored = getValidationCheckboxStates({ options });
            
            expect(restored.auto).toBe(true);
        });
    });
});

describe('Field Configuration Transformations', () => {
    
    describe('buildFieldFromUIState', () => {
        
        test('should build simple field', () => {
            const fieldState = {
                name: 'user_id',
                type: 'long'
            };
            
            const result = buildFieldFromUIState(fieldState);
            
            expect(result.name).toBe('user_id');
            expect(result.type).toBe('long');
            expect(result.options).toBeUndefined();
        });
        
        test('should build field with options from Map', () => {
            const options = new Map();
            options.set('min', 0);
            options.set('max', 1000);
            
            const fieldState = {
                name: 'amount',
                type: 'double',
                options: options
            };
            
            const result = buildFieldFromUIState(fieldState);
            
            expect(result.name).toBe('amount');
            expect(result.type).toBe('double');
            expect(result.options).toEqual({ min: 0, max: 1000 });
        });
        
        test('should build field with options from Object', () => {
            const fieldState = {
                name: 'email',
                type: 'string',
                options: { expression: '#{Internet.emailAddress}' }
            };
            
            const result = buildFieldFromUIState(fieldState);
            
            expect(result.options.expression).toBe('#{Internet.emailAddress}');
        });
        
        test('should build nested struct fields', () => {
            const fieldState = {
                name: 'address',
                type: 'struct',
                nestedFields: [
                    { name: 'street', type: 'string' },
                    { name: 'city', type: 'string' }
                ]
            };
            
            const result = buildFieldFromUIState(fieldState);
            
            expect(result.type).toBe('struct');
            expect(result.fields).toHaveLength(2);
            expect(result.fields[0].name).toBe('street');
            expect(result.fields[1].name).toBe('city');
        });
    });
    
    describe('extractUIStateFromField', () => {
        
        test('should extract simple field', () => {
            const field = {
                name: 'user_id',
                type: 'long'
            };
            
            const result = extractUIStateFromField(field);
            
            expect(result.name).toBe('user_id');
            expect(result.type).toBe('long');
        });
        
        test('should extract field with options', () => {
            const field = {
                name: 'amount',
                type: 'double',
                options: { min: 0, max: 1000 }
            };
            
            const result = extractUIStateFromField(field);
            
            expect(result.options).toEqual({ min: 0, max: 1000 });
        });
        
        test('should extract nested struct fields', () => {
            const field = {
                name: 'address',
                type: 'struct',
                fields: [
                    { name: 'street', type: 'string' },
                    { name: 'city', type: 'string' }
                ]
            };
            
            const result = extractUIStateFromField(field);
            
            expect(result.nestedFields).toHaveLength(2);
            expect(result.nestedFields[0].name).toBe('street');
        });
    });
    
    describe('Round-trip: buildFieldFromUIState -> extractUIStateFromField', () => {
        
        test('simple field should round-trip correctly', () => {
            const original = {
                name: 'user_id',
                type: 'long'
            };
            
            const backendFormat = buildFieldFromUIState(original);
            const restored = extractUIStateFromField(backendFormat);
            
            expect(restored.name).toBe('user_id');
            expect(restored.type).toBe('long');
        });
        
        test('field with options should round-trip correctly', () => {
            const original = {
                name: 'amount',
                type: 'double',
                options: { min: 0, max: 1000.50, expression: '#{Number.randomDouble(0, 1000)}' }
            };
            
            const backendFormat = buildFieldFromUIState(original);
            const restored = extractUIStateFromField(backendFormat);
            
            expect(restored.options.min).toBe(0);
            expect(restored.options.max).toBe(1000.50);
            expect(restored.options.expression).toBe('#{Number.randomDouble(0, 1000)}');
        });
        
        test('nested struct should round-trip correctly', () => {
            const original = {
                name: 'user',
                type: 'struct',
                nestedFields: [
                    { name: 'name', type: 'string' },
                    {
                        name: 'address',
                        type: 'struct',
                        nestedFields: [
                            { name: 'street', type: 'string' },
                            { name: 'city', type: 'string' }
                        ]
                    }
                ]
            };
            
            const backendFormat = buildFieldFromUIState(original);
            const restored = extractUIStateFromField(backendFormat);
            
            expect(restored.nestedFields).toHaveLength(2);
            expect(restored.nestedFields[1].nestedFields).toHaveLength(2);
            expect(restored.nestedFields[1].nestedFields[0].name).toBe('street');
        });
    });
});

describe('Utility Functions', () => {
    
    describe('mapToObject', () => {
        
        test('should convert Map to object', () => {
            const map = new Map();
            map.set('key1', 'value1');
            map.set('key2', 'value2');
            
            const result = mapToObject(map);
            
            expect(result).toEqual({ key1: 'value1', key2: 'value2' });
        });
        
        test('should return object as-is', () => {
            const obj = { key1: 'value1' };
            
            const result = mapToObject(obj);
            
            expect(result).toBe(obj);
        });
    });
    
    describe('camelize', () => {
        
        test('should convert space-separated to camelCase', () => {
            expect(camelize('hello world')).toBe('helloWorld');
        });
        
        test('should handle single word', () => {
            expect(camelize('hello')).toBe('hello');
        });
        
        test('should handle already camelCase', () => {
            expect(camelize('helloWorld')).toBe('helloWorld');
        });
    });
});

