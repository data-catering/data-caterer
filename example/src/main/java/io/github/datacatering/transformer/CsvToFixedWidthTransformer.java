package io.github.datacatering.transformer;

import java.util.Map;

/**
 * Example Java per-record transformer that converts CSV to fixed-width format.
 * 
 * This demonstrates how to create a transformer in Java that can be used
 * with Data Caterer's transformation feature.
 * 
 * Converts from CSV:
 * <pre>
 * ACC001,John Doe,1000.50
 * </pre>
 * 
 * To fixed-width format:
 * <pre>
 * ACC001    John Doe          1000.50
 * </pre>
 * 
 * Usage in Data Caterer (Java):
 * <pre>{@code
 * var task = csv("accounts", "/tmp/accounts")
 *     .fields(
 *         field().name("account_id"),
 *         field().name("name"),
 *         field().name("balance")
 *     )
 *     .transformationPerRecord(
 *         "io.github.datacatering.transformer.CsvToFixedWidthTransformer",
 *         "transform"
 *     )
 *     .transformationOptions(Map.of(
 *         "field1Width", "10",
 *         "field2Width", "20",
 *         "field3Width", "10"
 *     ));
 * }</pre>
 */
public class CsvToFixedWidthTransformer {
    
    /**
     * Transform a CSV record to fixed-width format with default widths.
     * 
     * @param record The input CSV record
     * @return The fixed-width formatted record
     */
    public String transform(String record) {
        String[] fields = record.split(",");
        StringBuilder result = new StringBuilder();
        
        // Default widths: 10, 20, 10
        int[] widths = {10, 20, 10};
        
        for (int i = 0; i < fields.length && i < widths.length; i++) {
            String field = fields[i].trim();
            result.append(padRight(field, widths[i]));
        }
        
        return result.toString();
    }
    
    /**
     * Transform a CSV record to fixed-width format with configurable widths.
     * 
     * @param record The input CSV record
     * @param options Configuration options containing field widths:
     *                - "field1Width": width for first field (default: 10)
     *                - "field2Width": width for second field (default: 20)
     *                - "field3Width": width for third field (default: 10)
     *                - "paddingChar": character to use for padding (default: space)
     * @return The fixed-width formatted record
     */
    public String transform(String record, Map<String, String> options) {
        String[] fields = record.split(",");
        StringBuilder result = new StringBuilder();
        
        // Get field widths from options or use defaults
        int field1Width = Integer.parseInt(options.getOrDefault("field1Width", "10"));
        int field2Width = Integer.parseInt(options.getOrDefault("field2Width", "20"));
        int field3Width = Integer.parseInt(options.getOrDefault("field3Width", "10"));
        String paddingChar = options.getOrDefault("paddingChar", " ");
        
        int[] widths = {field1Width, field2Width, field3Width};
        
        for (int i = 0; i < fields.length && i < widths.length; i++) {
            String field = fields[i].trim();
            result.append(padRight(field, widths[i], paddingChar));
        }
        
        return result.toString();
    }
    
    /**
     * Pad a string to the right with spaces.
     */
    private String padRight(String str, int length) {
        return padRight(str, length, " ");
    }
    
    /**
     * Pad a string to the right with a specified character.
     */
    private String padRight(String str, int length, String paddingChar) {
        if (str.length() >= length) {
            return str.substring(0, length);
        }
        
        StringBuilder padded = new StringBuilder(str);
        while (padded.length() < length) {
            padded.append(paddingChar);
        }
        
        return padded.toString();
    }
}

