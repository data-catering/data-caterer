package io.github.datacatering.transformer

/**
 * Example per-record transformer that converts all text to uppercase.
 * 
 * This demonstrates the simplest form of per-record transformation where
 * each line/record is transformed independently as a string.
 * 
 * Usage in Data Caterer:
 * {{{
 * val task = csv("accounts", "/tmp/accounts")
 *   .fields(
 *     field.name("account_id"),
 *     field.name("name")
 *   )
 *   .transformationPerRecord(
 *     "io.github.datacatering.transformer.UpperCasePerRecordTransformer",
 *     "transform"
 *   )
 * }}}
 */
class UpperCasePerRecordTransformer {
  
  /**
   * Transform a single record/line to uppercase.
   * 
   * @param record The input record as a string (e.g., CSV line, JSON object)
   * @return The transformed record
   */
  def transform(record: String): String = {
    record.toUpperCase
  }
  
  /**
   * Transform a single record with options support.
   * 
   * @param record The input record as a string
   * @param options Configuration options (e.g., "prefix" -> "ACCOUNT_")
   * @return The transformed record
   */
  def transformWithOptions(record: String, options: Map[String, String]): String = {
    val prefix = options.getOrElse("prefix", "")
    val suffix = options.getOrElse("suffix", "")
    prefix + record.toUpperCase + suffix
  }
}

