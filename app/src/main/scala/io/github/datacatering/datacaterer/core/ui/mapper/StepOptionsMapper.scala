package io.github.datacatering.datacaterer.core.ui.mapper

import io.github.datacatering.datacaterer.api.model.Constants._
import org.apache.log4j.Logger

/**
 * Maps UI step options to internal format required by data sources
 * 
 * The UI may send options in a user-friendly format (e.g., separate schema and table fields)
 * that need to be converted to the format expected by the underlying data source connectors
 */
object StepOptionsMapper {

  private val LOGGER = Logger.getLogger(getClass.getName)

  /**
   * Convert UI step options to the format expected by the data source
   * 
   * @param options Step options from the UI
   * @param format Data source format (jdbc, cassandra, etc.)
   * @return Converted options
   */
  def mapStepOptions(options: Map[String, String], format: String): Map[String, String] = {
    format.toLowerCase match {
      case JDBC => mapJdbcOptions(options)
      case CASSANDRA => mapCassandraOptions(options)
      case ICEBERG => mapIcebergOptions(options)
      case BIGQUERY => mapBigQueryOptions(options)
      case _ => options // No mapping needed for other formats
    }
  }

  /**
   * Map JDBC options: convert schema + table to dbtable
   * 
   * JDBC connections require 'dbtable' in format 'schema.table' or just 'table'
   * The UI sends these as separate 'schema' and 'table' fields
   */
  private def mapJdbcOptions(options: Map[String, String]): Map[String, String] = {
    val hasSchema = options.contains("schema")
    val hasTable = options.contains("table")
    val hasDbTable = options.contains(JDBC_TABLE)
    
    if (!hasDbTable && hasSchema && hasTable) {
      // Convert schema + table to dbtable
      val schema = options("schema")
      val table = options("table")
      val dbtable = s"$schema.$table"
      LOGGER.debug(s"Converting JDBC schema+table to dbtable: schema=$schema, table=$table, dbtable=$dbtable")
      options - "schema" - "table" + (JDBC_TABLE -> dbtable)
    } else if (!hasDbTable && hasTable) {
      // Only table provided, rename to dbtable
      LOGGER.debug(s"Converting JDBC table to dbtable: table=${options("table")}")
      options - "table" + (JDBC_TABLE -> options("table"))
    } else {
      // dbtable already exists or neither schema nor table provided
      options
    }
  }

  /**
   * Map Cassandra options: ensure keyspace and table are properly set
   * 
   * Cassandra requires separate 'keyspace' and 'table' options
   * This is already the format the UI sends, so no conversion needed currently
   */
  private def mapCassandraOptions(options: Map[String, String]): Map[String, String] = {
    // Cassandra already uses keyspace and table separately
    // No conversion needed, but we validate they exist
    if (options.contains(CASSANDRA_KEYSPACE) && options.contains(CASSANDRA_TABLE)) {
      LOGGER.debug(s"Cassandra options validated: keyspace=${options(CASSANDRA_KEYSPACE)}, table=${options(CASSANDRA_TABLE)}")
    }
    options
  }

  /**
   * Map Iceberg options: ensure table is properly formatted
   * 
   * Iceberg requires 'table' in format 'database.table'
   * The UI sends this as a single 'table' field
   */
  private def mapIcebergOptions(options: Map[String, String]): Map[String, String] = {
    // Iceberg uses 'table' field which should already be in database.table format from UI
    if (options.contains(TABLE)) {
      LOGGER.debug(s"Iceberg table option: table=${options(TABLE)}")
    }
    options
  }

  /**
   * Map BigQuery options: ensure table is properly formatted
   * 
   * BigQuery requires 'table' in format 'project.dataset.table'
   * The UI sends this as a single 'table' field
   */
  private def mapBigQueryOptions(options: Map[String, String]): Map[String, String] = {
    // BigQuery uses 'table' field which should already be in project.dataset.table format from UI
    if (options.contains(TABLE)) {
      LOGGER.debug(s"BigQuery table option: table=${options(TABLE)}")
    }
    options
  }
}
