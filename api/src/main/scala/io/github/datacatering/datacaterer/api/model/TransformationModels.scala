package io.github.datacatering.datacaterer.api.model

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
 * Configuration for custom transformation of generated data.
 * Transformations can operate in two modes:
 * - "per-record": Transform each record/line in the file individually
 * - "whole-file": Transform the entire file as a unit
 *
 * Both modes execute after the file has been written ("last mile" transformation).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
case class TransformationConfig(
                                 className: String = "",
                                 methodName: String = "transform",
                                 mode: String = "whole-file",  // "per-record" or "whole-file"
                                 outputPath: Option[String] = None,  // Output path (if different from input)
                                 deleteOriginal: Boolean = false,  // Delete original file after transformation
                                 options: Map[String, String] = Map(),
                                 enabled: Boolean = true
                               ) {
  /**
   * Check if transformation is configured and enabled
   */
  def isEnabled: Boolean = className.nonEmpty && enabled

  /**
   * Check if mode is per-record
   */
  def isPerRecord: Boolean = mode.equalsIgnoreCase("per-record")

  /**
   * Check if mode is whole-file
   */
  def isWholeFile: Boolean = mode.equalsIgnoreCase("whole-file")
}
