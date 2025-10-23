package io.github.datacatering.datacaterer.api

/**
 * Example demonstrating the improved YamlBuilder API
 */
object YamlBuilderUsageExample {

  def main(args: Array[String]): Unit = {
    val yaml = YamlBuilder()

    // Example 1: Loading from a YAML task file (when you only have one task or don't care which one)
    // WARNING: If the YAML task file contains multiple tasks or multiple steps, 
    // you may encounter schema ambiguity.
    val taskFromFile = yaml.taskByFile("/path/to/single-task.yaml")

    // Example 2: Loading by task name (when task is defined globally)
    // WARNING: If the specified task has multiple steps, you may encounter schema ambiguity.
    val taskByName = yaml.taskByName("my_specific_task")

    // Example 3: Loading a specific step from a YAML task file (recommended for multi-step tasks)
    val stepFromFile = yaml.stepByFile("/path/to/multi-step-task.yaml", "specific_step")

    // Example 4: Loading a specific step from a named task (most explicit)
    val stepByName = yaml.stepByName("my_specific_task", "specific_step")

    // Example 5: Loading a specific step from a specific task in a specific file (fully explicit)
    val stepFromFileAndName = yaml.stepByFileAndName(
      "/path/to/multi-task.yaml", 
      "target_task", 
      "target_step"
    )

    println("YamlBuilder API usage examples demonstrated")
    println("Key improvements:")
    println("- Clear distinction between task and step operations")
    println("- Explicit method names that indicate what parameters are expected")
    println("- Better warnings about schema ambiguity")
    println("- No backward compatibility burden")
  }
}