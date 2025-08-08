import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.networknt.schema.JsonSchema
import com.networknt.schema.JsonSchemaFactory
import com.networknt.schema.SpecVersion
import com.networknt.schema.ValidationMessage
import org.gradle.api.DefaultTask
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.TaskAction
import org.gradle.api.GradleException

abstract class ValidateYamlAgainstSchema : DefaultTask() {

    @get:InputDirectory
    abstract val yamlDirectory: DirectoryProperty

    @get:InputFile
    abstract val schemaFile: RegularFileProperty

    @TaskAction
    fun validateYaml() {
        val objectMapper = ObjectMapper(YAMLFactory()) // Registers YAML module
        objectMapper.readTree(schemaFile.get().asFile.readText())
        val schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V201909) //Or V7, V201909, etc.
        val schemaNode: JsonNode = objectMapper.readTree(schemaFile.asFile.get())
        val schema: JsonSchema = schemaFactory.getSchema(schemaNode)

        val validationResults = yamlDirectory.asFile.get().walk()
            .filter { it.isFile && (it.extension == "yaml" || it.extension == "yml") }
            .map { file ->
                try {
                    val yamlNode: JsonNode = objectMapper.readTree(file)
                    val validationResult: Set<ValidationMessage> = schema.validate(yamlNode)
                    file to validationResult
                } catch (e: Exception) {
                    println("Fail read or validation failed, path: ${file.absolutePath}, message: ${e.message}")
                    file to setOf() // Create a validation message for errors
                }
            }
            .toList() // Evaluate and collect all results immediately

        val failedValidations = validationResults.filter { it.second.isNotEmpty() }

        if (failedValidations.isNotEmpty()) {
            val errorMessage = StringBuilder("YAML validation failed for the following files:\n")
            failedValidations.forEach { (file, errors) ->
                errorMessage.append("  - file://${file}:\n")
                errors.forEach { message ->
                    errorMessage.append("    - ${message.message} (path: ${message.schemaPath})\n")
                }
            }
            throw GradleException(errorMessage.toString())
        } else {
            println("All YAML files validated successfully.")
        }
    }
}