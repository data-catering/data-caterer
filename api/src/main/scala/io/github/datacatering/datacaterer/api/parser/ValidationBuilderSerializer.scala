package io.github.datacatering.datacaterer.api.parser

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}
import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.Constants.{VALIDATION_FIELD_NAME_COUNT_BETWEEN, VALIDATION_FIELD_NAME_COUNT_EQUAL, VALIDATION_FIELD_NAME_MATCH_ORDER, VALIDATION_FIELD_NAME_MATCH_SET}
import io.github.datacatering.datacaterer.api.model.{ExpressionValidation, FieldNamesValidation, FieldValidations, GroupByValidation, UpstreamDataSourceValidation}

import scala.util.Try

class ValidationBuilderSerializer extends JsonSerializer[ValidationBuilder] {
  override def serialize(value: ValidationBuilder, gen: JsonGenerator, serializers: SerializerProvider): Unit = {
    val validation = value.validation
    Try(gen.writeStartObject())
    validation.preFilter.foreach(preFilter => {
      gen.writeStringField("preFilterExpr", preFilter.toExpression)
    })
    validation match {
      case FieldValidations(field, fieldValid) =>
        gen.writeStringField("field", field)
        gen.writeArrayFieldStart("validation")
        fieldValid.foreach(fv => {
//          gen.writeStringField("type", fv.`type`)
//          fv match {
//            case BetweenFieldValidation(min, max, negate) =>
//              gen.writeStringField("min", min.toString)
//              gen.writeStringField("max", max.toString)
//              gen.writeStringField("negate", negate.toString)
//          }
        })
        gen.writeEndArray()
      case ExpressionValidation(expr, selectExpr) =>
        gen.writeArrayFieldStart("selectExpr")
        selectExpr.foreach(gen.writeObject)
        gen.writeEndArray()
        gen.writeStringField("whereExpr", expr)
      case GroupByValidation(groupByFields, aggField, aggType, expr, validation) =>
        gen.writeArrayFieldStart("groupByFields")
        groupByFields.foreach(gen.writeObject)
        gen.writeEndArray()
        gen.writeStringField("aggField", aggField)
        gen.writeStringField("aggType", aggType)
        gen.writeStringField("expr", expr)
        gen.writeArrayFieldStart("validation")
        validation.foreach(v => serialize(ValidationBuilder(v), gen, serializers))
        gen.writeEndArray()
      case FieldNamesValidation(fieldNameValidationType, count, min, max, names) =>
        gen.writeStringField("fieldNameType", fieldNameValidationType)
        fieldNameValidationType match {
          case VALIDATION_FIELD_NAME_COUNT_EQUAL =>
            gen.writeStringField("count", count.toString)
          case VALIDATION_FIELD_NAME_COUNT_BETWEEN =>
            gen.writeStringField("min", min.toString)
            gen.writeStringField("max", max.toString)
          case VALIDATION_FIELD_NAME_MATCH_ORDER =>
            gen.writeStringField("matchOrder", names.mkString(","))
          case VALIDATION_FIELD_NAME_MATCH_SET =>
            gen.writeStringField("matchSet", names.mkString(","))
        }
      case UpstreamDataSourceValidation(validationBuilders, upstreamDataSource, upstreamReadOptions, joinFields, joinType) =>
        gen.writeStringField("upstreamDataSource", upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName)
        gen.writeObjectFieldStart("upstreamReadOptions")
        upstreamReadOptions.foreach(opt => gen.writeObjectField(opt._1, opt._2))
        gen.writeEndObject()
        gen.writeStringField("joinFields", joinFields.mkString(","))
        gen.writeStringField("joinType", joinType)
        gen.writeArrayFieldStart("validations")
        validationBuilders.foreach(v => serialize(v, gen, serializers))
        gen.writeEndArray()
      case _ =>
    }
    gen.writeEndObject()
  }
}
