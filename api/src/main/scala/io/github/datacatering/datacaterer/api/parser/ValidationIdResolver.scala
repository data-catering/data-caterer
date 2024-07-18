package io.github.datacatering.datacaterer.api.parser

import com.fasterxml.jackson.annotation.JsonTypeInfo.Id
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.jsontype.impl.TypeIdResolverBase
import com.fasterxml.jackson.databind.{DatabindContext, JavaType, JsonSerializer, SerializerProvider}
import io.github.datacatering.datacaterer.api.ValidationBuilder
import io.github.datacatering.datacaterer.api.model.Constants.{VALIDATION_COLUMN_NAME_COUNT_BETWEEN, VALIDATION_COLUMN_NAME_COUNT_EQUAL, VALIDATION_COLUMN_NAME_MATCH_ORDER, VALIDATION_COLUMN_NAME_MATCH_SET}
import io.github.datacatering.datacaterer.api.model.{ColumnNamesValidation, ExpressionValidation, GroupByValidation, UpstreamDataSourceValidation}

import scala.util.Try

class ValidationIdResolver extends TypeIdResolverBase {
  private var superType: JavaType = null

  override def init(bt: JavaType): Unit = {
    superType = bt
  }

  override def idFromValue(value: Any): String = {
    idFromValueAndType(value, value.getClass)
  }

  override def idFromBaseType(): String = {
    idFromValueAndType(null, superType.getRawClass)
  }

  override def idFromValueAndType(value: Any, suggestedType: Class[_]): String = {
    val Expr = classOf[ExpressionValidation]
    val Group = classOf[GroupByValidation]
    val Upstream = classOf[UpstreamDataSourceValidation]
    val Columns = classOf[ColumnNamesValidation]
    suggestedType match {
      case Expr => "ExpressionValidation"
      case Group => "GroupByValidation"
      case Upstream => "UpstreamDataSourceValidation"
      case Columns => "ColumnNamesValidation"
      case _ => "ExpressionValidation"
    }
  }

  override def getMechanism: Id = null

  override def typeFromId(context: DatabindContext, id: String): JavaType = {
    val subType = id match {
      case "ExpressionValidation" => classOf[ExpressionValidation]
      case "GroupByValidation" => classOf[GroupByValidation]
      case "UpstreamDataSourceValidation" => classOf[UpstreamDataSourceValidation]
      case "ColumnNamesValidation" => classOf[ColumnNamesValidation]
      case _ => classOf[ExpressionValidation]
    }
    context.constructSpecializedType(superType, subType)
  }
}

//class ValidationDeserializer extends JsonDeserializer[Validation] {
//  override def deserialize(p: JsonParser, ctxt: DeserializationContext): Validation = {
//
//  }
//}

class ValidationBuilderSerializer extends JsonSerializer[ValidationBuilder] {
  override def serialize(value: ValidationBuilder, gen: JsonGenerator, serializers: SerializerProvider): Unit = {
    val validation = value.validation
    Try(gen.writeStartObject())
    validation.preFilter.foreach(preFilter => {
      gen.writeStringField("preFilterExpr", preFilter.toExpression)
    })
    validation match {
      case ExpressionValidation(expr, selectExpr) =>
        gen.writeArrayFieldStart("selectExpr")
        selectExpr.foreach(gen.writeObject)
        gen.writeEndArray()
        gen.writeStringField("whereExpr", expr)
      case GroupByValidation(groupByCols, aggCol, aggType, expr) =>
        gen.writeArrayFieldStart("groupByCols")
        groupByCols.foreach(gen.writeObject)
        gen.writeEndArray()
        gen.writeStringField("aggCol", aggCol)
        gen.writeStringField("aggType", aggType)
        gen.writeStringField("expr", expr)
      case ColumnNamesValidation(columnNameValidationType, count, minCount, maxCount, names) =>
        gen.writeStringField("columnNameType", columnNameValidationType)
        columnNameValidationType match {
          case VALIDATION_COLUMN_NAME_COUNT_EQUAL =>
            gen.writeStringField("count", count.toString)
          case VALIDATION_COLUMN_NAME_COUNT_BETWEEN =>
            gen.writeStringField("min", minCount.toString)
            gen.writeStringField("max", maxCount.toString)
          case VALIDATION_COLUMN_NAME_MATCH_ORDER =>
            gen.writeStringField("matchOrder", names.mkString(","))
          case VALIDATION_COLUMN_NAME_MATCH_SET =>
            gen.writeStringField("matchSet", names.mkString(","))
        }
      case UpstreamDataSourceValidation(validationBuilder, upstreamDataSource, upstreamReadOptions, joinCols, joinType) =>
        gen.writeStringField("upstreamDataSource", upstreamDataSource.connectionConfigWithTaskBuilder.dataSourceName)
        gen.writeObjectFieldStart("upstreamReadOptions")
        upstreamReadOptions.foreach(opt => gen.writeObjectField(opt._1, opt._2))
        gen.writeEndObject()
        gen.writeStringField("joinColumns", joinCols.mkString(","))
        gen.writeStringField("joinType", joinType)
        gen.writeObjectFieldStart("validation")
        serialize(validationBuilder, gen, serializers)
      case _ =>
    }
    gen.writeEndObject()
  }
}
