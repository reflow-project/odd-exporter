package io.piveau.consus

import io.piveau.utils.normalizeDateTime
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.sqlclient.Tuple
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.poi.ss.usermodel.*
import org.slf4j.LoggerFactory
import java.io.File
import java.io.InputStream
import java.io.InputStreamReader
import java.math.BigInteger
import java.nio.charset.Charset
import java.time.*
import java.time.format.DateTimeFormatter
import kotlin.math.floor
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ArrayNode



fun parseTransormedData(jsonString: String): JsonObject{

    val json = JsonObject(jsonString)

    val schema = json.getJsonObject("schema")
    val rows = json.getJsonArray("rows")
    val keys = json.getJsonObject("keys")
    
    return JsonObject()
        .put("tableSchema", schema)
        .put("tableRows", rows)
        .put("tableKeys", keys)
}

fun parseTableRows(rows: JsonArray, schema: JsonObject) = rows.map {
    Tuple.from((it as JsonArray).mapIndexed { index, any -> convert(index, any, schema.map.values) })
}

private fun detectFormat(value: String, oldFormat: String?): String = when {
    value.toBooleanStrictOrNull() != null -> "BOOLEAN"
    value.toBigIntegerOrNull() != null -> "INTEGER"
    value.toDoubleOrNull() != null -> "NUMERIC"
    isDate(value) -> "DATE"
    else -> "TEXT"
}

fun isDate(value: String) = datePattern(value).isNotBlank() || normalizeDateTime(value) != null

val pattern1 = Regex("""^(0[1-9]|[12][0-9]|3[01])\.(0[1-9]|1[012])\.(19|20|21)\d\d$""")
val pattern2 = Regex("""^(0[1-9]|[12][0-9]|3[01])\.(0[1-9]|1[012])\.\d\d$""")
val pattern3 = Regex("""^(19|20|21)\d\d-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$""")

fun datePattern(value: String) = when {
    value.matches(pattern1) -> "dd.MM.yyyy"
    value.matches(pattern2) -> "dd.MM.yy"
    value.matches(pattern3) -> "yyyy-MM-dd"
    else -> ""
}

private fun convert(index: Int, any: Any, formats: Collection<Any>): Any? {
    return when (formats.map { it.toString() }[index]) {
        "DATE" -> if (any is String && any.isNotBlank()) {
            try {
                LocalDate.ofInstant(Instant.parse(any), ZoneId.systemDefault())
            } catch (e: Exception) {
                null
            }
        } else {
            null
        }
        "INTEGER" -> when {
            any is Int -> any
            any is BigInteger -> any
            any is String && any.isNotBlank() -> any.toBigIntegerOrNull()
            else -> null
        }
        "NUMERIC" -> when {
            any is Double -> any
            any is Int -> any
            any is BigInteger -> any
            any is String && any.isNotBlank() -> any.toDoubleOrNull()
            else -> null
        }
        "BOOLEAN" -> when {
            any is Boolean -> any
            any is String && any.isNotBlank() -> any.toBooleanStrictOrNull()
            else -> null
        }
        "TEXT" -> when {
            any is String && any.isNotBlank() -> any
            any is String && any.isBlank() -> null
            else -> any.toString()
        }
        else -> null
    }
}
