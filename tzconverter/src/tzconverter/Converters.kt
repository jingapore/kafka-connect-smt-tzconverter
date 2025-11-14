package tzconverter

import java.util.concurrent.ConcurrentHashMap
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Timestamp
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.errors.DataException
import java.util.Date as JDate
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.data.SchemaBuilder
import java.text.SimpleDateFormat
import java.util.TimeZone

internal fun <R : ConnectRecord<R>> valueTzConverter(cfg: Config): (R) -> R =
    recordTransformer(cfg, Lens.Value)

internal fun <R : ConnectRecord<R>> keyTzConverter(cfg: Config): (R) -> R = recordTransformer(cfg, Lens.Key)

private fun <R : ConnectRecord<R>> recordTransformer(cfg: Config, lens: Lens): (R) -> R {
    // why do we want a cache? first, this is a mapping between the original schema and the desired schema.
    // without this cache, for every record, we'll be checking all record fields for a match with the target field
    // and converting the schema accordingly. with this cache, we can skip such checks.
    val schemaCache = ConcurrentHashMap<Schema, Schema>()
    return { record ->
        val schema = lens.getSchema(record)
        if (schema == null) throw DataException("Cannot apply without schema on ${lens.javaClass.simpleName.lowercase()} for topic='${record.topic()}'")
        val value = lens.getValue(record)
        if (value == null) {
            record
        } else {
            val (newSchema, newValue) =
                if (cfg.fieldToTransform.isNullOrBlank()) {
                    if (schema.name() != Timestamp.LOGICAL_NAME) throw DataException(
                        "Can only apply values with type ${Date.LOGICAL_NAME} but type is ${schema.name()} "
                                + "while being transformed by ${lens.javaClass.simpleName.lowercase()}"
                    )
                    val dateValue = value as? JDate ?: throw DataException(
                        "Expected ${lens.javaClass.simpleName.lowercase()} value to be java.util.Date but was ${value::class}"
                    )
                    schema to convertOne(dateValue, cfg)
                } else {
                    val struct = requireStructOrNull(value)
                    val updatedSchema = schemaCache.computeIfAbsent(schema) {
                        buildUpdatedSchema(it, cfg)
                    }
                    val updatedStruct = Struct(updatedSchema)

                    for (field in schema.fields()) {
                        val fieldName = field.name()
                        val originalFieldValue = struct.get(fieldName)

                        if (fieldName == cfg.fieldToTransform) {
                            if (originalFieldValue == null) {
                                updatedStruct.put(fieldName, null)
                            } else {
                                // For field-based transform we *prefer* but do not strictly require
                                // the field to be a logical Date; if you want to enforce it:
                                val dateValue = originalFieldValue as? JDate
                                    ?: throw DataException(
                                        "Field '$fieldName' is configured for tz conversion but " +
                                                "is not a java.util.Date (was ${originalFieldValue::class})"
                                    )
                                updatedStruct.put(fieldName, convertOne(dateValue, cfg))
                            }
                        } else {
                            updatedStruct.put(fieldName, originalFieldValue)
                        }
                    }
                    updatedSchema to updatedStruct
                }
            lens.createNewRecord(record, newSchema, newValue)
        }


    }
}

private sealed interface Lens {
    fun <R : ConnectRecord<R>> getSchema(r: R): Schema?
    fun <R : ConnectRecord<R>> getValue(r: R): Any?
    fun <R : ConnectRecord<R>> createNewRecord(r: R, schema: Schema?, value: Any?): R

    data object Key : Lens {
        override fun <R : ConnectRecord<R>> getValue(r: R): Any? = r.key()
        override fun <R : ConnectRecord<R>> getSchema(r: R): Schema? = r.keySchema()
        override fun <R : ConnectRecord<R>> createNewRecord(r: R, schema: Schema?, value: Any?): R {
            TODO("Not yet implemented")
        }
    }

    data object Value : Lens {
        override fun <R : ConnectRecord<R>> getValue(r: R): Any? = r.value()
        override fun <R : ConnectRecord<R>> getSchema(r: R): Schema? = r.valueSchema()
        override fun <R : ConnectRecord<R>> createNewRecord(r: R, schema: Schema?, value: Any?): R {
            TODO("Not yet implemented")
        }
    }
}

private fun convertOne(sourceValue: JDate, cfg: Config): Any? =
    when (cfg.targetType) {
        TimestampTargetType.STRING -> {
            val fmt = (cfg.targetTzFormat.clone() as SimpleDateFormat).apply {
                timeZone = TimeZone.getTimeZone(cfg.targetTz)
            }
            fmt.format(sourceValue)
        }

        TimestampTargetType.DATE -> {

            val targetLocalDate = sourceValue.toInstant()
                .atZone(cfg.targetTz)
                .toLocalDate()

            val utcMidnightInstant = targetLocalDate
                .atStartOfDay(ZoneOffset.UTC)
                .toInstant()
            JDate.from(utcMidnightInstant)
        }
    }


private fun requireStructOrNull(value: Any?): Struct {
    return when (value) {
        null -> throw DataException("Expected Struct but value was null")
        is Struct -> value
        else -> throw DataException("Expected Struct but value was ${value::class}")
    }
}

private fun buildUpdatedSchema(original: Schema, cfg: Config): Schema {
    if (original.type() != Schema.Type.STRUCT) {
        throw DataException(
            "tzconverter field-based transform requires Struct schema, but was ${original.type()}"
        )
    }

    val fieldNameToChange = cfg.fieldToTransform
        ?: throw DataException("fieldToTransform must not be null/blank for field-based transform")

    val builder = SchemaBuilder.struct()
        .name(original.name())
        .version(original.version())
        .doc(original.doc())

    for (field in original.fields()) {
        val fieldSchema =
            if (field.name() == fieldNameToChange) {
                when (cfg.targetType) {
                    TimestampTargetType.STRING -> SchemaBuilder.string().optional().build()
                    TimestampTargetType.DATE -> Date.builder().optional().build()
                }
            } else {
                field.schema()
            }
        builder.field(field.name(), fieldSchema)
    }

    return builder.build()
}