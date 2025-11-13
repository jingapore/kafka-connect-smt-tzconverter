package tzconverter

import java.util.concurrent.ConcurrentHashMap
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Date
import org.apache.kafka.connect.data.Schema

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
        val converted: Pair<Schema?, Any?> =
            if (schema == null) {
                null to null
            } else {
                null to null
            }
        lens.createNewRecord(record, converted.first, converted.second)
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

private fun convertOne(sourceValue: Date, cfg: Config): Any? {
    when (cfg.targetType) {
        TimestampTargetType.STRING -> TODO()
        TimestampTargetType.DATE -> TODO()
    }
}