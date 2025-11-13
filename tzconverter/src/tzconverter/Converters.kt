package tzconverter

import java.util.concurrent.ConcurrentHashMap
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema

internal fun <R : ConnectRecord<R>> valueTzConverter(cfg: Config): (R) -> R =
    recordTransformer(cfg, Lens.Value)

internal fun <R : ConnectRecord<R>> keyTzConverter(cfg: Config): (R) -> R = recordTransformer(cfg, Lens.Key)

private fun <R : ConnectRecord<R>> recordTransformer(cfg: Config, lens: Lens): (R) -> R {
    val schemaCache = ConcurrentHashMap<Schema, Schema>()
    return {record ->
        lens.createNewRecord(record, converted.first, converted.second)
    }
}

private sealed interface Lens {
    fun <R: ConnectRecord<R>> getSchema(r: R): Schema?
    fun <R: ConnectRecord<R>> getValue(r: R): Any?
    fun <R: ConnectRecord<R>> createNewRecord(r: R, schema: Schema?, value: Any?): R
    data object Key : Lens {
        override fun <R: ConnectRecord<R>> getValue(r: R): Any? = {TODO("asd")}
        override fun <R : ConnectRecord<R>> getSchema(r: R): Schema? {
            TODO("Not yet implemented")
        }
        override fun <R : ConnectRecord<R>> createNewRecord(r: R, schema: Schema?, value: Any?): R {
            TODO("Not yet implemented")
        }
    }
    data object Value: Lens {
        override fun <R: ConnectRecord<R>> getValue(r: R): Any? = {TODO("asd")}
        override fun <R : ConnectRecord<R>> getSchema(r: R): Schema? {
            TODO("Not yet implemented")
        }
        override fun <R : ConnectRecord<R>> createNewRecord(r: R, schema: Schema?, value: Any?): R {
            TODO("Not yet implemented")
        }
    }
}