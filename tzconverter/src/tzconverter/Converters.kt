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