package tzconverter

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.SimpleConfig

import java.time.ZoneId

open class TzConverter<R : ConnectRecord<R>>(private val which: Which) : Transformation<R> {
    enum class Which { KEY, VALUE }

    private var tx: ((R) -> R)? = null

    class Key<R : ConnectRecord<R>> : TzConverter<R>(which = Which.KEY)
    class Value<R : ConnectRecord<R>> : TzConverter<R>(which = Which.VALUE)

    override fun apply(record: R): R =
        tx?.invoke(record) ?: throw IllegalStateException("TzConverter not configured")


    override fun config(): ConfigDef {
        return CONFIG_DEF
    }

    // `configure` is called to initialise configs passed in to CONFIG_DEF
    // this begs the qn: what is config() for?
    override fun configure(configs: Map<String?, *>) {
        val simpleConfig = SimpleConfig(CONFIG_DEF, configs)
        val cfg = Config(
            fieldToTransform = simpleConfig.getString(FIELD_TO_TRANSFORM_FIELDNAME),
            targetTz = ZoneId.of(simpleConfig.getString(TARGET_TIMEZONE_FIELDNAME)),
            targetType = TimestampTargetType.valueOf(simpleConfig.getString(TARGET_TYPE_FIELDNAME).uppercase()),
        )
        tx = when(which) {
            Which.KEY -> keyTzConverter(cfg)
            Which.VALUE -> valueTzConverter(cfg)
        }
    }

    override fun close() {}

}