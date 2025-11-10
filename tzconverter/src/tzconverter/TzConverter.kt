package tzconverter

import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.transforms.util.SimpleConfig

abstract class TzConverter<R : ConnectRecord<R>> : Transformation<R> {
    private lateinit var targetTz: ZoneId

    protected abstract fun operatingSchema(record: R): Schema
    protected abstract fun operatingValue(record: R): Any
    protected abstract fun newRecord(record: R, updatedSchema: Schema, updatedValue: Any)

    companion object {
        const val FIELD_TO_TRANSFORM_FIELDNAME = "field"
        const val TARGET_TIMEZONE_FIELDNAME = "target.tz"
        val CONFIG_DEF: ConfigDef = ConfigDef().apply {
            define(
                FIELD_TO_TRANSFORM_FIELDNAME,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                "This field contains the timestamp that we want to convert to another timezone"
            )
            define(
                TARGET_TIMEZONE_FIELDNAME,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                { _, value ->
                    val tz = value as? String ?: throw ConfigException("Timezone must be a string")
                    try {
                        ZoneId.of(tz)
                    } catch (e: Exception) {
                        throw ConfigException("Invalid timezone; '$tz'")
                    }
                },
                ConfigDef.Importance.HIGH, "Target timezone"
            )
        }
    }

    class Value<R : ConnectRecord<R>> : TzConverter<R>() {
        protected override fun operatingSchema(record: R): Schema {
            return record.valueSchema()
        }

        protected override fun operatingValue(record: R): Any {
            return record.value()
        }

        protected override fun newRecord(record: R, updatedSchema: Schema, updatedValue: Any): R {
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                updatedSchema,
                updatedValue,
                record.timestamp()
            )
        }
    }

    override fun apply(record: R): R {
        return applyWithSchema(record)
    }

    override fun config(): ConfigDef {
        return CONFIG_DEF
    }

    // `configure` is called to initialise configs passed in to CONFIG_DEF
    // this begs the qn: what is config() for?
    override fun configure(configs: Map<String?, *>) {
        val simpleConfig: SimpleConfig = SimpleConfig(CONFIG_DEF, configs)
        targetTz = simpleConfig.getString(TARGET_TIMEZONE_FIELDNAME)
    }

    override fun close() {}

}