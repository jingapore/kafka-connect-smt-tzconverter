package tzconverter

import org.apache.kafka.connect.components.Versioned
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.common.config.ConfigDef

abstract class TzConverter<R : ConnectRecord<R>> : Transformation<R>, Versioned {

    protected abstract fun operatingSchema(record: R): Schema
    protected abstract fun operatingValue(record: R): Any
    protected abstract fun newRecord(record: R, updatedSchema: Schema, updatedValue: Any)

    companion object {
        val FIELD_CONFIG = "field"
        val CONFIG_DEF: ConfigDef = ConfigDef().apply {
            define(
                FIELD_CONFIG,
                ConfigDef.Type.STRING,
                FIELD_DEFAULT,
                ConfigDef.Importance.HIGH,
                "This field contains the timestamp that we want to convert to another timezone"
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
}