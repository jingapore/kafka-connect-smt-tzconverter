package tzconverter

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.Schema
import org.apache.kafka.connect.data.SchemaBuilder
import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements.requireStruct
import org.apache.kafka.connect.transforms.util.SchemaUtil
import org.apache.kafka.connect.transforms.util.SimpleConfig
import org.apache.kafka.common.cache.Cache
import org.apache.kafka.common.cache.LRUCache
import org.apache.kafka.common.cache.SynchronizedCache
import org.apache.kafka.connect.data.Field
import java.text.SimpleDateFormat
import java.time.ZoneId
import java.util.Date
import java.util.TimeZone

open class TzConverter<R : ConnectRecord<R>>(private val which: Which) : Transformation<R> {
    enum class Which { KEY, VALUE }

    private var tx: ((R) -> R)? = null

    class Key<R : ConnectRecord<R>> : TzConverter<R>(Which.KEY)
    class Value<R : ConnectRecord<R>> : TzConverter<R>(Which.VALUE)

    override fun apply(record: R): R =
        tx?.invoke(record) ?: throw IllegalStateException("TzConverter not configured")

    /*
        private lateinit var targetTz: ZoneId
        private lateinit var targetType: TimestampTargetType
        private lateinit var fieldToTransform: String
    */

    // we could let the user define this in config, but for simplicity we remove this flexibility from the user
    // later in `configure`, we set timezone based on user input
    /*    private val targetTimestampWithTzFormat: SimpleDateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX")*/

    // why do we want a cache? first, this is a mapping between the original schema and the desired schema.
    // without this cache, for every record, we'll be checking all record fields for a match with the target field
    // and converting the schema accordingly. with this cache, we can skip such checks.
    private lateinit var schemaUpdateCache: Cache<Schema, Schema>

    /*    protected abstract fun operatingSchema(record: R): Schema
        protected abstract fun operatingValue(record: R): Any
        protected abstract fun newRecord(record: R, updatedSchema: Schema, updatedValue: Any): R*/

    /*    private interface TimezoneTranslator {
            fun typeSchema(isOptional: Boolean): Schema
            fun toType(originalVal: Date): Object
        }*/

    /*    private enum class TimestampTargetType(val wireName: String, val translator: TimezoneTranslator) {
            STRING("string", object : TimezoneTranslator {
                override fun typeSchema(isOptional: Boolean): Schema {
                    return SchemaBuilder.string()
                }

                override fun toType(originalVal: Date): String {
                    return "a"
                }
            }),
            DATE("date");

            companion object {
                fun fromWireName(name: String): TimestampTargetType =
                    entries.firstOrNull { it.wireName == name }
                        ?: throw ConfigException("Unsupported timestamp target type: $name")
            }
        }

        companion object {
            const val FIELD_TO_TRANSFORM_FIELDNAME = "field"
            const val TARGET_TIMEZONE_FIELDNAME = "target.tz"
            const val CACHE_SIZE: Int = 16

            // string is an ISO format that contains timezone. we cannot use Timestamp (https://kafka.apache.org/11/javadoc/org/apache/kafka/connect/data/Timestamp.html)
            // because Timestamp doesn't have any representation for timezone.
            const val TARGET_TYPE_STRING = "string";

            // https://kafka.apache.org/11/javadoc/org/apache/kafka/connect/data/Date.html
            const val TARGET_TYPE_DATE = "Date";

            // https://kafka.apache.org/11/javadoc/org/apache/kafka/connect/data/Time.html
            const val TARGET_TYPE_TIME = "Time";
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

            private val TRANSLATORS: Map<TimestampTargetType, TimezoneTranslator> = buildMap {
                put(TimestampTargetType.STRING)
            }
        }

        //TODO: handle key

        class Value<R : ConnectRecord<R>> : TzConverter<R>() {
            override fun operatingSchema(record: R): Schema {
                return record.valueSchema()
            }

            override fun operatingValue(record: R): Any {
                return record.value()
            }

            override fun newRecord(record: R, updatedSchema: Schema, updatedValue: Any): R {
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
        }*/

    override fun config(): ConfigDef {
        return CONFIG_DEF
    }

    // `configure` is called to initialise configs passed in to CONFIG_DEF
    // this begs the qn: what is config() for?
    override fun configure(configs: Map<String?, *>) {
        val simpleConfig: SimpleConfig = SimpleConfig(CONFIG_DEF, configs)
        Config(
            fieldToTransform = simpleConfig.getString(FIELD_TO_TRANSFORM_FIELDNAME),
            targetTz = ZoneId.of(simpleConfig.getString(TARGET_TIMEZONE_FIELDNAME)),
            targetType = TimestampTargetType.valueOf(simpleConfig.getString(TARGET_TYPE_FIELDNAME)),
            targetTzFormat = SimpleDateFormat(simpleConfig.getString(TARGET_TIMEZONE_FORMAT_FIELDNAME))
        )
        schemaUpdateCache = SynchronizedCache(LRUCache(CACHE_SIZE));
    }

    override fun close() {}

    /*    private fun getOrBuildUpdatedSchema(schema: Schema): Schema =
            schemaUpdateCache.get(schema) ?: buildUpdatedSchema(schema).also {
                schemaUpdateCache.put(schema, it)
            }

        private fun buildUpdatedSchema(schema: Schema): Schema {
            val builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct())

            schema.fields().forEach { field ->
                if (field.name().equals(fieldToTransform)) {
                    builder.field(field.name(), TRANSLATORS[targetType] ?: . typeSchema (field.schema().isOptional))

                } else {
                    builder.field(field.name(), field.schema())
                }

            }

            schema.defaultValue()?.let { default ->
                val updatedDefaultValue = applyValueWithSchema(default as Struct, builder)
                builder.defaultValue(updatedDefaultValue)
            }
            return builder.build()
        }

        private fun applyWithSchema(record: R): R {
            // stock timestampconvertor checks if config.field is empty
            // is there a reason to do so? maybe it will be more apparent when
            // we extend this transformation to key (not just value)
            val schema: Schema = operatingSchema(record)
            val value: Struct = requireStruct(operatingValue(record), PURPOSE)

            val updatedSchema = getOrBuildUpdatedSchema(schema)
            val updatedValue = applyValueWithSchema(value, updatedSchema)

            return newRecord(record, updatedSchema, updatedValue)
        }*/

}