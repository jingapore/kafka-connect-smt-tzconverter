package tzconverter

import java.text.SimpleDateFormat
import java.time.ZoneId
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException

internal const val FIELD_TO_TRANSFORM_FIELDNAME = "field"
internal const val TARGET_TIMEZONE_FIELDNAME = "target.tz"
internal const val TARGET_TYPE_FIELDNAME = "target.type"
internal const val TARGET_TIMEZONE_FORMAT_FIELDNAME = "target.tzformat"
internal const val CACHE_SIZE: Int = 16

// string is an ISO format that contains timezone. we cannot use Timestamp (https://kafka.apache.org/11/javadoc/org/apache/kafka/connect/data/Timestamp.html)
// because Timestamp doesn't have any representation for timezone.
internal const val TARGET_TYPE_STRING = "string";

// https://kafka.apache.org/11/javadoc/org/apache/kafka/connect/data/Date.html
internal const val TARGET_TYPE_DATE = "Date";

// https://kafka.apache.org/11/javadoc/org/apache/kafka/connect/data/Time.html
internal const val TARGET_TYPE_TIME = "Time";

internal val CONFIG_DEF: ConfigDef = ConfigDef().apply {
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

data class Config(
    val fieldToTransform: String?,
    // why do we use ZoneId instead of TimeZone (which we set as an attr for SimpleDateFormat)?
    // because ZoneId is more modern and TimeZone is legacy: https://stackoverflow.com/questions/79073807/whats-the-difference-between-timezone-and-zoneid
    val targetTz: ZoneId,
    val targetType: TimestampTargetType,
    val targetTzFormat: SimpleDateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX")
)

enum class TimestampTargetType { STRING, DATE }