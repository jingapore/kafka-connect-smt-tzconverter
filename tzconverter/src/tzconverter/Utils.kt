package tzconverter

import java.text.SimpleDateFormat
import java.time.ZoneId
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigException

internal const val FIELD_TO_TRANSFORM_FIELDNAME = "field"
internal const val TARGET_TIMEZONE_FIELDNAME = "target.tz"
internal const val TARGET_TYPE_FIELDNAME = "target.type"
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
    define(
        TARGET_TYPE_FIELDNAME,
        ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Target type"
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

enum class TimestampTargetType {
    STRING,

    // IMPORTANT: note that DATE is not Date in Kafka Data type
    // In Kafka, Date is NOT a timestamp, it is zero-ed out on
    // everything except date. In Java, DATE is a timestamp.
    // Here's the relevant excerpt from Kafka docs.
    // https://kafka.apache.org/25/javadoc/org/apache/kafka/connect/data/Date.html
    // "A date representing a calendar day with no time of day or timezone.
    // The corresponding Java type is a java.util.Date with hours, minutes, seconds, milliseconds set to 0. The underlying representation is an integer representing the number of standardized days (based on a number of milliseconds with 24 hours/day, 60 minutes/hour, 60 seconds/minute, 1000 milliseconds/second with n) since Unix epoch."
    DATE,

    TIMESTAMP
}