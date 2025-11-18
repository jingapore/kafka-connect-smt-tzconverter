package tzconverter

import org.apache.kafka.connect.source.SourceRecord


class TzConverterTests {

    private val xformKey: TzConverter<SourceRecord> = TzConverter.Key()
    private val xformValue: TzConverter<SourceRecord> = TzConverter.Value()

    companion object {}
}