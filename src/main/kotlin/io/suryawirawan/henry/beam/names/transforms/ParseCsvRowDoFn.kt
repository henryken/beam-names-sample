package io.suryawirawan.henry.beam.names.transforms

import de.siegmar.fastcsv.reader.CsvReader
import de.siegmar.fastcsv.reader.CsvRow
import io.suryawirawan.henry.beam.names.model.Person
import org.apache.beam.sdk.transforms.DoFn
import org.slf4j.LoggerFactory

import java.io.IOException
import java.io.StringReader

class ParseCsvRowDoFn : DoFn<String, Person>() {

    @ProcessElement
    fun processElement(context: ProcessContext) {
        val csvRow = context.element()
        val csvReader = CsvReader()

        try {
            csvReader.parse(StringReader(csvRow)).use { csvParser ->
                val row = csvParser.nextRow()
                val person = createPerson(row)

                context.output(person)
            }
        } catch (e: IOException) {
            LOGGER.warn("Invalid CSV row: $csvRow", e)
        }

    }

    private fun createPerson(row: CsvRow): Person {
        return Person(
                row.getField(0),
                row.getField(1),
                row.getField(2),
                row.getField(3),
                row.getField(4),
                row.getField(5),
                row.getField(6),
                row.getField(7),
                row.getField(8),
                row.getField(9),
                row.getField(10),
                row.getField(11)
        )
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(ParseCsvRowDoFn::class.java)
    }

}
