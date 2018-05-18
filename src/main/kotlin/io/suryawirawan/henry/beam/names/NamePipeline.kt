package io.suryawirawan.henry.beam.names

import io.suryawirawan.henry.beam.names.model.Person
import io.suryawirawan.henry.beam.names.transforms.ParseCsvRowDoFn
import io.suryawirawan.henry.beam.names.transforms.PersonLoggingDoFn
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.Filter
import org.apache.beam.sdk.transforms.Flatten
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.PCollectionList

object NamePipeline {

    @JvmStatic
    fun main(args: Array<String>) {
        val options = PipelineOptionsFactory.fromArgs(*args).create()
        val pipeline = Pipeline.create(options)

        val persons = pipeline
                .apply("Read Persons", TextIO.read().from("names.csv"))
                .apply("Parse CSV row", ParDo.of(ParseCsvRowDoFn()))

        val aPersons = persons
                .apply("Filter 'A' names",
                        Filter.by(firstNameStartingWith("A")))


        val bPersons = persons
                .apply("Filter 'B' names",
                        Filter.by(firstNameStartingWith("B")))


        PCollectionList.of(aPersons).and(bPersons)
                .apply("Flatten", Flatten.pCollections())
                .apply("Persons 'A' and 'B'", ParDo.of(PersonLoggingDoFn()))

        pipeline.run()
    }

    private fun firstNameStartingWith(prefix: String) = SerializableFunction<Person, Boolean> {
        person: Person -> person.firstName.startsWith(prefix)
    }

}
