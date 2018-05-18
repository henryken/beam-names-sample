package io.suryawirawan.henry.beam.names.transforms

import io.suryawirawan.henry.beam.names.model.Person
import org.apache.beam.sdk.transforms.DoFn
import org.slf4j.LoggerFactory

class PersonLoggingDoFn : DoFn<Person, Person>() {

    @ProcessElement
    fun processElement(context: ProcessContext) {
        LOGGER.info("Person: " + context.element().firstName)
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(PersonLoggingDoFn::class.java)
    }

}
