package io.suryawirawan.henry.beam.names.model

import java.io.Serializable

data class Person(
        val firstName: String,
        val lastName: String,
        val companyName: String,
        val address: String,
        val city: String,
        val county: String,
        val state: String,
        val zip: String,
        val phone1: String,
        val phone2: String,
        val email: String,
        val web: String
) : Serializable
