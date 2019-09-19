package ru.justlink.postback

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.dao.DataAccessException
import org.springframework.dao.IncorrectResultSizeDataAccessException
import org.springframework.http.HttpStatus.*
import org.springframework.jdbc.core.JdbcOperations
import org.springframework.jdbc.core.RowMapper
import org.springframework.web.bind.annotation.*

enum class Parameter {
    USER_ID, CODE, AIM
}

@SpringBootApplication
open class PostBackApplication(private val jdbc: JdbcOperations) {

    private val log : Logger = LoggerFactory.getLogger(PostBackApplication::class.java)

    private val rowMapper = RowMapper<Map<Parameter, String>> { rs, _ ->
        /* for example:
            USER_ID -> user
            CODE -> click_id
            AIM -> goal
         */
        Parameter.values().map { it to rs.getString(it.name) }.toMap()
    }

    @RequestMapping(path = ["/{source}"], method = [RequestMethod.POST, RequestMethod.GET])
    fun consume(@PathVariable source: String, @RequestParam parameters: Map<String, String>) {
        val parametersMapping: Map<Parameter, String> = jdbc
                .queryForObject("select * from mapping where source = ?", rowMapper, source)!!

        if (!parameters.keys.containsAll(parametersMapping.values)) {
            val missingParameters = parametersMapping.values.minus(parameters.keys).joinToString(", ")
            throw IllegalArgumentException("Not enough parameters: $missingParameters")
        }
        val arg = parametersMapping
                .map {
                    val (parameterType, parameterName) = it
                    val parameterValue = parameters.getValue(parameterName)
                    val value: Any = when (parameterType) {
                        Parameter.AIM -> mapAimParameterValue(parameterValue, parameterName)
                        else -> parameterValue
                    }
                    value
                }
                .plus(source)
                .toTypedArray()

        val columns = Parameter.values().joinToString { ", " }
        jdbc.update("insert into postback($columns, source) values (?, ?, ?, ?)", arg)

    }

    @ExceptionHandler(IncorrectResultSizeDataAccessException::class)
    @ResponseStatus(NOT_FOUND)
    fun handler() {
    }

    @ExceptionHandler(DataAccessException::class)
    @ResponseStatus(INTERNAL_SERVER_ERROR)
    fun handler(ex: DataAccessException) {
        log.error(ex.message, ex)
    }

    @ExceptionHandler(IllegalArgumentException::class)
    @ResponseStatus(BAD_REQUEST)
    fun handler(ex: IllegalArgumentException): String {
        return ex.message!!
    }

    private fun mapAimParameterValue(parameterValue: String, parameterName: String): Int {
        return when (parameterValue) {
            "reg" -> 0
            "dep" -> 1
            "dep_without_reg" -> 2
            else -> throw IllegalArgumentException("Unknown value for parameter <$parameterName>")
        }
    }
}


fun main(args: Array<String>) {
    runApplication<PostBackApplication>(*args)
}

