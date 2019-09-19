package ru.justlink.postback;

import static java.util.stream.Collectors.toList;
import static org.springframework.http.HttpStatus.BAD_REQUEST;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;
import static ru.justlink.postback.PostbackApplication.Parameter.AIM;
import static ru.justlink.postback.PostbackApplication.Parameter.CODE;
import static ru.justlink.postback.PostbackApplication.Parameter.USER_ID;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class PostbackApplication {

  private static final Logger log = LoggerFactory.getLogger(PostbackApplication.class);

  private final JdbcOperations jdbcOperations;

  private final RowMapper<Map<Parameter, String>> rowMapper;

  public PostbackApplication(
    JdbcOperations jdbcOperations, RowMapper<Map<Parameter, String>> rowMapper) {
    this.jdbcOperations = jdbcOperations;
    this.rowMapper = rowMapper;
  }

  public static void main(String[] args) {
    SpringApplication.run(PostbackApplication.class, args);
  }

  @RequestMapping(value = "/{source}", method = {GET, POST})
  public ResponseEntity consume(
    @PathVariable String source,
    @RequestParam Map<String, String> parameters
  ) {
    // example CODE -> click_id, left -> our standard values, right -> parameters name by client
    Map<Parameter, String> mappingParameters = extractParameterNames(source);

    if (!parameters.keySet().containsAll(mappingParameters.values())) {
      Collection<String> values = new ArrayList<>(mappingParameters.values());
      values.removeAll(parameters.keySet());
      String message = "Not enough parameters: " + String.join(", ", values);
      return ResponseEntity.badRequest().body(message);
    }
    List<Object> parameterValues = computeParameterValues(parameters, mappingParameters);
    savePostbackRecord(source, parameterValues);
    return ResponseEntity.ok().build();
  }

  private void savePostbackRecord(
    @PathVariable String source,
    List<Object> parameterValues) {
    parameterValues.add(source);
    Object[] arg = parameterValues.toArray();
    jdbcOperations
      .update("insert into postback(user_id, code, aim, source) values (?, ? , ?, ?)", arg);
  }

  private List<Object> computeParameterValues(
    Map<String, String> parameters,
    Map<Parameter, String> parameterNames
  ) {
    return parameterNames.entrySet()
      .stream()
      .map(entry -> {
        Parameter parameterType = entry.getKey();
        String parameterName = entry.getValue();
        String parameterValue = parameters.get(parameterName);
        switch (parameterType) {
          case AIM:
            switch (parameterValue) {
              case "reg" : return 0;
              case "dep" : return 1;
              case "dep_without_reg" : return 2;
              default: throw new IllegalArgumentException("Unknown value for parameter <goal>");
            }
          default: return parameterValue;
        }
      }).collect(toList());
  }

  @NonNull
  private Map<Parameter, String> extractParameterNames(String source) {
    return jdbcOperations
      .queryForObject("select * from mapping where source = ?", rowMapper, source);
  }

  @ExceptionHandler(IncorrectResultSizeDataAccessException.class)
  @ResponseStatus(NOT_FOUND)
  public void handler() {
  }

  @ExceptionHandler(DataAccessException.class)
  @ResponseStatus(INTERNAL_SERVER_ERROR)
  public void handler(DataAccessException ex) {
    log.error(ex.getMessage(), ex);
  }

  @ExceptionHandler(IllegalArgumentException.class)
  @ResponseStatus(BAD_REQUEST)
  public String handler(IllegalArgumentException ex) {
    return ex.getMessage();
  }

  enum Parameter {
    USER_ID,
    CODE,
    AIM
  }

  @Component
  static class Mapper implements RowMapper<Map<Parameter, String>> {

    @Override
    public Map<Parameter, String> mapRow(ResultSet rs, int rowNum) throws SQLException {
      return Map.of(
        USER_ID, rs.getString("user_id"),
        CODE, rs.getString("code"),
        AIM, rs.getString("aim")
      );
    }
  }

}
