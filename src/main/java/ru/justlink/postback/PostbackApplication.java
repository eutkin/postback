package ru.justlink.postback;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_FOUND;
import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

import java.sql.ResultSet;
import java.sql.SQLException;
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

  private final RowMapper<List<String>> rowMapper;

  public PostbackApplication(
    JdbcOperations jdbcOperations, RowMapper<List<String>> rowMapper) {
    this.jdbcOperations = jdbcOperations;
    this.rowMapper = rowMapper;
  }

  public static void main(String[] args) {
    SpringApplication.run(PostbackApplication.class, args);
  }

  @RequestMapping(value = "/{source}", method = {GET, POST})
  public ResponseEntity consume(@PathVariable String source,
    @RequestParam Map<String, String> parameters) {
    List<String> parameterNames = jdbcOperations
      .queryForObject("select * from mapping where source = ?", rowMapper, source);
    assert parameterNames != null;
    if (!parameters.keySet().containsAll(parameterNames)) {
      parameterNames.removeAll(parameters.keySet());
      String message = "Not enough parameters: " + String.join(", ", parameterNames);
      return ResponseEntity.badRequest().body(message);
    }
    List<String> parameterValues = parameterNames.stream().map(parameters::get).collect(toList());
    parameterValues.add(source);
    Object[] arg = parameterValues.toArray();
    jdbcOperations
      .update("insert into postback(user_id, code, aim, source) values (?, ? , ?, ?)", arg);
    return ResponseEntity.ok().build();
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

  @Component
  static class Mapper implements RowMapper<List<String>> {

    @Override
    public List<String> mapRow(ResultSet rs, int rowNum) throws SQLException {
      return asList(
        rs.getString("user_id"),
        rs.getString("code"),
        rs.getString("aim")
      );
    }
  }

}
