/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.protocol.response.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.common.utils.StringUtils.format;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_MISSING;
import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.protocol.response.format.CsvResponseFormatter.CONTENT_TYPE;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.protocol.response.QueryResult;

/** Unit test for {@link CsvResponseFormatter}. */
public class CsvResponseFormatterTest {
  private static final CsvResponseFormatter formatter = new CsvResponseFormatter();

  @Test
  void formatResponse() {
    ExecutionEngine.Schema schema =
        new ExecutionEngine.Schema(
            ImmutableList.of(
                new ExecutionEngine.Schema.Column("name", "name", STRING),
                new ExecutionEngine.Schema.Column("age", "age", INTEGER)));
    QueryResult response =
        new QueryResult(
            schema,
            Arrays.asList(
                tupleValue(ImmutableMap.of("name", "John", "age", 20)),
                tupleValue(ImmutableMap.of("name", "Smith", "age", 30))));
    CsvResponseFormatter formatter = new CsvResponseFormatter();
    String expected = "name,age%nJohn,20%nSmith,30";
    assertEquals(format(expected), formatter.format(response));
  }

  @Test
  void sanitizeHeaders() {
    ExecutionEngine.Schema schema =
        new ExecutionEngine.Schema(
            ImmutableList.of(
                new ExecutionEngine.Schema.Column("=firstname", null, STRING),
                new ExecutionEngine.Schema.Column("+lastname", null, STRING),
                new ExecutionEngine.Schema.Column("-city", null, STRING),
                new ExecutionEngine.Schema.Column("@age", null, INTEGER)));
    QueryResult response =
        new QueryResult(
            schema,
            Arrays.asList(
                tupleValue(
                    ImmutableMap.of(
                        "=firstname",
                        "John",
                        "+lastname",
                        "Smith",
                        "-city",
                        "Seattle",
                        "@age",
                        20))));
    String expected = "'=firstname,'+lastname,'-city,'@age%nJohn,Smith,Seattle,20";
    assertEquals(format(expected), formatter.format(response));
  }

  @Test
  void sanitizeData() {
    ExecutionEngine.Schema schema =
        new ExecutionEngine.Schema(
            ImmutableList.of(new ExecutionEngine.Schema.Column("city", "city", STRING)));
    QueryResult response =
        new QueryResult(
            schema,
            Arrays.asList(
                tupleValue(ImmutableMap.of("city", "Seattle")),
                tupleValue(ImmutableMap.of("city", "=Seattle")),
                tupleValue(ImmutableMap.of("city", "+Seattle")),
                tupleValue(ImmutableMap.of("city", "-Seattle")),
                tupleValue(ImmutableMap.of("city", "@Seattle")),
                tupleValue(ImmutableMap.of("city", "Seattle="))));
    String expected =
        "city%n"
            + "Seattle%n"
            + "'=Seattle%n"
            + "'+Seattle%n"
            + "'-Seattle%n"
            + "'@Seattle%n"
            + "Seattle=";
    assertEquals(format(expected), formatter.format(response));
  }

  @Test
  void quoteIfRequired() {
    ExecutionEngine.Schema schema =
        new ExecutionEngine.Schema(
            ImmutableList.of(
                new ExecutionEngine.Schema.Column("na,me", "na,me", STRING),
                new ExecutionEngine.Schema.Column(",,age", ",,age", INTEGER)));
    QueryResult response =
        new QueryResult(
            schema,
            Arrays.asList(
                tupleValue(ImmutableMap.of("na,me", "John,Smith", ",,age", "30,,,")),
                tupleValue(ImmutableMap.of("na,me", "Line\nBreak", ",,age", "28,,,")),
                tupleValue(ImmutableMap.of("na,me", "\"Janice Jones", ",,age", "26\""))));
    String expected =
        "\"na,me\",\",,age\"%n\"John,Smith\",\"30,,,\"%n\"Line\nBreak\",\"28,,,\"%n"
            + "\"\"\"Janice Jones\",\"26\"\"\"";
    assertEquals(format(expected), formatter.format(response));
  }

  @Test
  void formatError() {
    Throwable t = new RuntimeException("This is an exception");
    String expected =
        "{\n  \"type\": \"RuntimeException\",\n  \"reason\": \"This is an exception\"\n}";
    assertEquals(expected, formatter.format(t));
  }

  @Test
  void escapeSanitize() {
    CsvResponseFormatter escapeFormatter = new CsvResponseFormatter(false);
    ExecutionEngine.Schema schema =
        new ExecutionEngine.Schema(
            ImmutableList.of(new ExecutionEngine.Schema.Column("city", "city", STRING)));
    QueryResult response =
        new QueryResult(
            schema,
            Arrays.asList(
                tupleValue(ImmutableMap.of("city", "=Seattle")),
                tupleValue(ImmutableMap.of("city", ",,Seattle"))));
    String expected = "city%n=Seattle%n\",,Seattle\"";
    assertEquals(format(expected), escapeFormatter.format(response));
  }

  @Test
  void replaceNullValues() {
    ExecutionEngine.Schema schema =
        new ExecutionEngine.Schema(
            ImmutableList.of(
                new ExecutionEngine.Schema.Column("name", "name", STRING),
                new ExecutionEngine.Schema.Column("city", "city", STRING)));
    QueryResult response =
        new QueryResult(
            schema,
            Arrays.asList(
                tupleValue(ImmutableMap.of("name", "John", "city", "Seattle")),
                ExprTupleValue.fromExprValueMap(
                    ImmutableMap.of("firstname", LITERAL_NULL, "city", stringValue("Seattle"))),
                ExprTupleValue.fromExprValueMap(
                    ImmutableMap.of("firstname", stringValue("John"), "city", LITERAL_MISSING))));
    String expected = "name,city%nJohn,Seattle%n,Seattle%nJohn,";
    assertEquals(format(expected), formatter.format(response));
  }

  @Test
  void testContentType() {
    assertEquals(formatter.contentType(), CONTENT_TYPE);
  }
}
