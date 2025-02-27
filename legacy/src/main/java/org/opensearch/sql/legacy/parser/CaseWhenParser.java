/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.parser;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLCaseExpr;
import com.alibaba.druid.sql.ast.expr.SQLNullExpr;
import com.google.common.base.Joiner;
import java.util.ArrayList;
import java.util.List;
import org.opensearch.sql.legacy.domain.Condition;
import org.opensearch.sql.legacy.domain.Where;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.utils.Util;

/** Created by allwefantasy on 9/3/16. */
public class CaseWhenParser {

  private final SQLCaseExpr caseExpr;
  private final String alias;
  private final String tableAlias;

  public CaseWhenParser(SQLCaseExpr caseExpr, String alias, String tableAlias) {
    this.alias = alias;
    this.tableAlias = tableAlias;
    this.caseExpr = caseExpr;
  }

  public String parse() throws SqlParseException {
    List<String> result = new ArrayList<>();

    if (caseExpr.getValueExpr() != null) {
      for (SQLCaseExpr.Item item : caseExpr.getItems()) {
        SQLExpr left = caseExpr.getValueExpr();
        SQLExpr right = item.getConditionExpr();
        SQLBinaryOpExpr conditionExpr =
            new SQLBinaryOpExpr(left, SQLBinaryOperator.Equality, right);
        item.setConditionExpr(conditionExpr);
      }
      caseExpr.setValueExpr(null);
    }

    for (SQLCaseExpr.Item item : caseExpr.getItems()) {
      SQLExpr conditionExpr = item.getConditionExpr();

      WhereParser parser = new WhereParser(new SqlParser(), conditionExpr);
      String scriptCode = explain(parser.findWhere());
      if (scriptCode.startsWith(" &&")) {
        scriptCode = scriptCode.substring(3);
      }
      if (result.size() == 0) {
        result.add(
            "if("
                + scriptCode
                + ")"
                + "{"
                + Util.getScriptValueWithQuote(item.getValueExpr(), "'")
                + "}");
      } else {
        result.add(
            "else if("
                + scriptCode
                + ")"
                + "{"
                + Util.getScriptValueWithQuote(item.getValueExpr(), "'")
                + "}");
      }
    }
    SQLExpr elseExpr = caseExpr.getElseExpr();
    if (elseExpr == null) {
      result.add("else { null }");
    } else {
      result.add("else {" + Util.getScriptValueWithQuote(elseExpr, "'") + "}");
    }

    return Joiner.on(" ").join(result);
  }

  public String explain(Where where) throws SqlParseException {
    List<String> codes = new ArrayList<>();
    while (where.getWheres().size() == 1) {
      where = where.getWheres().getFirst();
    }
    explainWhere(codes, where);
    String relation = where.getConn().name().equals("AND") ? " && " : " || ";
    return Joiner.on(relation).join(codes);
  }

  private void explainWhere(List<String> codes, Where where) throws SqlParseException {
    if (where instanceof Condition) {
      Condition condition = (Condition) where;

      if (condition.getValue() instanceof ScriptFilter) {
        codes.add("(" + ((ScriptFilter) condition.getValue()).getScript() + ")");
      } else if (condition.getOPERATOR() == Condition.OPERATOR.BETWEEN) {
        Object[] objs = (Object[]) condition.getValue();
        codes.add(
            "("
                + "doc['"
                + condition.getName()
                + "'].value >= "
                + objs[0]
                + " && doc['"
                + condition.getName()
                + "'].value <="
                + objs[1]
                + ")");
      } else {
        SQLExpr nameExpr = condition.getNameExpr();
        SQLExpr valueExpr = condition.getValueExpr();
        if (valueExpr instanceof SQLNullExpr) {
          codes.add("(" + "doc['" + nameExpr.toString() + "']" + ".empty)");
        } else {
          codes.add(
              "("
                  + Util.getScriptValueWithQuote(nameExpr, "'")
                  + condition.getOpertatorSymbol()
                  + Util.getScriptValueWithQuote(valueExpr, "'")
                  + ")");
        }
      }
    } else {
      for (Where subWhere : where.getWheres()) {
        List<String> subCodes = new ArrayList<>();
        explainWhere(subCodes, subWhere);
        String relation = subWhere.getConn().name().equals("AND") ? "&&" : "||";
        codes.add(Joiner.on(relation).join(subCodes));
      }
    }
  }
}
