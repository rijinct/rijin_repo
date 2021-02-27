
package com.project.rithomas.jobexecution.common.util;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.project.simpledom.Element;
import com.rijin.project.sqlparser.DOMConstants;
import com.rijin.project.sqlparser.SQL2DOMConverter;
import com.rijin.project.sqltree.ExprFacade;
import com.rijin.project.sqltree.FromClause;
import com.rijin.project.sqltree.Join;
import com.rijin.project.sqltree.JoinClause;
import com.rijin.project.sqltree.SQLBuilder;
import com.rijin.project.sqltree.SelectStatement;
import com.rijin.project.sqltree.Statement;
import com.rijin.project.sqltree.Table;
import com.rijin.project.sqltree.TableOrSubquery;
import com.rijin.project.sqltree.WhereClause;
import com.rijin.project.sqltree.expr.BinaryOperatorExpr;
import com.rijin.project.sqltree.expr.Expr;
import com.rijin.project.sqltree.expr.HashExpr;
import com.rijin.project.sqltree.expr.LiteralExpr;
import com.rijin.project.sqltree.expr.LogicalExpr;
import com.rijin.project.sqltree.expr.SuffixHashExpr;

public class ReaggQueryGenerator {

	private static final String NULL_STRING_REPLACEMENT = "NULL_STRING";

	private static final String NULL_STRING = "\"\"null\"\"";

	private static final String DISTRIBUTE = "DISTRIBUTE";

	private static final String SELECT = "SELECT";

	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(ReaggQueryGenerator.class);

	public ReaggQueryGenerator() {
	}

	public String generateReaggQuery(String aggregationQuery) {
		String aggregationQueryUpperCase = aggregationQuery.toUpperCase();
		String insertPart = "";
		String distributePart = "";
		String selectQuery = null;
		insertPart = StringUtils.substring(aggregationQuery, 0,
				StringUtils.indexOf(aggregationQueryUpperCase, SELECT));
		if (aggregationQueryUpperCase.contains(DISTRIBUTE)) {
			selectQuery = StringUtils.substring(aggregationQuery,
					StringUtils.indexOf(aggregationQueryUpperCase, SELECT),
					StringUtils.indexOf(aggregationQueryUpperCase, DISTRIBUTE));
			distributePart = StringUtils.substring(aggregationQuery,
					StringUtils.indexOf(aggregationQueryUpperCase, DISTRIBUTE));
		} else {
			selectQuery = StringUtils.substring(aggregationQuery,
					StringUtils.indexOf(aggregationQueryUpperCase, SELECT));
		}
		selectQuery = preQueryUpdates(selectQuery);
		Element element = new SQL2DOMConverter(selectQuery).getRoot();
		Statement stmt = new Statement(element);
		updateQuery(element);
		selectQuery = postQueryUpdates(stmt.toSQLString());
		LOGGER.debug("insert_sql: {} \n select_sql: {} \n  distibute_sql: {}",
				insertPart, selectQuery, distributePart);
		return String.format("%s %s %s", insertPart, selectQuery,
				distributePart);
	}

	private String postQueryUpdates(String selectQuery) {
		selectQuery = updateHashPlaceHoldersWithHashSufixRemoved(selectQuery);
		selectQuery = StringUtils.replace(selectQuery,
				NULL_STRING_REPLACEMENT, NULL_STRING);
		return selectQuery;
	}

	private String preQueryUpdates(String selectQuery) {
		selectQuery = updateHashPlaceHoldersWithHashSuffix(selectQuery);
		selectQuery = StringUtils.replace(selectQuery, NULL_STRING,
				NULL_STRING_REPLACEMENT);
		return selectQuery;
	}

	private String updateHashPlaceHoldersWithHashSufixRemoved(String sql) {
		Set<String> matchSet = new HashSet<String>();
		while (matcher.find()) {
		}
		return sql;
	}

	private void updateQuery(Element element) {
		for (Element elem : element.getChildNodes()) {
			if (elem.getNodeName().equals("select_statement")) {
				if (elem.hasAttribute(
						DOMConstants.SELECT_STATEMENT_TYPE.toString())) {
					for (Element childElement : element.getChildNodes()) {
						updateQuery(childElement);
					}
				} else if (elem.getChildNodes("FROM") != null) {
					updateSelectQuery(new SelectStatement(elem));
				}
			} else if (elem.getNodeName().equals("FROM")) {
				updateSelectQuery(new SelectStatement(element));
			}
		}
	}

	private void updateSelectQuery(SelectStatement stat) {
		FromClause from = stat.getFromClause();
		if (from.hasJoinClause()) {
			JoinClause joinClause = from.getJoinClause();
			handleTableOrSubquery(stat, joinClause.getTableOrSubquery());
			handleJoins(stat, joinClause.getJoins());
		} else {
			TableOrSubquery tableOrSubquery = from.getTableOrSubquery();
			handleTableOrSubquery(stat, tableOrSubquery);
		}
	}

	private void handleJoins(SelectStatement stat, List<Join> joins) {
		for (Join join : joins) {
			handleTableOrSubquery(stat, join.getTableOrSubquery());
		}
	}

	private void handleTableOrSubquery(SelectStatement stat,
			TableOrSubquery tableOrSubquery) {
		if (tableOrSubquery.containsTable() && stat.containsWhereClause()) {
			updateTableNameAndWhereClause(stat, tableOrSubquery);
		} else if (tableOrSubquery.containsInnerSelect()) {
			updateQuery(tableOrSubquery.getSubquery().getElement());
		}
	}

	private void updateTableNameAndWhereClause(SelectStatement stat,
			TableOrSubquery tableOrSubquery) {
		String tableName = updateTableName(tableOrSubquery);
		WhereClause where = stat.getWhereClause();
		for (Expr exp : splitOnOperand(where.getExpr())) {
			if (exp instanceof BinaryOperatorExpr) {
				updateBoundaryExpressions(tableName, exp);
			} else if (exp instanceof HashExpr) {
				updateReportTimeCondition(tableName, exp);
			}
		}
	}

	public List<Expr> splitOnOperand(Expr expr) {
		List<Expr> list = new ArrayList<>();
		splitOnOperand(expr, list);
		return list;
	}

	private void splitOnOperand(Expr expr, List<Expr> outputList) {
		if (expr instanceof LogicalExpr) {
			splitOnOperand(expr.getOperand(1), outputList);
			splitOnOperand(expr.getOperand(2), outputList);
		} else if (expr instanceof SuffixHashExpr) {
			splitOnOperand(expr.getOperand(1), outputList);
			splitOnOperand(expr.getOperand(2), outputList);
		} else {
			outputList.add(expr);
		}
	}

	private void updateReportTimeCondition(String tableName, Expr exp) {
		ExprFacade hash = new ExprFacade(exp);
		for (HashExpr ex : hash.getHashExpressions()) {
			addReportCondition(tableName, ex);
		}
	}

	private void addReportCondition(String tableName, Expr ex) {
					.getElement());
		}
	}

	private void updateBoundaryExpressions(String tableName, Expr exp) {
		ExprFacade binary = new ExprFacade(exp);
		for (LiteralExpr ex : binary.getLiteralExpressions()) {
			String literalValue = ex.getLiteralValue().toSQLString();
			LOGGER.debug("Literral Value: {}", literalValue);
						.getElement());
						.getElement());
			}
		}
	}

	private String updateTableName(TableOrSubquery tableOrSubquery) {
		Table table = tableOrSubquery.getTable();
		String tableName = table.getName();
		String tableAlias = table.getAlias().isEmpty() ? tableName
				: table.getAlias();
							tableAlias)
					.getElement());
		}
		return tableName.toUpperCase();
	}
}
