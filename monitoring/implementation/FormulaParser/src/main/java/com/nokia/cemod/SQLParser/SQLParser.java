
package com.nokia.cemod.SQLParser;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import com.nokia.cemod.sqlparser.SQL2DOMConverter;
import com.nokia.cemod.sqltree.ExprFacade;
import com.nokia.cemod.sqltree.expr.Expr;
import com.nokia.cemod.sqltree.expr.ExprFactory;

public class SQLParser {

	public List<String> getColumnList(String formula)
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException,
			SecurityException {
		formula = formula.replaceAll("[{}]+", " ").replaceAll("[:]", "_")
				.replace("\\", "");
		Expr expr = ExprFactory
				.create(new SQL2DOMConverter(formula, "expr").getRoot());
		ExprFacade exprFacade = new ExprFacade(expr);
		List<String> columns = exprFacade.getReferencedColumnNames().stream()
				.distinct().collect(Collectors.toList());
		return columns;
	}

	public List<String> parserFormula(String formula)
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException,
			SecurityException {
		List<String> columns = null;
		formula = formula.replaceAll("&gt;", ">").replaceAll("''", "'");
		if (formula.contains("msisdn_normalize")) {
			columns = parseMsisdnFormula(formula);
		} else if (formula.contains("denorm:")) {
			columns = parseDenormFormula(formula);
		} else if (isUNIFormula(formula)) {
			if (formula.contains("floor")) {
				formula = formula.replaceAll(" ", "").replaceAll("[mM]ono", "")
						.replaceAll("[fF]ree", "");
			}
			columns = getColumnList(parseDimensionFormulaFromUNIFiles(formula));
		} else {
			columns = getColumnList(formula);
		}
		return columns;
	}

	private boolean isUNIFormula(String formula) {
		String regex = "\\{[a-zA-Z]+[\\\\]*";
		Matcher matcher = getMatcher(formula, regex);
		return matcher.find();
	}

	public List<String> parseMsisdnFormula(String formula)
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException,
			SecurityException {
		formula = formula.replace(",\"Y\"", "").replace("-", "")
				.replaceAll("[{}]+", " ").replace(':', '_');
		String[] sqlList = formula.substring(17, formula.length() - 1)
				.split(",");
		List<String> result = new ArrayList<String>();
		for (String formula_part : sqlList) {
			if (formula_part.contains("TO") || formula_part.contains("from")) {
				continue;
			}
			result.addAll(getColumnList(formula_part));
		}
		return result;
	}

	public List<String> parseDenormFormula(String formula)
			throws IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException,
			SecurityException {
		List<String> columns = getColumnList(formula);
		columns.remove(0);
		return columns;
	}

	private String parseDimensionFormulaFromUNIFiles(String formula) {
		String formula_subString[] = StringUtils.substringsBetween(formula, "{",
				"}");
		for (String substring : formula_subString) {
			formula = formula.replace(substring + "}",
					substring.replaceAll(" ", "_") + "}");
		}
		if (shouldReplaceUnderscore(formula)) {
			formula = formula.replaceAll("[-]", "_");
		}
		String regex = "\\{[a-zA-Z]+[\\\\]+";
		Matcher matcher = getMatcher(formula, regex);
		if (matcher.find()) {
			formula = matcher.replaceAll("(").replace('}', ')');
		} else {
			regex = "\\{([^$]+)_([^_$]+)[\\\\]+";
			matcher = getMatcher(formula, regex);
			formula = matcher.replaceAll("(").replace('}', ')');
		}
		return formula;
	}

	private boolean shouldReplaceUnderscore(String formula) {
		return !StringUtils.substringBefore(formula, "-").endsWith(" ")
				&& !StringUtils.substringBefore(formula, "-")
						.matches("^.*\\d$");
	}

	private Matcher getMatcher(String formula, String regex) {
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(formula);
		return matcher;
	}

	public static void main(String[] args) throws IllegalAccessException,
			IllegalArgumentException, InvocationTargetException,
			NoSuchMethodException, SecurityException {
		SQLParser sqlParser = new SQLParser();
		System.out.println(sqlParser.parserFormula(args[0]));
	}
}
