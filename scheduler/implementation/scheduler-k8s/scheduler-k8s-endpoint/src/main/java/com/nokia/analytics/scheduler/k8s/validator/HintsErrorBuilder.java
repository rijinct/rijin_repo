package com.rijin.analytics.scheduler.k8s.validator;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.xml.sax.SAXParseException;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class HintsErrorBuilder {

	private StringBuilder errorMessage = new StringBuilder();

	private int errorCount = 1;
	
	public HintsErrorBuilder setErrorMessage(SAXParseException saxParseException) {
		errorMessage.append(errorCount++).append(".").append(saxParseException.getMessage().split(":")[1]).append(" at line number ")
				.append(saxParseException.getLineNumber()).append(" and column number")
				.append(saxParseException.getColumnNumber()).append(". ");
		return this;
	}

	public String build() {
		return "XML validation failed with below errors - " + errorMessage.toString();
	}

}
