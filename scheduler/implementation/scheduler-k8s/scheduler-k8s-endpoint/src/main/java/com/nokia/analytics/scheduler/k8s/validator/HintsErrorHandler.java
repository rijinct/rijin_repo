package com.rijin.analytics.scheduler.k8s.validator;

import java.util.LinkedList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class HintsErrorHandler implements ErrorHandler {

	private final List<SAXParseException> exceptions = new LinkedList<>();

	@Autowired
	private HintsErrorBuilder builder;

	public void warning(SAXParseException exception) throws SAXException {
		exceptions.add(exception);
	}

	public void error(SAXParseException exception) throws SAXException {
		exceptions.add(exception);
	}

	public void fatalError(SAXParseException exception) throws SAXException {
		exceptions.add(exception);
	}

	public List<SAXParseException> getExceptions() {
		return new LinkedList<>(exceptions);
	}

	public void handleMessage() throws SAXException {
		if (!this.exceptions.isEmpty()) {

			for (SAXParseException saxParseException : this.exceptions) {
				builder.setErrorMessage(saxParseException);
			}
			throw new SAXException();
		}
	}

	public String getErrorMessage() {
		return builder.build();
	}

}