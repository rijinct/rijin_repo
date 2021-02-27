
package com.rijin.analytics.scheduler.k8s.validator;

import java.io.File;
import java.io.StringReader;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.analytics.scheduler.k8s.exception.InvalidXMLException;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class HintsSchemaValidator {
	private static final AnalyticsLogger LOGGER = AnalyticsLoggerFactory.getLogger(HintsSchemaValidator.class);

	private static final String scheduler_CONF_LOCATION = "scheduler_CONF_LOCATION";

	@Autowired
	HintsErrorHandler hintsErrorHandler;

	public void validate(String xmlString, String queryEngine) {
		StreamSource source = null;
		String xsdFileLocation = null;
		try {
			xsdFileLocation = getXSDFilePath(queryEngine);
			StringReader reader = new StringReader(xmlString);
			File file = new File(xsdFileLocation);
			Schema schema = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI).newSchema(file);

			Validator validator = schema.newValidator();
			validator.setErrorHandler(hintsErrorHandler);
			source = new StreamSource(reader);
			validator.validate(source);
			hintsErrorHandler.handleMessage();
		} catch (Exception ex) {
			LOGGER.error("XML Validation failed", ex);
			throw new InvalidXMLException(hintsErrorHandler.getErrorMessage(), "invalidXML");
		} finally {
			if (source != null) {
				IOUtils.closeQuietly(source.getInputStream());
			}
		}

	}

	private String getXSDFilePath(String queryExecutionEngine) {
		return System.getenv(scheduler_CONF_LOCATION) + StringUtils.toLowerCase(queryExecutionEngine) + "_settings.xsd";
	}
}
