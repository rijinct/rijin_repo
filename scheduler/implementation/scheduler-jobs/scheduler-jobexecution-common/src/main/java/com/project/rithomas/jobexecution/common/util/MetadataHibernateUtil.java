
package com.project.rithomas.jobexecution.common.util;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;

public class MetadataHibernateUtil {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(MetadataHibernateUtil.class);

	private static SessionFactory sessionFactory;

	private static String hibernateConfigFile;
	static {
		try {
			hibernateConfigFile = "hibernate_postgresql.cfg.xml";
			Configuration configuration = new Configuration()
					.configure(hibernateConfigFile)
					.setProperty("hibernate.connection.url",
							GetDBResource.getPostgresUrl())
					.setProperty("hibernate.connection.password",
							GetDBResource.getPostgrePassword());
			
			StandardServiceRegistryBuilder serviceRegistryBuilder = new StandardServiceRegistryBuilder().applySettings(
					configuration.getProperties());

			sessionFactory = configuration.buildSessionFactory(serviceRegistryBuilder.build());
		} catch (Exception ex) {
			LOGGER.error("Failed to create sessionFactory object."
					+ ex.getLocalizedMessage());
			throw new ExceptionInInitializerError(ex);
		}
	}

	public static SessionFactory getSessionFactory() {
		return sessionFactory;
	}
}
