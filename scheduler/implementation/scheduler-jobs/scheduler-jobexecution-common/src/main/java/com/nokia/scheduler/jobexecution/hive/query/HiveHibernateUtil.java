
package com.rijin.scheduler.jobexecution.hive.query;

import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.rijin.scheduler.jobexecution.hive.HiveConfiguration;
import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;

public class HiveHibernateUtil {

	private static SessionFactory sessionFactory;

	private static String hibernateConfigFile;

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(HiveHibernateUtil.class);
	static {
		try {
			HiveConfiguration hiveConfiguration = HiveConfigurationProvider.getInstance().getConfiguration();
			hibernateConfigFile = "hibernate_hive.cfg.xml";
			Configuration configuration = new Configuration().configure(
					hibernateConfigFile).setProperty(
					"hibernate.connection.url", hiveConfiguration.getDbUrl());
			
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
