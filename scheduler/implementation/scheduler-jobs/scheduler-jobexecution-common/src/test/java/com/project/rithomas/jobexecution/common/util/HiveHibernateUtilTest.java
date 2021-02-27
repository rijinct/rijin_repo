
package com.project.rithomas.jobexecution.common.util;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import java.util.Properties;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.query.HiveHibernateUtil;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { HiveHibernateUtil.class, Configuration.class,
		Properties.class, SessionFactory.class })
public class HiveHibernateUtilTest {

	@Ignore
	public void testGetSessionFactory() throws Exception {
		Configuration mockConfiguration = mock(Configuration.class);
		PowerMockito.when(mockConfiguration.configure("hibernate_hive.cfg.xml"))
				.thenReturn(mockConfiguration);
		
		Properties mockProperties = mock(Properties.class);
		SessionFactory mockSessionFactory = mock(SessionFactory.class);
		PowerMockito.whenNew(Configuration.class).withNoArguments()
				.thenReturn(mockConfiguration);
		PowerMockito.when(mockConfiguration.getProperties())
				.thenReturn(mockProperties);
		PowerMockito
				.when(mockConfiguration.buildSessionFactory(
						Mockito.any(ServiceRegistry.class)))
				.thenReturn(mockSessionFactory);
		assertNotNull(HiveHibernateUtil.getSessionFactory());
	}
}
