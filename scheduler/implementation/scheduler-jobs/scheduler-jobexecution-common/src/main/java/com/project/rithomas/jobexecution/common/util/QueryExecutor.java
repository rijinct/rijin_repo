
package com.project.rithomas.jobexecution.common.util;

import java.security.SecureRandom;
import java.util.Iterator;
import java.util.List;

import org.hibernate.HibernateException;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.query.NativeQuery;

import com.rijin.analytics.logging.AnalyticsLogger;
import com.rijin.analytics.logging.AnalyticsLoggerFactory;
import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

public class QueryExecutor {

	private final static AnalyticsLogger LOGGER = AnalyticsLoggerFactory
			.getLogger(QueryExecutor.class);

	public ReConnectUtil reConnectUtil = new ReConnectUtil();

	public void executePostgresqlUpdate(String sql, WorkFlowContext context)
			throws JobExecutionException {
		Session session = null;
		String sessionId = null;
		reConnectUtil = getReconnector();
		while (reConnectUtil.shouldRetry()) {
			try  {
				session = getMetadataSession(context);
				sessionId = "" + System.currentTimeMillis()
						+ new SecureRandom().nextInt();
				LOGGER.debug("Got new db session: {}", sessionId);
				
				Transaction tx = session.beginTransaction();
				NativeQuery nativequery = session.createNativeQuery(sql);
				nativequery.executeUpdate();
				tx.commit();
				
				break;
			} catch (HibernateException e) {
				LOGGER.error(" Hibernate Exception: {}, for query: {}",
						e.getLocalizedMessage(), sql);
				reConnectUtil.updateRetryAttempts();
			} 
			finally {
				if (session != null && session.isOpen()) {
					session.close();
					LOGGER.debug("Closed db session: {}", sessionId);
				}
			}
		}
	}
	
	ReConnectUtil getReconnector() {
		return new ReConnectUtil();
	}

	public Object[] executeMetadatasqlQuery(String sql, WorkFlowContext context)
			throws JobExecutionException {
		List resultList = null;
		Object[] data = null;
		Session session = null;
		String sessionId = null;
		reConnectUtil = getReconnector();
		while (reConnectUtil.shouldRetry()) {
			try {
				session = getMetadataSession(context);
				sessionId = "" + System.currentTimeMillis()
						+ new SecureRandom().nextInt();
				LOGGER.debug("Got new db session: {}", sessionId);
				LOGGER.debug("Query to execute: {}", sql);
				SQLQuery query = session.createSQLQuery(sql);
				resultList = query.list();
				Iterator it = resultList.iterator();
				while (it.hasNext()) {
					Object itObj = it.next();
					if (itObj != null) {
						if (itObj.getClass().isArray()) {
							data = (Object[]) itObj;
						} else {
							data = new Object[] { itObj };
						}
					}
				}
				break;
			} catch (HibernateException e) {
				LOGGER.error("Exception in Hibernate: {} for query: {}",
						e.getLocalizedMessage(), sql);
				reConnectUtil.updateRetryAttempts();
			} finally {
				if (session != null && session.isOpen()) {
					session.close();
					LOGGER.debug("Closed db session: {}", sessionId);
				}
			}
		}
		return data;
	}

	public List executeMetadatasqlQueryMultiple(String sql,
			WorkFlowContext context) throws JobExecutionException {
		Session session = null;
		List resultList = null;
		String sessionId = null;
		reConnectUtil = getReconnector();
		while (reConnectUtil.shouldRetry()) {
			try {
				session = getMetadataSession(context);
				sessionId = "" + System.currentTimeMillis()
						+ new SecureRandom().nextInt();
				LOGGER.debug("Got new db session: {}", sessionId);
				LOGGER.debug("Query to execute: {}", sql);
				SQLQuery query = session.createSQLQuery(sql);
				resultList = query.list();
				break;
			} catch (HibernateException e) {
				LOGGER.error("Error in Hibernate: {} for the query: {}",
						e.getLocalizedMessage(), sql);
				reConnectUtil.updateRetryAttempts();
			} finally {
				if (session != null && session.isOpen()) {
					LOGGER.debug("Closing db session: {}", sessionId);
					session.close();
					LOGGER.debug("Closed db session: {}", sessionId);
				}
			}
		}
		return resultList;
	}

	@Deprecated
	public Session getPostgreSession() {
		return MetadataHibernateUtil.getSessionFactory().openSession();
	}

	private Session getMetadataSession(WorkFlowContext context)
			throws JobExecutionException {
		Session session = null;
		try {
			session = MetadataHibernateUtil.getSessionFactory().openSession();
			context.setProperty(JobExecutionContext.POSTGRE_SESSION, session);
		} catch (HibernateException e) {
			LOGGER.error("Error in Hibernate:", e);
			throw new JobExecutionException("Error in Hibernate:"
					+ e.getLocalizedMessage(), e);
		}
		return session;
	}
}