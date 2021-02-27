
package com.rijin.analytics.scheduler.k8s;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.rijin.scheduler.jobexecution.hive.HiveConfigurationProvider;
import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ HiveConfigurationProvider.class, ConnectionManager.class,
		GetDBResource.class, QueryExecutor.class, FileSystem.class,
		HealthCheckUtils.class })
public class HealthCheckUtilsTest {

	@Mock
	GetDBResource mockGetDBResource;

	@Mock
	JobExecutionContext mockContext;

	@Mock
	Configuration conf;

	@Mock
	FileSystem fileSystem;

	@Mock
	QueryExecutor mockQueryExecutor;

	@Mock
	Path path;

	@Mock
	FileStatus status;

	@Before
	public void setUp() throws Exception {
		PowerMockito.mockStatic(GetDBResource.class);
		PowerMockito.when(GetDBResource.getInstance())
				.thenReturn(mockGetDBResource);
		doNothing().when(mockGetDBResource)
				.retrievePostgresObjectProperties(mockContext);
		doReturn(conf).when(mockGetDBResource).getHdfsConfiguration();
		PowerMockito.whenNew(JobExecutionContext.class).withNoArguments()
				.thenReturn(mockContext);
		PowerMockito.mockStatic(FileSystem.class);
		PowerMockito.when(FileSystem.get(conf)).thenReturn(fileSystem);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(mockQueryExecutor);
	}

	@Test
	public void getFolderModifiedTimestampTest() throws Exception {
		String directory = "/rithomas/us/import/VOICE_1/VOICE_1";
		Long time = new Long(1598504724);
		FileStatus[] statusArray = { status };
		PowerMockito.whenNew(Path.class).withArguments(directory)
				.thenReturn(path);
		PowerMockito.when(path.getParent()).thenReturn(path);
		PowerMockito.when(status.getModificationTime()).thenReturn(time);
		when(fileSystem.listStatus(path)).thenReturn(statusArray);
		Assert.assertNotNull(
				HealthCheckUtils.getFolderModifiedTimestamp(directory));
	}
}
