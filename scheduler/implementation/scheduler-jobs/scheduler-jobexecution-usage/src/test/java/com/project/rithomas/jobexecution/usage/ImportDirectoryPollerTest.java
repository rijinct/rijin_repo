
package com.project.rithomas.jobexecution.usage;

import static org.powermock.api.mockito.PowerMockito.mock;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.hibernate.Session;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.model.meta.JobDictionary;
import com.project.rithomas.sdk.model.meta.JobProperty;
import com.project.rithomas.sdk.model.meta.query.JobDictionaryQuery;
import com.project.rithomas.sdk.model.meta.query.JobPropertyQuery;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;
@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { GetDBResource.class, FileSystem.class,
		Configuration.class, QueryExecutor.class, Session.class,
		UpdateJobStatus.class, ImportDirectoryPoller.class, Thread.class,
		ImportDirectoryPollerTest.class })
public class ImportDirectoryPollerTest {

	GeneratorWorkFlowContext context = new GeneratorWorkFlowContext();

	ImportDirectoryPoller importDirectoryPoller = new ImportDirectoryPoller();

	@Mock
	JobDictionaryQuery jobDictQuery;
	@Mock
	JobPropertyQuery jobPropQuery;
	@Mock
	FileSystem mockFileSystem;

	@Mock
	Configuration mockConfiguration;

	long WAIT_INTERVAL = 10l;
	JobDictionary jobDictionary = new JobDictionary();
	List<JobProperty> jobPropList = new ArrayList<JobProperty>();

	@Before
	public void setUp() throws Exception {
		context.setProperty(JobExecutionContext.JOB_NAME, "Usage_GN_1_LoadJob");
		context.setProperty(JobExecutionContext.IMPORT_PATH,
				"/rithomas/us/import/GN_1/GN_1/");
		context.setProperty(JobExecutionContext.WORK_DIR_PATH,
				"/rithomas/us/work/GN_1/GN_1/");
		context.setProperty(JobExecutionContext.HADOOP_NAMENODE_IP,
				"10.58.127.74");
		context.setProperty(JobExecutionContext.HADOOP_NAMENODE_PORT, "54310");
		context.setProperty(JobExecutionContext.ETL_STATUS_SEQ, BigInteger.ONE);
		GetDBResource mockGetDBResource = mock(GetDBResource.class);
		PowerMockito.mockStatic(GetDBResource.class);
		PowerMockito.when(GetDBResource.getInstance()).thenReturn(
				mockGetDBResource);
		PowerMockito.when(mockGetDBResource.getHdfsConfiguration())
				.thenReturn(mockConfiguration);
		QueryExecutor mockQueryExecutor = mock(QueryExecutor.class);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(mockQueryExecutor);
		Session mockSession = mock(Session.class);
		PowerMockito.mockStatic(FileSystem.class);
		PowerMockito.when(FileSystem.get(mockConfiguration))
				.thenReturn(mockFileSystem);
		Path workPath = new Path("/rithomas/us/work/GN_1/GN_1/");
		Path importPath = new Path("/rithomas/us/import/GN_1/GN_1/");
		PowerMockito.when(mockFileSystem.exists(workPath)).thenReturn(true);
		PowerMockito.when(mockFileSystem.exists(importPath)).thenReturn(true);
		// Path workPath = new Path("/mnt/staging/work/GN_1/GN_1/");
		context.setProperty(JobExecutionContext.PREV_STATUS, "E");
		FileStatus fswork = new FileStatus(0, true, 1, 1, 123132, 234324, null,
				"rithomas", "rithomas", new Path("/rithomas/us/work/GN_1/GN_1/"));
		FileStatus fs1 = new FileStatus(0, false, 1, 0, 1232312, 131234213,
				null, "rithomas", "rithomas", workPath);
		FileStatus fs2 = new FileStatus(0, false, 1, 0, 1232312, 131234213,
				null, "rithomas", "rithomas", workPath);
		FileStatus[] fileStatusArray = { fswork };
		FileStatus[] fileStatusArrayDatFiles = { fs1, fs2 };
		PowerMockito.when(mockFileSystem.listStatus(workPath))
				.thenReturn(fileStatusArray);
		PowerMockito
				.when(mockFileSystem.listStatus(Mockito.eq(workPath),
						Mockito.any(PathFilter.class)))
				.thenReturn(fileStatusArrayDatFiles);
		PowerMockito.when(mockFileSystem.delete(Mockito.any(Path.class),
						Mockito.anyBoolean())).thenReturn(true);
		PowerMockito.whenNew(JobDictionaryQuery.class).withNoArguments()
				.thenReturn(jobDictQuery);
		PowerMockito.whenNew(JobPropertyQuery.class).withNoArguments()
				.thenReturn(jobPropQuery);
		Mockito.when(jobDictQuery.retrieve(Mockito.anyString()))
				.thenReturn(jobDictionary);
		Mockito.when(jobPropQuery.retrieve(Mockito.anyInt()))
				.thenReturn(jobPropList);
	}

	@Test
	public void testExecutePreviousFailed()
			throws WorkFlowExecutionException, IOException {
		importDirectoryPoller.execute(context);
	}

	@Test
	public void testExecutePreviousCommitted() throws Exception {
		Path importPath = new Path("/rithomas/us/import/GN_1/GN_1/");
		FileStatus fs = new FileStatus(0, true, 1, 1, 123132, 234324, null,
				"rithomas", "rithomas", importPath);
		FileStatus fs1 = new FileStatus(0, false, 1, 0, 1232312, 131234213,
				null, "rithomas", "rithomas", importPath);
		FileStatus fs2 = new FileStatus(0, false, 1, 0, 1232312, 131234213,
				null, "rithomas", "rithomas", importPath);
		FileStatus[] fileStatusArray = { fs };
		FileStatus[] fileStatusArrayDatFiles = { fs1, fs2 };
		Path workPath = new Path("/rithomas/us/work/GN_1/GN_1/");
		FileStatus fswork = new FileStatus(0, true, 1, 1, 123132, 234324, null,
				"rithomas", "rithomas", new Path("/rithomas/us/work/GN_1/GN_1/"));
		FileStatus fs1w = new FileStatus(0, false, 1, 0, 1232312, 131234213,
				null, "rithomas", "rithomas", workPath);
		FileStatus fs2w = new FileStatus(0, false, 1, 0, 1232312, 131234213,
				null, "rithomas", "rithomas", workPath);
		FileStatus[] fileStatusArrayw = { fswork };
		FileStatus[] fileStatusArrayDatFilesw = { fs1w, fs2w };
		PowerMockito.when(mockFileSystem.listStatus(workPath))
				.thenReturn(fileStatusArrayw);
		PowerMockito
				.when(mockFileSystem.listStatus(Mockito.eq(workPath),
						Mockito.any(PathFilter.class)))
				.thenReturn(fileStatusArrayDatFilesw);
		PowerMockito.when(mockFileSystem.listStatus(importPath))
				.thenReturn(fileStatusArray);
		PowerMockito
				.when(mockFileSystem.listStatus(Mockito.eq(importPath),
						Mockito.any(PathFilter.class)))
				.thenReturn(null, fileStatusArray, fileStatusArrayDatFiles);
		context.setProperty(JobExecutionContext.PREV_STATUS,
				JobExecutionContext.COMMIT_DONE);
		importDirectoryPoller.execute(context);
	}

	// @Test(expected = WorkFlowExecutionException.class)
	// public void testInterruptedException()
	// throws WorkFlowExecutionException, IOException, InterruptedException {
	//
	// PowerMockito.mockStatic(Thread.class);
	// // PowerMockito.when(Thread.sleep(WAIT_INTERVAL));
	// // Thread.sleep(WAIT_INTERVAL);
	// importDirectoryPoller.execute(context);
	// }
	// @Test
	// public void testIOException()
	// throws WorkFlowExecutionException, IOException {
	//
	// Path workPath = new Path("/mnt/staging/work/GN_1/GN_1/");
	//
	// PowerMockito.when(mockFileSystem.delete(workPath, true)).thenThrow(
	// new IOException());
	// importDirectoryPoller.execute(context);
	//
	// }
	@Test(expected = WorkFlowExecutionException.class)
	public void testWorkFlowExecutionException2()
			throws WorkFlowExecutionException, IOException {
		PowerMockito.when(FileSystem.get(Mockito.any(Configuration.class)))
				.thenThrow(new IOException());
		importDirectoryPoller.execute(context);
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void testWorkFlowExecutionException()
			throws WorkFlowExecutionException {
		context.setProperty(JobExecutionContext.IMPORT_PATH, null);
		importDirectoryPoller.execute(context);
	}

	// Test cases for TIME_ZONE_SUPPORT
	// test this
	@Test
	public void testExecutePreviousCommittedWithTZ() throws Exception {
		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "yes");
		Path importPath = new Path("/rithomas/us/import/GN_1/GN_1/");
		FileStatus fs = new FileStatus(0, true, 1, 1, 123132, 234324, null,
				"rithomas", "rithomas",
				new Path("/rithomas/us/import/GN_1/GN_1/dt=1395941400000"));
		FileStatus fsWithRgn = new FileStatus(0, true, 1, 0, 1232312, 131234213,
				null, "rithomas", "rithomas",
				new Path("/rithomas/us/import/GN_1/GN_1/dt=1395941400000/tz=RGN1"));
		FileStatus fs1 = new FileStatus(0, false, 1, 0, 1232312, 131234213,
				null, "rithomas", "rithomas", new Path(
						"/rithomas/us/import/GN_1/GN_1/dt=1395941400000/tz=RGN1/gn.dat"));
		FileStatus fs2 = new FileStatus(0, false, 1, 0, 1232312, 131234213,
				null, "rithomas", "rithomas", new Path(
						"/rithomas/us/import/GN_1/GN_1/dt=1395941400000/tz=RGN1/gn1.dat"));
		FileStatus[] fileStatusArray = { fs };
		FileStatus[] fileStatusRegion = { fsWithRgn };
		FileStatus[] fileStatusArrayDatFiles = { fs1, fs2 };
		Path workPath = new Path("/rithomas/us/work/GN_1/GN_1/");
		FileStatus fswork = new FileStatus(0, true, 1, 1, 123132, 234324, null,
				"rithomas", "rithomas", new Path("/rithomas/us/work/GN_1/GN_1/"));
		FileStatus fs1w = new FileStatus(0, false, 1, 0, 1232312, 131234213,
				null, "rithomas", "rithomas", workPath);
		FileStatus fs2w = new FileStatus(0, false, 1, 0, 1232312, 131234213,
				null, "rithomas", "rithomas", workPath);
		FileStatus[] fileStatusArrayw = { fswork };
		FileStatus[] fileStatusArrayDatFilesw = { fs1w, fs2w };
		PowerMockito.when(mockFileSystem.listStatus(workPath))
				.thenReturn(fileStatusArrayw);
		PowerMockito
				.when(mockFileSystem.listStatus(Mockito.eq(workPath),
						Mockito.any(PathFilter.class)))
				.thenReturn(fileStatusArrayDatFilesw);
		PowerMockito.when(mockFileSystem.listStatus(importPath))
				.thenReturn(fileStatusArray);
		PowerMockito
				.when(mockFileSystem.listStatus(
						new Path("/rithomas/us/import/GN_1/GN_1/dt=1395941400000")))
				.thenReturn(fileStatusRegion);
		PowerMockito
				.when(mockFileSystem.listStatus(Mockito.eq(new Path(
						"/rithomas/us/import/GN_1/GN_1/dt=1395941400000/tz=RGN1")),
						Mockito.any(PathFilter.class)))
				.thenReturn(null, fileStatusArray, fileStatusArrayDatFiles);
		context.setProperty(JobExecutionContext.PREV_STATUS,
				JobExecutionContext.COMMIT_DONE);
		importDirectoryPoller.execute(context);
	}
}
