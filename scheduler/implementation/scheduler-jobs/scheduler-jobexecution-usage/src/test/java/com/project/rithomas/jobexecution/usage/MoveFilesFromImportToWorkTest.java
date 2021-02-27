
package com.project.rithomas.jobexecution.usage;

import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.cem.sai.jdbc.ConnectionManager;
import com.project.rithomas.jobexecution.common.util.GetDBResource;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.sai.rithomas.scheduler.constants.schedulerConstants;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { MoveFilesFromImportToWork.class, GetDBResource.class,
		ConnectionManager.class, FileSystem.class, BufferedReader.class })
public class MoveFilesFromImportToWorkTest {

	MoveFilesFromImportToWork moveFiles = new MoveFilesFromImportToWork();

	WorkFlowContext context = new JobExecutionContext();

	@Mock
	UpdateJobStatus updateJobStatus;

	@Mock
	GetDBResource getDBResource;

	@Mock
	Configuration conf;

	@Mock
	FileSystem fileSystem, fileSystem1;

	@Mock
	Connection connection;

	@Mock
	PreparedStatement statement;

	@Mock
	ResultSet resultSet;

	@Mock
	MoveFilesFromImportToWork mockMoveFiles;

	@Mock
	FSDataInputStream mockDataInputStream;

	@Mock
	BufferedReader mockBufferedReader;

	@Mock
	InputStreamReader mockInputStreamReader;


	@Before
	public void setUp() throws Exception {
		context.setProperty(JobExecutionContext.HIVE_PARTITION_COLUMN, "dt");
		context.setProperty(JobExecutionContext.TIMEZONE_PARTITION_COLUMN, "tz");
		
		PowerMockito.whenNew(UpdateJobStatus.class).withNoArguments()
				.thenReturn(updateJobStatus);
		mockStatic(GetDBResource.class);
		PowerMockito.when(GetDBResource.getInstance())
				.thenReturn(getDBResource);
		Mockito.when(getDBResource.getHdfsConfiguration()).thenReturn(
				conf);
		mockStatic(FileSystem.class);
		Mockito.when(FileSystem.get(conf)).thenReturn(fileSystem);
		mockStatic(ConnectionManager.class);
		PowerMockito.when(
				ConnectionManager
						.getConnection(schedulerConstants.HIVE_DATABASE))
				.thenReturn(connection);
		PowerMockito.when(connection.createStatement()).thenReturn(statement);
		PowerMockito.whenNew(InputStreamReader.class)
				.withArguments(mockDataInputStream)
				.thenReturn(mockInputStreamReader);
		PowerMockito.whenNew(BufferedReader.class)
				.withArguments(mockInputStreamReader)
				.thenReturn(mockBufferedReader);
	}

	@Test
	public void testMoveFilesFromImportToWork() throws Exception {
		Set<String> importDirPaths = new HashSet<String>();
		importDirPaths.add("/rithomas/us/import/SMS_1/SMS_1/dt=1394703900000");
		context.setProperty(JobExecutionContext.IMPORT_DIR_PATH, importDirPaths);
		context.setProperty(JobExecutionContext.SOURCE_TABLE_NAME, "INT_SMS_1");
		context.setProperty(JobExecutionContext.JOB_NAME, "Usage_SMS_1_LoadJob");
		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "NO");
		Mockito.when(
				statement
						.executeQuery("alter table INT_SMS_1 add partition (dt='1394703900000')"))
				.thenReturn(resultSet);
		Mockito.when(FileSystem.get((Configuration) Mockito.anyObject()))
				.thenReturn(fileSystem);
		Path srcPath = new Path("/rithomas/us/import/SMS_1/SMS_1/dt=1394703900000");
		PowerMockito.whenNew(Path.class)
				.withArguments("/rithomas/us/import/SMS_1/SMS_1/dt=1394703900000")
				.thenReturn(srcPath);
		FileStatus fs1 = new FileStatus(0, false, 1, 0, 1232312, 131234213,
				null, "rithomas", "rithomas", srcPath);
		FileStatus[] fileStatusArrayDatFiles = { fs1 };
		Mockito.when(fileSystem.exists(srcPath)).thenReturn(true);
		PowerMockito.when(
				fileSystem.listStatus(Mockito.eq(srcPath),
						Mockito.any(PathFilter.class))).thenReturn(
				fileStatusArrayDatFiles);
		PowerMockito.when(
				fileSystem.rename(Mockito.any(Path.class),
						Mockito.any(Path.class))).thenReturn(true);
		Mockito.when(fileSystem.open(Mockito.any(Path.class))).thenReturn(
				mockDataInputStream);
		Mockito.when(mockBufferedReader.readLine()).thenReturn("1");
		assertTrue(moveFiles.execute(context));
	}

	@Test
	public void testMoveFilesFromImportToWorkWhenSrcDestAreSame()
			throws Exception {
		Set<String> importDirPaths = new HashSet<String>();
		importDirPaths
				.add("/rithomas/us/work/SMS_1/SMS_1/dt=1394703900000/tz=Default");
		context.setProperty(JobExecutionContext.IMPORT_DIR_PATH, importDirPaths);
		context.setProperty(JobExecutionContext.SOURCE_TABLE_NAME, "INT_SMS_1");
		context.setProperty(JobExecutionContext.JOB_NAME, "Usage_SMS_1_LoadJob");
		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "NO");
		Mockito.when(
				statement
						.executeQuery("alter table INT_SMS_1 add partition (dt='1394703900000', tz='Default')"))
				.thenReturn(resultSet);
		Mockito.when(FileSystem.get((Configuration) Mockito.anyObject()))
				.thenReturn(fileSystem);
		Path srcPath = new Path(
				"/rithomas/us/work/SMS_1/SMS_1/dt=1394703900000/tz=Default");
		PowerMockito
				.whenNew(Path.class)
				.withArguments(
						"/rithomas/us/work/SMS_1/SMS_1/dt=1394703900000/tz=Default")
				.thenReturn(srcPath);
		FileStatus fs1 = new FileStatus(0, false, 1, 0, 1232312, 131234213,
				null, "rithomas", "rithomas", srcPath);
		FileStatus[] fileStatusArrayDatFiles = { fs1 };
		Mockito.when(fileSystem.exists(srcPath)).thenReturn(true);
		PowerMockito.when(
				fileSystem.listStatus(Mockito.eq(srcPath),
						Mockito.any(PathFilter.class))).thenReturn(
				fileStatusArrayDatFiles);
		PowerMockito.when(
				fileSystem.rename(Mockito.any(Path.class),
						Mockito.any(Path.class))).thenReturn(true);
		Mockito.when(fileSystem.open(Mockito.any(Path.class))).thenReturn(
				mockDataInputStream);
		Mockito.when(mockBufferedReader.readLine()).thenReturn("1");
		assertTrue(moveFiles.execute(context));
	}

	@Test
	public void testMoveFilesFromImportToWorkRenamingError()
			throws SQLException, IOException {
		Set<String> importDirPaths = new HashSet<String>();
		importDirPaths.add("/rithomas/us/import/SMS_1/SMS_1/dt=1394703900000");
		context.setProperty(JobExecutionContext.IMPORT_DIR_PATH, importDirPaths);
		context.setProperty(JobExecutionContext.SOURCE_TABLE_NAME, "INT_SMS_1");
		context.setProperty(JobExecutionContext.JOB_NAME, "Usage_SMS_1_LoadJob");
		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "NO");
		try {
			Mockito.when(
					statement
							.executeQuery("alter table INT_SMS_1 add partition (dt='1394703900000')"))
					.thenReturn(resultSet);
			Mockito.when(FileSystem.get((Configuration) Mockito.anyObject()))
					.thenReturn(fileSystem);
			Path srcPath = new Path(
					"/rithomas/us/import/SMS_1/SMS_1/dt=1394703900000");
			Path destPath = new Path(
					"/rithomas/us/import/SMS_1/SMS_1/dt=1394703900000");
			FileStatus fs1 = new FileStatus(0, false, 1, 0, 1232312, 131234213,
					null, "rithomas", "rithomas", srcPath);
			FileStatus[] fileStatusArrayDatFiles = { fs1 };
			Mockito.when(fileSystem.exists(srcPath)).thenReturn(true);
			PowerMockito.when(
					fileSystem.listStatus(Mockito.eq(srcPath),
							Mockito.any(PathFilter.class))).thenReturn(
					fileStatusArrayDatFiles);
			PowerMockito.when(fileSystem.rename(srcPath, destPath)).thenReturn(
					false);
			Mockito.when(fileSystem.open(Mockito.any(Path.class))).thenReturn(
					mockDataInputStream);
			Mockito.when(mockBufferedReader.readLine()).thenReturn("1");
			moveFiles.execute(context);
		} catch (WorkFlowExecutionException e) {
			assertTrue(e.getMessage().contains(
					"Error occured while renaming the files"));
		}
	}

	@Test
	public void testIOException() throws WorkFlowExecutionException,
			IOException {
		try {
			PowerMockito.when(FileSystem.get(Mockito.any(Configuration.class)))
					.thenThrow(new IOException());
			moveFiles.execute(context);
		} catch (WorkFlowExecutionException e) {
			assertTrue(e.getMessage().contains(
					"Exception in hadoop filesystem creation"));
		}
	}

	@Test
	public void testSQLException() throws WorkFlowExecutionException,
			SQLException {
		Set<String> importDirPaths = new HashSet<String>();
		importDirPaths.add("/rithomas/us/import/SMS_1/SMS_1/dt=1394703900000");
		context.setProperty(JobExecutionContext.IMPORT_DIR_PATH, importDirPaths);
		context.setProperty(JobExecutionContext.SOURCE_TABLE_NAME, "INT_SMS_1");
		context.setProperty(JobExecutionContext.JOB_NAME, "Usage_SMS_1_LoadJob");
		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "NO");
		try {
			PowerMockito.when(connection.createStatement()).thenThrow(
					new SQLException());
			moveFiles.execute(context);
		} catch (WorkFlowExecutionException e) {
			assertTrue(e.getMessage().contains("SQLException"));
		}
	}

	@Test
	public void testMoveFilesFromImportToWorkNoDestinationError()
			throws SQLException, IOException {
		Set<String> importDirPaths = new HashSet<String>();
		importDirPaths.add("/rithomas/us/import/SMS_1/SMS_1/dt=1394703900000");
		context.setProperty(JobExecutionContext.IMPORT_DIR_PATH, importDirPaths);
		context.setProperty(JobExecutionContext.SOURCE_TABLE_NAME, "INT_SMS_1");
		context.setProperty(JobExecutionContext.JOB_NAME, "Usage_SMS_1_LoadJob");
		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "NO");
		try {
			Mockito.when(
					statement
							.executeQuery("alter table INT_SMS_1 add partition (dt='1394703900000')"))
					.thenReturn(resultSet);
			Mockito.when(FileSystem.get((Configuration) Mockito.anyObject()))
					.thenReturn(fileSystem);
			Path srcPath = new Path(
					"/rithomas/us/import/SMS_1/SMS_1/dt=1394703900000");
			Mockito.when(fileSystem.exists(srcPath)).thenReturn(false);
			moveFiles.execute(context);
		} catch (WorkFlowExecutionException e) {
			assertTrue(e.getMessage().contains(
					"Error occured while renaming the files"));
		}
	}

	@Test
	public void testMoveFilesFromImportToWorkForLTE4G() throws Exception {
		Set<String> importDirPaths = new HashSet<String>();
		importDirPaths.add("/rithomas/us/import/LTE_1/LTE_4G_1/dt=1394703900000");
		context.setProperty(JobExecutionContext.IMPORT_DIR_PATH, importDirPaths);
		context.setProperty(JobExecutionContext.SOURCE_TABLE_NAME,
				"INT_LTE_4G_1");
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Usage_LTE_4G_1_LoadJob");
		Mockito.when(
				statement
						.executeQuery("alter table INT_LTE_4G_1 add partition (dt='1394703900000')"))
				.thenReturn(resultSet);
		Mockito.when(FileSystem.get((Configuration) Mockito.anyObject()))
				.thenReturn(fileSystem1);
		Path srcPath = new Path(
				"/rithomas/us/import/LTE_1/LTE_4G_1/dt=1394703900000");
		Mockito.when(fileSystem1.exists(srcPath)).thenReturn(true);
		Path destPathOfHTTP4G = new Path(
				"/rithomas/us/import/HTTP_4G_1/HTTP_4G_1/dt=1394703900000");
		FileStatus mockFileStatus = Mockito.mock(FileStatus.class);
		FileStatus[] listOfFilesInDir = {};
		Mockito.when(fileSystem1.listStatus(srcPath)).thenReturn(
				listOfFilesInDir);
		Path finalDestPathOfHTTP4G = new Path(
				"/rithomas/us/import/HTTP_4G_1/HTTP_4G_1/dt=1394703900000/lte.dat.tmp");
		FileStatus fs = new FileStatus(0, false, 1, 0, 1232312, 131234213,
				null, "rithomas", "rithomas", finalDestPathOfHTTP4G);
		FileStatus[] listOfFilesInDir1 = { fs };
		Mockito.when(fileSystem1.listStatus(destPathOfHTTP4G)).thenReturn(
				listOfFilesInDir1);
		Mockito.when(fileSystem1.exists(destPathOfHTTP4G)).thenReturn(false);
		Mockito.when(mockFileStatus.isDir()).thenReturn(false);
		Path eachFileInDir = Mockito.mock(Path.class);
		Mockito.when(mockFileStatus.getPath()).thenReturn(eachFileInDir);
		Mockito.when(eachFileInDir.toString()).thenReturn("lte.dat");
		Mockito.when(eachFileInDir.getName()).thenReturn("lte.dat");
		FileStatus fs1 = new FileStatus(0, false, 1, 0, 1232312, 131234213,
				null, "rithomas", "rithomas", srcPath);
		FileStatus[] fileStatusArrayDatFiles = { fs1 };
		PowerMockito.when(
				fileSystem1.listStatus(Mockito.eq(srcPath),
						Mockito.any(PathFilter.class))).thenReturn(
				fileStatusArrayDatFiles);
		Path destPathOfLTE = new Path(
				"/rithomas/us/work/LTE_1/LTE_4G_1/dt=1394703900000");
		PowerMockito.when(fileSystem1.rename(srcPath, destPathOfLTE))
				.thenReturn(true);
		PowerMockito
				.when(fileSystem1
						.rename(finalDestPathOfHTTP4G,
								new Path(
										"/rithomas/us/import/HTTP_4G_1/HTTP_4G_1/dt=1394703900000/lte.dat")))
				.thenReturn(true);
		Mockito.when(fileSystem1.open(Mockito.any(Path.class))).thenReturn(
				mockDataInputStream);
		Mockito.when(mockBufferedReader.readLine()).thenReturn("1");
		assertTrue(moveFiles.execute(context));
	}

	@Test
	public void testMoveFilesFromImportToWorkWithTZ() throws Exception {
		Set<String> importDirPaths = new HashSet<String>();
		importDirPaths
				.add("/rithomas/us/import/SMS_1/SMS_1/dt=1394703900000/tz=RGN1");
		context.setProperty(JobExecutionContext.IMPORT_DIR_PATH, importDirPaths);
		context.setProperty(JobExecutionContext.SOURCE_TABLE_NAME, "INT_SMS_1");
		context.setProperty(JobExecutionContext.JOB_NAME, "Usage_SMS_1_LoadJob");
		context.setProperty(JobExecutionContext.TIME_ZONE_SUPPORT, "YES");
		Mockito.when(
				statement
						.executeQuery("alter table INT_SMS_1 add partition (dt='1394703900000',tz='RGN1')"))
				.thenReturn(resultSet);
		Mockito.when(FileSystem.get((Configuration) Mockito.anyObject()))
				.thenReturn(fileSystem);
		Path srcPath = new Path(
				"/rithomas/us/import/SMS_1/SMS_1/dt=1394703900000/tz=RGN1");
		// PowerMockito.whenNew(Path.class)
		// .withArguments("/rithomas/us/import/SMS_1/SMS_1/dt=1394703900000")
		// .thenReturn(srcPath);
		FileStatus fs1 = new FileStatus(0, false, 1, 0, 1232312, 131234213,
				null, "rithomas", "rithomas", srcPath);
		FileStatus[] fileStatusArrayDatFiles = { fs1 };
		Mockito.when(fileSystem.exists(srcPath)).thenReturn(true);
		PowerMockito.when(
				fileSystem.listStatus(Mockito.eq(srcPath),
						Mockito.any(PathFilter.class))).thenReturn(
				fileStatusArrayDatFiles);
		PowerMockito.when(
				fileSystem.rename(Mockito.any(Path.class),
						Mockito.any(Path.class))).thenReturn(true);
		Mockito.when(fileSystem.open(Mockito.any(Path.class))).thenReturn(
				mockDataInputStream);
		Mockito.when(mockBufferedReader.readLine()).thenReturn("1");
		assertTrue(moveFiles.execute(context));
	}
}
