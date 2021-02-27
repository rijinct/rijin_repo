
package com.project.rithomas.jobexecution.project.dqm;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import com.project.rithomas.sdk.model.meta.DeploymentDictionary;
import com.project.rithomas.sdk.model.meta.GeneratorProperty;
import com.project.rithomas.sdk.model.meta.query.GeneratorPropertyQuery;
import com.project.rithomas.sdk.model.utils.ModelResources;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.generator.GeneratorWorkFlowContext;


@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { ScanFilesInDQMFolder.class, ModelResources.class,
		FileUtils.class, GetDBResource.class, FileSystem.class, GetDBResource.class})
public class ScanFilesInDQMFolderTest {

	ScanFilesInDQMFolder fileScanner = null;

	WorkFlowContext context = new GeneratorWorkFlowContext();
	
	@Mock
	GetDBResource getDBResource;

	@Before
	public void setUp() throws Exception {
		fileScanner = new ScanFilesInDQMFolder();
		GeneratorPropertyQuery mockGenPropQuery = mock(GeneratorPropertyQuery.class);
		PowerMockito.whenNew(GeneratorPropertyQuery.class).withNoArguments()
				.thenReturn(mockGenPropQuery);
		mockStatic(ModelResources.class);
		mockStatic(FileUtils.class);
		mockStatic(GetDBResource.class);
		// mockStatic(GetDBResource.class);
		mockStatic(FileSystem.class);
		PowerMockito.when(GetDBResource.getInstance())
		.thenReturn(getDBResource);
		EntityManager entityManager = mock(EntityManager.class);
		TypedQuery<String> query = (TypedQuery<String>) mock(TypedQuery.class);
		
		TypedQuery<DeploymentDictionary> typedQueryDeploymentDictionary = (TypedQuery<DeploymentDictionary>) mock(TypedQuery.class);
		DeploymentDictionary deploymentDictionary = new DeploymentDictionary();
		deploymentDictionary.setAdaptationid("SMS");
		deploymentDictionary.setAdaptationVersion("1");
		deploymentDictionary.setObjectId("SMS");
		deploymentDictionary.setVersion("1");
		List<DeploymentDictionary> dictList = new ArrayList<DeploymentDictionary>();
		dictList.add(deploymentDictionary);
		List<String> idList = new ArrayList<String>();
		idList.add("SMS");
		Collection files = new ArrayList();
		files.add(new File("test.dqm"));
		PowerMockito.when(
				FileUtils.listFiles((File) Mockito.any(),
						(IOFileFilter) Mockito.any(),
						(IOFileFilter) Mockito.any())).thenReturn(files);
		PowerMockito.when(ModelResources.getEntityManager()).thenReturn(
				entityManager);
		Mockito.when(
				entityManager.createNamedQuery(DeploymentDictionary.FIND_DISTINCT_DEPLOYED_ENTITY,
						String.class)).thenReturn(query);
		
		Mockito.when(
				entityManager.createNamedQuery(DeploymentDictionary.FIND_BY_OBJID_VER_ENT_TYPE_ONLY,
						DeploymentDictionary.class)).thenReturn(typedQueryDeploymentDictionary);
		Mockito.when(
				query.setParameter(Mockito.anyString(), Mockito.any()))
				.thenReturn(query);
		Mockito.when(query.getResultList()).thenReturn(idList);
		
		
		Mockito.when(
				typedQueryDeploymentDictionary.setParameter(Mockito.anyString(), Mockito.any()))
				.thenReturn(typedQueryDeploymentDictionary);
		Mockito.when(typedQueryDeploymentDictionary.getResultList()).thenReturn(dictList);
		// PowerMockito.when(GetDBResource.getInstance())
		FileSystem fileSystem = mock(FileSystem.class);
		PowerMockito.when(FileSystem.get((Configuration) Mockito.any()))
				.thenReturn(fileSystem);
		GeneratorProperty dqmDirectory = new GeneratorProperty();
		dqmDirectory.setParamvalue("/mnt/staging/dqm/");
		Mockito.when(
				mockGenPropQuery
						.retrieve(GeneratorWorkFlowContext.RITHOMAS_DQM_DIR))
				.thenReturn(dqmDirectory);
		context.setProperty(JobExecutionContext.HADOOP_NAMENODE_IP, "rithomas");
		context.setProperty(JobExecutionContext.HADOOP_NAMENODE_PORT, "54310");
		context.setProperty(JobExecutionContext.RETENTIONDAYS, "30");
		Calendar calendar = Calendar.getInstance();
		Date date = new Date();
		long dateInUnixFormat = date.getTime();
		calendar.setTimeInMillis(dateInUnixFormat);
		calendar.add(Calendar.DAY_OF_MONTH, -25);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		Path path = new Path("file1");
		FileStatus fs1 = new FileStatus(0, false, 0, 0,
				calendar.getTimeInMillis(), 0, null, null, null, path);
		calendar.setTimeInMillis(dateInUnixFormat);
		calendar.add(Calendar.DAY_OF_MONTH, -35);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		FileStatus fs2 = new FileStatus(0, false, 0, 0,
				calendar.getTimeInMillis(), 0, null, null, null, path);
		FileStatus[] fsArray = { fs1, fs2 };
		PowerMockito.when(
				fileSystem.listStatus(new Path(
						ScanFilesInDQMFolder.PROCESS_MONITOR_HDFS_LOC)))
				.thenReturn(fsArray);
		Mockito.when(getDBResource.getHdfsConfiguration()).thenReturn(
				 new Configuration());
	}

	@Test
	public void testExecute() throws WorkFlowExecutionException {
		fileScanner.execute(context);
	}

	@Test
	public void testGetDQMFolder() {
		assertEquals("/mnt/staging/dqm/", fileScanner.getDQMFolder(null));
	}

	@Test
	public void testGetDeployedUsageSpecDirs() {
		assertEquals("[SMS_1/SMS_1/]",
				fileScanner.getDeployedUsageSpecDirs(context).toString());
	}
}
