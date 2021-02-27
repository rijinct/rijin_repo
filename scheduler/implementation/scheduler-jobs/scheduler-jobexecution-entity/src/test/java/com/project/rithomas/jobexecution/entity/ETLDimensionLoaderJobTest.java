
package com.project.rithomas.jobexecution.entity;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.etl.dimension.exception.DimensionLoadingException;
import com.project.rithomas.etl.dimension.exception.ETLDimensionLoaderInitializationException;
import com.project.rithomas.etl.dimension.service.ETLDimensionLoader;
import com.project.rithomas.etl.exception.ETLServiceInitializerException;
import com.project.rithomas.etl.service.ETLServiceInitializer;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.UpdateJobStatus;
import com.project.rithomas.sdk.workflow.WorkFlowContext;
import com.project.rithomas.sdk.workflow.WorkFlowExecutionException;
import com.project.rithomas.sdk.workflow.deployment.util.ProcessBuilderHelper;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { ETLDimensionLoaderJob.class })
public class ETLDimensionLoaderJobTest {

	private ETLDimensionLoaderJob dimensionLoaderJob;

	@Mock
	private ETLServiceInitializer serviceInitializer;

	@Mock
	private ProcessBuilderHelper mockProcessBuilder;

	@Mock
	private Process mockProcess;

	@Mock
	private InputStream mockInputStream;

	@Mock
	private InputStreamReader mockInputStreamReader;

	@Mock
	private BufferedReader mockBufferedReader;

	@Mock
	private ETLDimensionLoader dimensionLoader;

	@Mock
	private UpdateJobStatus updateJobStatus;

	List<Map<String, Object>> metadataList;

	@Before
	public void setUp() {
		dimensionLoaderJob = new ETLDimensionLoaderJob();
		Map<String, Object> metadataMap = new LinkedHashMap<String, Object>();
		metadataMap.put("ADAPTATION_ID", "SMS");
		metadataMap.put("ADAPTATION_VERSION", "1");
		metadataMap.put("ENTITY_SPEC_ID", "SMS_TYPE");
		metadataMap.put("ENTITY_SPEC_VERSION", "1");
		metadataMap.put("ENTITY_SPEC_NAME", "SMS_TYPE");
		metadataMap.put("ADAP_IMPORT_DIR",
				"/mnt/staging/import/COMMON_DIMENSION_1/LOCATION_1");
		metadataMap.put("ADAP_IMPORT_DIR",
				"/mnt/staging/import/COMMON_DIMENSION_1/LOCATION_1");
		metadataList = new ArrayList<Map<String, Object>>();
		metadataList.add(metadataMap);
	}

	@Test
	public void jobShouldExecuteSuccesfully() throws Exception {
		whenNew(ETLServiceInitializer.class).withNoArguments()
				.thenReturn(serviceInitializer);
		when(serviceInitializer.retrieveMetadataOfEntitySpec(
				"Entity_SMS_TYPE_1_CorrelationJob")).thenReturn(metadataList);
		whenNew(ETLDimensionLoader.class).withNoArguments()
				.thenReturn(dimensionLoader);
		whenNew(ProcessBuilderHelper.class).withNoArguments()
				.thenReturn(mockProcessBuilder);
		when(mockProcessBuilder.executeCommand(Mockito.anyListOf(String.class),
				Mockito.any(File.class))).thenReturn(mockProcess);
		when(mockProcess.getInputStream()).thenReturn(mockInputStream);
		whenNew(InputStreamReader.class).withArguments(mockInputStream)
				.thenReturn(mockInputStreamReader);
		whenNew(BufferedReader.class).withArguments(mockInputStreamReader)
				.thenReturn(mockBufferedReader);
		when(mockBufferedReader.readLine()).thenReturn(null);
		Mockito.doNothing().when(dimensionLoader)
				.initialize(Mockito.anyMapOf(String.class, Object.class));
		Mockito.doNothing().when(dimensionLoader)
				.loadESData(new ArrayList<String>());
		Mockito.doNothing().when(dimensionLoader).closeResources();
		WorkFlowContext context = new JobExecutionContext();
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Entity_SMS_TYPE_1_CorrelationJob");
		boolean actual = dimensionLoaderJob.execute(context);
		assertEquals(true, actual);
	}

	@Test(expected = WorkFlowExecutionException.class)
	public void jobShouldFailAsETLServiceInitializerExceptionIsThrown()
			throws Exception {
		WorkFlowContext context = new JobExecutionContext();
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Entity_SMS_TYPE_1_CorrelationJob");
		ETLServiceInitializerException serviceInitializerException = new ETLServiceInitializerException(
				"Some Error");
		whenNew(ETLServiceInitializer.class).withNoArguments()
				.thenReturn(serviceInitializer);
		whenNew(ETLDimensionLoader.class).withNoArguments()
				.thenReturn(dimensionLoader);
		whenNew(UpdateJobStatus.class).withNoArguments()
				.thenReturn(updateJobStatus);
		Mockito.when(serviceInitializer.retrieveMetadataOfEntitySpec(
				"Entity_SMS_TYPE_1_CorrelationJob"))
				.thenThrow(serviceInitializerException);
		Mockito.doNothing().when(updateJobStatus).updateETLErrorStatusInTable(
				serviceInitializerException, context);
		Mockito.doNothing().when(dimensionLoader).closeResources();
		dimensionLoaderJob.execute(context);
	}

	@SuppressWarnings("unchecked")
	@Test(expected = WorkFlowExecutionException.class)
	public void jobShouldFailAsETLDimensionLoaderInitializationExceptionIsThrown()
			throws Exception {
		WorkFlowContext context = new JobExecutionContext();
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Entity_SMS_TYPE_1_CorrelationJob");
		ETLDimensionLoaderInitializationException dimLoaderInitializerException = new ETLDimensionLoaderInitializationException(
				"Some Error");
		whenNew(ETLServiceInitializer.class).withNoArguments()
				.thenReturn(serviceInitializer);
		whenNew(ETLDimensionLoader.class).withNoArguments()
				.thenReturn(dimensionLoader);
		whenNew(UpdateJobStatus.class).withNoArguments()
				.thenReturn(updateJobStatus);
		Mockito.when(serviceInitializer.retrieveMetadataOfEntitySpec(
				"Entity_SMS_TYPE_1_CorrelationJob")).thenReturn(metadataList);
		Mockito.doThrow(dimLoaderInitializerException).when(dimensionLoader)
				.initialize(Mockito.anyMapOf(String.class, Object.class));
		Mockito.doNothing().when(updateJobStatus).updateETLErrorStatusInTable(
				dimLoaderInitializerException, context);
		Mockito.doNothing().when(dimensionLoader).closeResources();
		dimensionLoaderJob.execute(context);
	}

	@SuppressWarnings("unchecked")
	@Test(expected = WorkFlowExecutionException.class)
	public void jobShouldFailAsDimensionLoadingExceptionIsThrown()
			throws Exception {
		WorkFlowContext context = new JobExecutionContext();
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Entity_SMS_TYPE_1_CorrelationJob");
		DimensionLoadingException dimLoadingException = new DimensionLoadingException(
				"Some Error", null);
		whenNew(ETLServiceInitializer.class).withNoArguments()
				.thenReturn(serviceInitializer);
		whenNew(ETLDimensionLoader.class).withNoArguments()
				.thenReturn(dimensionLoader);
		whenNew(UpdateJobStatus.class).withNoArguments()
				.thenReturn(updateJobStatus);
		Mockito.when(serviceInitializer.retrieveMetadataOfEntitySpec(
				"Entity_SMS_TYPE_1_CorrelationJob")).thenReturn(metadataList);
		Mockito.doNothing().when(dimensionLoader)
				.initialize(Mockito.anyMapOf(String.class, Object.class));
		Mockito.doThrow(dimLoadingException).when(dimensionLoader)
				.loadESData(new ArrayList<String>());
		Mockito.doNothing().when(updateJobStatus)
				.updateETLErrorStatusInTable(dimLoadingException, context);
		Mockito.doNothing().when(dimensionLoader).closeResources();
		dimensionLoaderJob.execute(context);
	}

	@SuppressWarnings("unchecked")
	@Test(expected = WorkFlowExecutionException.class)
	public void jobShouldFailAsWorkFlowExecutionExceptionIsThrown()
			throws Exception {
		WorkFlowContext context = new JobExecutionContext();
		context.setProperty(JobExecutionContext.JOB_NAME,
				"Entity_SMS_TYPE_1_CorrelationJob");
		WorkFlowExecutionException workflowExecutionException = new WorkFlowExecutionException(
				"Some Error", null);
		DimensionLoadingException dimLoadingException = new DimensionLoadingException(
				"Some Error", null);
		whenNew(ETLServiceInitializer.class).withNoArguments()
				.thenReturn(serviceInitializer);
		whenNew(ETLDimensionLoader.class).withNoArguments()
				.thenReturn(dimensionLoader);
		whenNew(UpdateJobStatus.class).withNoArguments()
				.thenReturn(updateJobStatus);
		Mockito.when(serviceInitializer.retrieveMetadataOfEntitySpec(
				"Entity_SMS_TYPE_1_CorrelationJob")).thenReturn(metadataList);
		Mockito.doNothing().when(dimensionLoader)
				.initialize(Mockito.anyMapOf(String.class, Object.class));
		Mockito.doThrow(dimLoadingException).when(dimensionLoader)
				.loadESData(new ArrayList<String>());
		Mockito.doThrow(workflowExecutionException).when(updateJobStatus)
				.updateETLErrorStatusInTable(dimLoadingException, context);
		Mockito.doNothing().when(dimensionLoader).closeResources();
		dimensionLoaderJob.execute(context);
	}
	
	
	@Test
	public void testValidateFileExtension() throws Exception {
		File file = new File(this.getClass().getClassLoader()
				.getResource("SubscriberGroup_2019.10.22-10.06.38_001.csv")
				.getFile());
		String filePath = file.getAbsolutePath();
		Method method = ETLDimensionLoaderJob.class
				.getDeclaredMethod("validateFileExtension", String.class);
		method.setAccessible(true);
		boolean returnValue = (boolean) method.invoke(dimensionLoaderJob,
				FilenameUtils.getFullPathNoEndSeparator(filePath));
		assertEquals(returnValue, false);
	}
	
	@Test
	public void testCheckPresenceOfEncodingScript() throws Exception {
		File file = new File(this.getClass().getClassLoader()
				.getResource("SubscriberGroup_2019.10.22-10.06.38_001.csv")
				.getFile());
		String filePath = file.getAbsolutePath();
		Method method = ETLDimensionLoaderJob.class.getDeclaredMethod(
				"checkPresenceOfEncodingScript", String.class);
		method.setAccessible(true);
		boolean returnValue = (boolean) method.invoke(dimensionLoaderJob,
				filePath);
		assertEquals(returnValue, true);
	}
}
