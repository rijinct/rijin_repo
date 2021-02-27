
package com.project.rithomas.jobexecution.reaggregation;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.project.rithomas.jobexecution.common.exception.JobExecutionException;
import com.project.rithomas.jobexecution.common.util.DateFunctionTransformation;
import com.project.rithomas.jobexecution.common.util.JobExecutionContext;
import com.project.rithomas.jobexecution.common.util.QueryExecutor;
import com.project.rithomas.sdk.workflow.WorkFlowContext;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ReaggregationSourceConfiguration.class, QueryExecutor.class,
		DateFunctionTransformation.class, ArchivedPartitionHelper.class })
public class ReaggregationQueryBuilderTest {

	private static final String SRC_TEST_RESOURCES = "src/test/resources/";

	@Mock
	private WorkFlowContext mockContext;

	@Mock
	private QueryExecutor mockQueryExecutor;

	@Mock
	private DateFunctionTransformation mockDateFunctionTransformation;

	ReaggregationQueryBuilder queryBuilder = new ReaggregationQueryBuilder();

	@Before
	public void setUp() throws Exception {
		PowerMockito.mockStatic(DateFunctionTransformation.class);
		PowerMockito.whenNew(QueryExecutor.class).withNoArguments()
				.thenReturn(mockQueryExecutor);
		PowerMockito.when(DateFunctionTransformation.getInstance())
				.thenReturn(mockDateFunctionTransformation);
	}

	@Test
	public void test_updateQueryWithSingleSourceArchivedPartitionsAnd5MinPartitionType()
			throws Exception {
		Mockito.when(
				mockContext.getProperty(JobExecutionContext.SOURCE_TABLE_NAME))
				.thenReturn("US_VOICE_1");
		String archiveDaysQuery = "select paramvalue from job_prop where jobid in (select id from job_dictionary where jobid = 'Usage_VOICE_1_ArchivingJob') and paramname = 'ARCHIVING_DAYS';";
		Mockito.when(mockQueryExecutor.executeMetadatasqlQuery(archiveDaysQuery,
				mockContext)).thenReturn(new String[] { "1" });
		String partitionTypeQuery = "select paramvalue from runtime_prop where paramname = 'ARCHIVED_PARTITION_TYPE';";
		Mockito.when(mockQueryExecutor
				.executeMetadatasqlQuery(partitionTypeQuery, mockContext))
				.thenReturn(new String[] { "5MIN" });
		String runtimePropArchivingDaysQuery = "select paramvalue from runtime_prop where paramname = 'ARCHIVED_DAYS';";
		Mockito.when(mockQueryExecutor.executeMetadatasqlQuery(
				runtimePropArchivingDaysQuery, mockContext))
				.thenReturn(new String[] { "1" });
		String reportTimeQuery = "select paramvalue from runtime_prop where paramname = 'ARCHIVED_PARTITION_TYPE'";
		Mockito.when(mockQueryExecutor.executeMetadatasqlQuery(reportTimeQuery,
				mockContext)).thenReturn(new String[] { "5MIN" });
		Mockito.when(mockDateFunctionTransformation
				.getTruncatedDateAfterSubtractingDays(1))
				.thenReturn(1556994600000l);
		Mockito.when(mockContext.getProperty("Usage_VOICE_1_LoadJob_PLEVEL"))
				.thenReturn("5MIN");
		Mockito.when(
				mockDateFunctionTransformation.getTrunc(1556735400000l, "DAY"))
				.thenReturn(1556735400000l);
		Mockito.when(mockDateFunctionTransformation.getNextBound(1556735400000l,
				"DAY")).thenCallRealMethod();
		Long lowerBoundInMillis = 1556735400000l;
		Long nextLb = 1556821800000l;
		List<String> sourceJobs = Arrays.asList("Usage_VOICE_1_LoadJob");
		Path path = Paths.get(SRC_TEST_RESOURCES + "single_source.txt");
		ReaggregationSourceConfiguration configuration = new ReaggregationSourceConfiguration();
		configuration.setContext(mockContext).setLowerBound(lowerBoundInMillis)
				.setNextLb(nextLb).setSourceJobs(sourceJobs);
		queryBuilder.setConfiguration(configuration);
		String sql = Files.readAllLines(path).get(0);
		String actual = queryBuilder.setSql(sql).update();
		Path expectedResponsePath = Paths.get(SRC_TEST_RESOURCES
				+ "expected_SingleSourceArchivedPartitionsAnd5MinPartitionType.txt");
		String expected = Files.readAllLines(expectedResponsePath).get(0);
		assertEquals(expected, actual);
	}

	@Test
	public void test_updateQueryWithSingleSourceUnArchivedPartitions()
			throws IOException, JobExecutionException {
		Mockito.when(
				mockContext.getProperty(JobExecutionContext.SOURCE_TABLE_NAME))
				.thenReturn("US_VOICE_1");
		String reportTimeQuery = "select paramvalue from runtime_prop where paramname = 'ARCHIVED_PARTITION_TYPE';";
		Mockito.when(mockQueryExecutor.executeMetadatasqlQuery(reportTimeQuery,
				mockContext)).thenReturn(new String[] { "5MIN" });
		String runtimePropArchivingDaysQuery = "select paramvalue from runtime_prop where paramname = 'ARCHIVED_DAYS';";
		Mockito.when(mockQueryExecutor.executeMetadatasqlQuery(
				runtimePropArchivingDaysQuery, mockContext))
				.thenReturn(new String[] { "0" });
		Mockito.when(mockDateFunctionTransformation
				.getTruncatedDateAfterSubtractingDays(1))
				.thenReturn(1556994600000l);
		Mockito.when(mockContext.getProperty("Usage_VOICE_1_LoadJob_PLEVEL"))
				.thenReturn("5MIN");
		
		Mockito.when(mockDateFunctionTransformation
				.getTruncatedDateAfterSubtractingDays(1))
				.thenReturn(1556994600000l);
		
		Long lowerBoundInMillis = 1556735400000l;
		Long nextLb = 1556821800000l;
		List<String> sourceJobs = Arrays.asList("Usage_VOICE_1_LoadJob");
		Path path = Paths.get(SRC_TEST_RESOURCES + "single_source.txt");
		ReaggregationSourceConfiguration configuration = new ReaggregationSourceConfiguration();
		configuration.setContext(mockContext).setLowerBound(lowerBoundInMillis)
				.setNextLb(nextLb).setSourceJobs(sourceJobs);
		queryBuilder.setConfiguration(configuration);
		String sql = Files.readAllLines(path).get(0);
		String actual = queryBuilder.setSql(sql).update();
		Path expectedResponsePath = Paths.get(SRC_TEST_RESOURCES
				+ "expected_SingleSourceUnArchivedPartitions.txt");
		String expected = Files.readAllLines(expectedResponsePath).get(0);
		assertEquals(expected, actual);
	}
	
	@Test
	public void test_updateQueryWithMultiSourceArchivedPartitionsAnd5MinPartitionType() throws IOException, JobExecutionException {
		String reportTimeQuery = "select paramvalue from runtime_prop where paramname = 'ARCHIVED_PARTITION_TYPE';";
		Mockito.when(mockQueryExecutor.executeMetadatasqlQuery(reportTimeQuery,
				mockContext)).thenReturn(new String[] { "5MIN" });
		
		String runtimePropArchivingDaysQuery = "select paramvalue from runtime_prop where paramname = 'ARCHIVED_DAYS';";
		Mockito.when(mockQueryExecutor.executeMetadatasqlQuery(
				runtimePropArchivingDaysQuery, mockContext))
				.thenReturn(new String[] { "1" });
		
		Mockito.when(mockDateFunctionTransformation
				.getTruncatedDateAfterSubtractingDays(1))
				.thenReturn(1556994600000l);
		
		Mockito.when(mockContext.getProperty("Usage_BB_APPLICATIONS_1_LoadJob_PLEVEL")).thenReturn("5MIN");
		Mockito.when(mockContext.getProperty("Usage_BB_BROWSING_1_LoadJob_PLEVEL")).thenReturn("5MIN");
		Mockito.when(mockContext.getProperty("Usage_BB_STREAMING_1_LoadJob_PLEVEL")).thenReturn("5MIN");
		
		Mockito.when(
				mockDateFunctionTransformation.getTrunc(1556735400000l, "DAY"))
				.thenReturn(1556735400000l);
		Mockito.when(mockDateFunctionTransformation.getNextBound(1556735400000l,
				"DAY")).thenCallRealMethod();
		
		Long lowerBoundInMillis = 1556735400000l;
		Long nextLb = 1556821800000l;
		List<String> sourceJobs = Arrays.asList("Usage_BB_APPLICATIONS_1_LoadJob","Usage_BB_BROWSING_1_LoadJob","Usage_BB_STREAMING_1_LoadJob");
		ReaggregationSourceConfiguration configuration = new ReaggregationSourceConfiguration();
		configuration.setContext(mockContext).setLowerBound(lowerBoundInMillis)
		.setNextLb(nextLb).setSourceJobs(sourceJobs);
		queryBuilder.setConfiguration(configuration );
		
		Path path = Paths.get(SRC_TEST_RESOURCES + "multi_source.txt");
		String sql = Files.readAllLines(path).get(0);
		String actual = queryBuilder.setSql(sql).update();
		Path expectedResponsePath = Paths.get(SRC_TEST_RESOURCES
				+ "expected_MultiSourceArchivedPartitionsAnd5MinPartitionType.txt");
		String expected = Files.readAllLines(expectedResponsePath).get(0);
		assertEquals(expected, actual);
	}
}
