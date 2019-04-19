package nullren.global.table.hack.dynamodb.preader;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import nullren.global.table.hack.dynamodb.DynamoDBClient;
import nullren.global.table.hack.dynamodb.DynamoDBFibonacciRetryer;
import nullren.global.table.hack.dynamodb.filter.DynamoDBQueryFilter;
import nullren.global.table.hack.dynamodb.split.DynamoDBSegmentsSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public final class ScanRecordReadRequestTest {

  @Mock
  DynamoDBRecordReaderContext context;
  @Mock
  DynamoDBClient client;

  @Test
  public void fetchPageReturnsZeroConsumedCapacityWhenResultsConsumedCapacityIsNull() {
    DynamoDBFibonacciRetryer.RetryResult
      stubbedResult = new DynamoDBFibonacciRetryer.RetryResult<>(new ScanResult().withConsumedCapacity(null)
        .withItems(new HashMap<String, AttributeValue>()), 0);
    stubScanTableWith(stubbedResult);

    when(context.getClient()).thenReturn(client);
    when(context.getConf()).thenReturn(new JobConf());
    when(context.getSplit()).thenReturn(new DynamoDBSegmentsSplit());
    ScanReadManager readManager = Mockito.mock(ScanReadManager.class);
    ScanRecordReadRequest readRequest = new ScanRecordReadRequest(readManager, context, 0, null);
    PageResults<Map<String, AttributeValue>> pageResults =
        readRequest.fetchPage(new RateController.RequestLimit(0, 0));
    assertEquals(0.0, pageResults.consumedRcu, 0.0);
  }

  private void stubScanTableWith(DynamoDBFibonacciRetryer.RetryResult<ScanResult> scanResultRetryResult) {
    when(client.scanTable(
        anyString(),
        any(DynamoDBQueryFilter.class),
        anyInt(),
        anyInt(),
        any(Map.class),
        anyLong(),
        any(Reporter.class))
    ).thenReturn(scanResultRetryResult);
  }

}
