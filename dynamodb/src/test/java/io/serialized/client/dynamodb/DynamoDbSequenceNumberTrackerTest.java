package io.serialized.client.dynamodb;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.local.embedded.DynamoDBEmbedded;
import io.serialized.client.feed.SequenceNumberTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DynamoDbSequenceNumberTrackerTest {

  private static final String TABLE_NAME = "test_StateTrackerTable";
  private static final String TRACKER_NAME = "test_trackerName";

  private SequenceNumberTracker sequenceNumberTracker;

  @BeforeEach
  void setUp() {
    AmazonDynamoDB dynamoDB = DynamoDBEmbedded.create().amazonDynamoDB();
    sequenceNumberTracker = new DynamoDbSequenceNumberTracker(dynamoDB, TABLE_NAME, TRACKER_NAME);
  }

  @Test
  public void testSequenceNumber() {
    assertThat(sequenceNumberTracker.lastConsumedSequenceNumber()).isEqualTo(0);

    sequenceNumberTracker.updateLastConsumedSequenceNumber(1);
    assertThat(sequenceNumberTracker.lastConsumedSequenceNumber()).isEqualTo(1);

    sequenceNumberTracker.updateLastConsumedSequenceNumber(100_000_000);
    assertThat(sequenceNumberTracker.lastConsumedSequenceNumber()).isEqualTo(100_000_000);

    sequenceNumberTracker.reset();
    assertThat(sequenceNumberTracker.lastConsumedSequenceNumber()).isEqualTo(0);
  }

  @Test
  public void testUpdateMany() {
    assertThat(sequenceNumberTracker.lastConsumedSequenceNumber()).isEqualTo(0);

    int iterations = 100;

    for (int i = 0; i <= iterations; ++i) {
      sequenceNumberTracker.updateLastConsumedSequenceNumber(i);
      assertThat(sequenceNumberTracker.lastConsumedSequenceNumber()).isEqualTo(i);
    }
    assertThat(sequenceNumberTracker.lastConsumedSequenceNumber()).isEqualTo(iterations);
  }

  @Test
  public void testSequenceNumberMustBePositive() {
    Throwable exception = assertThrows(IllegalArgumentException.class,
        () -> sequenceNumberTracker.updateLastConsumedSequenceNumber(-1));

    assertThat(exception.getMessage()).isEqualTo("Illegal sequenceNumber [-1] - last consumed sequence number cannot be negative");
  }

  @Test
  public void testSequenceNumberMustNotBeEqualToCurrent() {
    sequenceNumberTracker.updateLastConsumedSequenceNumber(10);

    Throwable exception = assertThrows(IllegalArgumentException.class,
        () -> sequenceNumberTracker.updateLastConsumedSequenceNumber(10));

    assertThat(exception.getMessage()).isEqualTo("Illegal sequenceNumber [10] - last consumed sequence number must be greater than current");
  }

  @Test
  public void testSequenceNumberMustBeGreaterThanCurrent() {
    sequenceNumberTracker.updateLastConsumedSequenceNumber(10);

    Throwable exception = assertThrows(IllegalArgumentException.class,
        () -> sequenceNumberTracker.updateLastConsumedSequenceNumber(9));

    assertThat(exception.getMessage()).isEqualTo("Illegal sequenceNumber [9] - last consumed sequence number must be greater than current");
  }

}
