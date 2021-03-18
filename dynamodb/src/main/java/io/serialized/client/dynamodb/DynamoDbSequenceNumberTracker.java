package io.serialized.client.dynamodb;


import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBSaveExpression;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ConditionalOperator;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import io.serialized.client.feed.SequenceNumberTracker;
import org.apache.commons.lang3.Validate;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;

public class DynamoDbSequenceNumberTracker implements SequenceNumberTracker {

  private final AmazonDynamoDB dynamoDB;
  private final String tableName;
  private final String trackerName;
  private final DynamoDBMapper dynamoDBMapper;

  public DynamoDbSequenceNumberTracker(AmazonDynamoDB dynamoDB, String tableName, String trackerName) {
    this.dynamoDB = dynamoDB;
    this.dynamoDBMapper = new DynamoDBMapper(dynamoDB,
        DynamoDBMapperConfig.builder().withTableNameResolver((clazz, config) -> tableName).build()
    );
    this.tableName = tableName;
    this.trackerName = trackerName;
    init();
  }

  protected void init() {
    try {
      TableUtils.createTableIfNotExists(dynamoDB, generateCreateTableRequest());
      TableUtils.waitUntilExists(dynamoDB, tableName);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  protected CreateTableRequest generateCreateTableRequest() {
    return dynamoDBMapper.generateCreateTableRequest(Tracker.class)
        .withProvisionedThroughput(new ProvisionedThroughput(1L, 1L));
  }

  @Override
  public long lastConsumedSequenceNumber() {
    Validate.isTrue(dynamoDBMapper != null, "DynamoDBMapper not initialized");
    Tracker tracker = dynamoDBMapper.load(Tracker.class, trackerName);
    return Optional.ofNullable(tracker).map(t -> t.sequenceNumber).orElse(0L);
  }

  @Override
  public void updateLastConsumedSequenceNumber(long sequenceNumber) {
    Validate.isTrue(dynamoDBMapper != null, "DynamoDBMapper not initialized");

    Validate.isTrue(sequenceNumber >= 0,
        "Illegal sequenceNumber [%d] - last consumed sequence number cannot be negative", sequenceNumber);

    try {
      Map<String, ExpectedAttributeValue> expected = new HashMap<>();
      expected.put("name", new ExpectedAttributeValue().withExists(false));
      expected.put("sequenceNumber", new ExpectedAttributeValue(new AttributeValue().withN(String.valueOf(sequenceNumber)))
          .withComparisonOperator(ComparisonOperator.LT));

      dynamoDBMapper.save(new Tracker(trackerName, sequenceNumber), new DynamoDBSaveExpression()
          .withExpected(expected)
          .withConditionalOperator(ConditionalOperator.OR));

    } catch (ConditionalCheckFailedException e) {
      throw new IllegalArgumentException(
          format("Illegal sequenceNumber [%d] - last consumed sequence number must be greater than current",
              sequenceNumber));
    }

  }

  @Override
  public void reset() {
    dynamoDBMapper.save(new Tracker(trackerName, 0));
  }

  @SuppressWarnings("unused")
  public static class Tracker {

    @DynamoDBHashKey(attributeName = "name")
    private String name;

    @DynamoDBAttribute(attributeName = "sequenceNumber")
    private long sequenceNumber;

    public Tracker() {
    }

    public Tracker(String name, long sequenceNumber) {
      this.name = name;
      this.sequenceNumber = sequenceNumber;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public long getSequenceNumber() {
      return sequenceNumber;
    }

    public void setSequenceNumber(long sequenceNumber) {
      this.sequenceNumber = sequenceNumber;
    }

  }

}
