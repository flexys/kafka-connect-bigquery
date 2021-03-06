package com.wepay.kafka.connect.bigquery.schemaregistry.schemaretriever;

import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;

import com.wepay.kafka.connect.bigquery.api.TopicAndRecordName;
import io.confluent.connect.avro.AvroData;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import org.apache.avro.Schema.Parser;

import org.apache.kafka.connect.data.Schema;

import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.Map;

/**
 * Uses the Confluent Schema Registry to fetch the latest schema for a given topic.
 */
public class SchemaRegistrySchemaRetriever implements SchemaRetriever {
  private static final Logger logger = LoggerFactory.getLogger(SchemaRegistrySchemaRetriever.class);

  private SchemaRegistryClient schemaRegistryClient;
  private AvroData avroData;

  /**
   * Only here because the package-private constructor (which is only used in testing) would
   * otherwise cover up the no-args constructor.
   */
  public SchemaRegistrySchemaRetriever() {
  }

  // For testing purposes only
  SchemaRegistrySchemaRetriever(SchemaRegistryClient schemaRegistryClient, AvroData avroData) {
    this.schemaRegistryClient = schemaRegistryClient;
    this.avroData = avroData;
  }

  @Override
  public void configure(Map<String, String> properties) {
    SchemaRegistrySchemaRetrieverConfig config =
        new SchemaRegistrySchemaRetrieverConfig(properties);
    Map<String, ?> schemaRegistryClientProperties =
        config.originalsWithPrefix(config.SCHEMA_REGISTRY_CLIENT_PREFIX);
    schemaRegistryClient = new CachedSchemaRegistryClient(
        config.getString(config.LOCATION_CONFIG),
        0,
        schemaRegistryClientProperties
    );
    avroData = new AvroData(config.getInt(config.AVRO_DATA_CACHE_SIZE_CONFIG));
  }

  @Override
  public Schema retrieveSchema(TableId table, TopicAndRecordName topicAndRecordName, KafkaSchemaRecordType schemaType) throws ConnectException {
    String topic = topicAndRecordName.getTopic();
    String subject = getSubject(topicAndRecordName, schemaType);
    logger.debug("Retrieving schema information for topic {} with subject {} and schema type {}", topic, subject, schemaType);
    try {
      SchemaMetadata latestSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
      org.apache.avro.Schema avroSchema = new Parser().parse(latestSchemaMetadata.getSchema());
      return avroData.toConnectSchema(avroSchema);
    } catch (IOException | RestClientException exception) {
      throw new ConnectException(String.format(
          "Exception while fetching latest schema metadata for topic=%s, subject=%s",
          topic, subject),
          exception
      );
    }
  }

  @Override
  public void setLastSeenSchema(TableId table, TopicAndRecordName topicAndRecordName, Schema schema) {
  }

  /**
   * Convert topic and record name into the schema registry subject.
   * Subjects follow topic-recordName format.
   *
   * If recordName is not present, "value" or "key" will be used, depending on the schema type.
   *
   * @param schemaType schema type used to resolve full subject when recordName is absent.
   * @return corresponding schema registry subject.
   */
  private String getSubject(TopicAndRecordName topicAndRecordName, KafkaSchemaRecordType schemaType) {
    String topic = topicAndRecordName.getTopic();
    String subjectPostfix = topicAndRecordName.getRecordName().orElseGet(schemaType::toString);
    return topic + "-" + subjectPostfix;
  }

}
