package com.wepay.kafka.connect.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;

import com.wepay.kafka.connect.bigquery.write.row.AdaptiveBigQueryWriter;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for managing Schemas of BigQuery tables (creating and updating).
 */
public class SchemaManager {
  private static final Logger logger = LoggerFactory.getLogger(SchemaManager.class);

  private final SchemaRetriever schemaRetriever;
  private final SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter;
  private final BigQuery bigQuery;

  /**
   * @param schemaRetriever Used to determine the Kafka Connect Schema that should be used for a
   *                        given table.
   * @param schemaConverter Used to convert Kafka Connect Schemas into BigQuery format.
   * @param bigQuery Used to communicate create/update requests to BigQuery.
   */
  public SchemaManager(
      SchemaRetriever schemaRetriever,
      SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter,
      BigQuery bigQuery) {
    this.schemaRetriever = schemaRetriever;
    this.schemaConverter = schemaConverter;
    this.bigQuery = bigQuery;
  }

  /**
   * Create a new table in BigQuery.
   * @param table The BigQuery table to create.
   * @param topic The Kafka topic used to determine the schema.
   */
  public void createTable(TableId table, String topic) {
    Schema keySchema = schemaRetriever.retrieveSchema(table, topic, true);
    Schema valueSchema = schemaRetriever.retrieveSchema(table, topic, false);
    bigQuery.create(constructTableInfo(table, keySchema, valueSchema));
  }

  /**
   * Update an existing table in BigQuery.
   * @param table The BigQuery table to update.
   * @param topic The Kafka topic used to determine the schema.
   */
  public void updateSchema(TableId table, String topic) {
    Schema keySchema = schemaRetriever.retrieveSchema(table, topic, true);
    Schema valueSchema = schemaRetriever.retrieveSchema(table, topic, false);
    TableInfo tableInfo = constructTableInfo(table, keySchema, valueSchema);
    logger.info("Attempting to update table `{}` with schema {}",
        table, tableInfo.getDefinition().getSchema());
    bigQuery.update(tableInfo);
  }

  // package private for testing.
  TableInfo constructTableInfo(TableId table, Schema keySchema, Schema valueSchema) {
    com.google.cloud.bigquery.Schema bigQuerySchema =
        schemaConverter.convertSchema(keySchema, valueSchema);
    StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
        .setSchema(bigQuerySchema)
        .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
        .build();
    TableInfo.Builder tableInfoBuilder =
        TableInfo.newBuilder(table, tableDefinition);
    if (valueSchema.doc() != null) {
      tableInfoBuilder.setDescription(valueSchema.doc());
    }
    return tableInfoBuilder.build();
  }
}
