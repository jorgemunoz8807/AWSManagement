/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.msprecovery.dynamodb;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import java.util.List;

/**
 *
 * @author jmunoz
 */
public class TableController {

    public static void CreateTable(AmazonDynamoDB ddb, String tableName, String keyName, long readCapacity, long writeCapacity) {

        CreateTableRequest request = new CreateTableRequest()
                .withAttributeDefinitions(new AttributeDefinition(keyName, ScalarAttributeType.S))
                .withKeySchema(new KeySchemaElement(keyName, KeyType.HASH))
                .withProvisionedThroughput(new ProvisionedThroughput(readCapacity, writeCapacity))
                .withTableName(tableName);
//        AmazonDynamoDBClient ddb = new AmazonDynamoDBClient();
        try {
            CreateTableResult result = ddb.createTable(request);
            System.out.println(result.getTableDescription());
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

    public static void DeleteTable(AmazonDynamoDB ddb, String tableName) {

        try {
            ddb.deleteTable(tableName);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

    public static void UpdateTable(String tableName, Long read_capacity, Long write_capacity) {
        ProvisionedThroughput table_throughput = new ProvisionedThroughput(read_capacity, write_capacity);

        final AmazonDynamoDBClient ddb = new AmazonDynamoDBClient();

        try {
            ddb.updateTable(tableName, table_throughput);
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

    public static void ListTable(AmazonDynamoDB ddb) {
//        final AmazonDynamoDBClient ddb = new AmazonDynamoDBClient();
        boolean more_tables = true;
        while (more_tables) {
            String last_name = null;
            try {
                ListTablesResult table_list = null;
                if (last_name == null) {
                    table_list = ddb.listTables();
                } else {
                    table_list = ddb.listTables(last_name);
                }
                List<String> tableNames = table_list.getTableNames();

                if (tableNames.size() > 0) {
                    for (String cur_name : tableNames) {
                        System.out.format("* %s\n", cur_name);
                    }
                } else {
                    System.out.println("No tables found!");
                    System.exit(0);
                }

                last_name = table_list.getLastEvaluatedTableName();
                if (last_name == null) {
                    more_tables = false;
                }
            } catch (AmazonServiceException e) {
                System.err.println(e.getErrorMessage());
                System.exit(1);
            }
        }
    }

    public static TableDescription GetTableInfo(AmazonDynamoDB ddb, String tableName) {
        try {
            TableDescription table_info
                    = ddb.describeTable(tableName).getTable();

            if (table_info != null) {
                System.out.format("Table name  : %s\n", table_info.getTableName());
                System.out.format("Table ARN   : %s\n", table_info.getTableArn());
                System.out.format("Status      : %s\n", table_info.getTableStatus());
                System.out.format("Item count  : %d\n", table_info.getItemCount().longValue());
                System.out.format("Size (bytes): %d\n", table_info.getTableSizeBytes().longValue());

                ProvisionedThroughputDescription throughput_info = table_info.getProvisionedThroughput();
                System.out.println("Throughput");
                System.out.format("  Read Capacity : %d\n", throughput_info.getReadCapacityUnits().longValue());
                System.out.format("  Write Capacity: %d\n", throughput_info.getWriteCapacityUnits().longValue());

                List<AttributeDefinition> attributes = table_info.getAttributeDefinitions();
                System.out.println("Attributes");
                for (AttributeDefinition a : attributes) {
                    System.out.format("  %s (%s)\n", a.getAttributeName(), a.getAttributeType());
                }
            }
            return table_info;
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
        return null;
    }
}
