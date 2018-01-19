/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.msprecovery.dynamodb;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import static com.amazonaws.services.dynamodbv2.model.AttributeAction.PUT;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import java.util.Set;

/**
 *
 * @author jmunoz
 */
public class ItemController {

    public static void WriteItem(AmazonDynamoDB ddb, String tableName, String keyName, Map<String, Object> infoMap) {
        DynamoDB dynamoDB = new DynamoDB(ddb);
        Table table = dynamoDB.getTable(tableName);

//        final Map<String, Object> infoMap = item_values;
        try {
//            System.out.println("Adding a new item...");
            /*PutItemOutcome outcome = */
            table.putItem(new Item()
                    .withPrimaryKey("msp_global_funnel_id", infoMap.get("msp_global_funnel_id"))
                    .withMap("info", infoMap));

//            System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());
        } catch (Exception e) {
            System.err.println("Unable to add item: " + infoMap);
            System.err.println(e.getMessage());
            System.exit(1);
        }

    }

    public static void WriteItems(DynamoDB dynamoDB, Table table, String keyName, HashMap<String, String> item_values) {
//        DynamoDB dynamoDB = new DynamoDB(ddb);
//        Table table = dynamoDB.getTable(tableName);
        try {
            Set set = item_values.entrySet();
            Iterator iter = set.iterator();
            Item item = new Item();
            while (iter.hasNext()) {
                Map.Entry mapEntry = (Map.Entry) iter.next();
                item.withString(mapEntry.getKey().toString(), mapEntry.getValue().toString());
            }
            table.putItem(item);

        } catch (Exception e) {
            System.err.println("Create items failed.");
            System.err.println(e.getMessage());
        }
    }

    private static void WriteMultipleItemsBatchWrite(DynamoDB dynamoDB, Table table) {
        try {
            // Add a new item
            TableWriteItems item1 = new TableWriteItems(table.getTableName()) // Forum
                    .withItemsToPut(new Item().withPrimaryKey("Name", "Amazon RDS").withNumber("Threads", 0));

            // Add a new item, and delete an existing item, from Thread
            // This table has a partition key and range key, so need to specify
            // both of them
            TableWriteItems item2 = new TableWriteItems(table.getTableName())
                    .withItemsToPut(new Item().withPrimaryKey("ForumName", "Amazon RDS", "Subject", "Amazon RDS Thread 1")
                            .withString("Message", "ElastiCache Thread 1 message")
                            .withStringSet("Tags", new HashSet<String>(Arrays.asList("cache", "in-memory"))))
                    .withHashAndRangeKeysToDelete("ForumName", "Subject", "Amazon S3", "S3 Thread 100");

            System.out.println("Making the request.");
            BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(item1, item2);

            do {

                // Check for unprocessed keys which could happen if you exceed
                // provisioned throughput
                Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

                if (outcome.getUnprocessedItems().size() == 0) {
                    System.out.println("No unprocessed items found");
                } else {
                    System.out.println("Retrieving the unprocessed items");
                    outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
                }

            } while (outcome.getUnprocessedItems().size() > 0);

        } catch (Exception e) {
            System.err.println("Failed to retrieve items: ");
            e.printStackTrace(System.err);
        }

    }

    public static void GetItem(AmazonDynamoDB ddb, String tableName, String key_value) {
        HashMap<String, AttributeValue> key_to_get = new HashMap<String, AttributeValue>();
        key_to_get.put("Name", new AttributeValue(key_value));

        GetItemRequest request = null;
        if (key_to_get != null) {
            request = new GetItemRequest()
                    .withKey(key_to_get)
                    .withTableName(tableName)
                    .withProjectionExpression(key_value);
        } else {
            request = new GetItemRequest()
                    .withKey(key_to_get)
                    .withTableName(tableName);
        }
//        final AmazonDynamoDBClient ddb = new AmazonDynamoDBClient();
        try {
            Map<String, AttributeValue> returned_item = ddb.getItem(request).getItem();
            if (returned_item != null) {
                Set<String> keys = returned_item.keySet();
                for (String key : keys) {
                    System.out.format("%s: %s\n",
                            key, returned_item.get(key).toString());
                }
            } else {
                System.out.format("No item found with the key %s!\n", key_value);
            }
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        }
    }

    public static void UpdateItem(AmazonDynamoDB ddb, String tableName) {
        HashMap<String, AttributeValue> item_key = new HashMap<String, AttributeValue>();
        item_key.put("Name", new AttributeValue("Jorges"));

        HashMap<String, AttributeValueUpdate> updated_values = new HashMap<String, AttributeValueUpdate>();
        //Valid Values: ADD | PUT | DELETE
        updated_values.put("Last_Name", new AttributeValueUpdate(new AttributeValue("JORGE M"), PUT));

//        for (String[] field : extra_fields) {
//            updated_values.put(field[0], new AttributeValueUpdate(
//                    new AttributeValue(field[1]), AttributeAction.PUT));
//        }
//        final AmazonDynamoDBClient ddb = new AmazonDynamoDBClient();
        try {
            ddb.updateItem(tableName, item_key, updated_values);
        } catch (ResourceNotFoundException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (AmazonServiceException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public static void RetrieveItem(AmazonDynamoDB ddb) {

        DynamoDB dynamoDB = new DynamoDB(ddb);
        Table table = dynamoDB.getTable("msp_test_jm");
        try {

            Item item = table.getItem("Name", "Jorge", "Last_Name,Date_of_Birth", null);

            System.out.println("Printing item after retrieving it....");
            System.out.println(item.toJSONPretty());

        } catch (Exception e) {
            System.err.println("GetItem failed.");
            System.err.println(e.getMessage());
        }

    }

}
