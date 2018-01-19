/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.msprecovery.main;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.msprecovery.dynamodb.ItemController;
import com.msprecovery.dynamodb.TableController;
import com.msprecovery.rds.RDSConnection;
import com.msprecovery.rds.RDSController;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author jmunoz
 */
public class Main {

    private static final String tableNameDynamoDB = "msp_global_claim_funnel_jm_2";
    private static final String keyNameDynamoDB = "msp_global_funnel_id";
    private static final long readCapacity = 10;
    private static final long writeCapacity = 3000;

    //batch_1
//    private static final String hmoList = "ACO,BRSIN,CC,COV,CRPL,FHCP";
    private static final String hmoList = "BRSIN";
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {

        AmazonDynamoDB ddb = AmazonDynamoDBClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
//                .withCredentials(new ProfileCredentialsProvider("aws.properties", "jmunoz"))
                                .withCredentials(new ProfileCredentialsProvider("src/com/msprecovery/dynamodb/aws.properties", "jmunoz"))
                .build();
        try {
            System.out.println("    1: Create Table: C_T");
            System.out.println("    2: Delete Table: D_T");
            System.out.println("    3: List Table: L_T");
            System.out.println("    4: Information Table: I_T");
            System.out.println("    5: Upload Members Table: U_GUM");
            System.out.println("    6: Upload Claims Funnels Table: U_GCF");
            System.out.println("---------------------------------------");
            System.out.println("Action: ");

            BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
            String input = bufferRead.readLine();

            //Table Section
            if (input.equals("C_T")) {
                TableController.CreateTable(ddb, tableNameDynamoDB, keyNameDynamoDB, readCapacity, writeCapacity);
            }
            if (input.equals("D_T")) {
                TableController.DeleteTable(ddb, tableNameDynamoDB);
            }
            if (input.equals("L_T")) {
                TableController.ListTable(ddb);
            }
            if (input.equals("I_T")) {
                TableDescription table_info = TableController.GetTableInfo(ddb, tableNameDynamoDB);
            }
            //Custom Section
            if (input.equals("U_GUM")) {
                RDSController.SendGlobalMembersToDynamoDB(ddb, tableNameDynamoDB, keyNameDynamoDB, hmoList);
            }
            if (input.equals("U_GCF")) {
                RDSController.SendGlobalClaimsFunnelToDynamoDB(ddb, tableNameDynamoDB, keyNameDynamoDB, hmoList);
            }
        } catch (IOException ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }

//Table 
//        TableDescription table_info = TableController.GetTableInfo(ddb, tableName);
//        System.out.println("<<<<<-------------------->>>>>");
//        System.out.println(table_info.getLatestStreamArn());
//        System.out.println(table_info.getItemCount());
//        System.out.println("<<<<<-------------------->>>>>");
        //Items
//        HashMap<String, String> item_values = new HashMap<String, String>();
//        item_values.put("msp_patient_id", "101");
//        item_values.put("msp_MEMB_NAME", "Juan");
//        item_values.put("msp_MEMB_LNAME", "Puleo");
//        ItemController.CreateItems(ddb, tableName, keyName, item_values);
//        ItemController.AddItem(ddb, tableName, item_values);
//        ItemController.UpdateItem(ddb, tableName);
//        ItemController.GetItem(ddb, tableName, "Jorge");
//        ItemController.RetrieveItem(ddb);
//        SendGlobalMembersToDynamoDB(ddb);
    }
}
