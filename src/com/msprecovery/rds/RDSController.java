/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.msprecovery.rds;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.msprecovery.dynamodb.ItemController;
import com.msprecovery.main.Main;
import static com.msprecovery.main.test.method1;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author jmunoz
 */
public class RDSController {

    public static void SendGlobalMembersToDynamoDB(AmazonDynamoDB ddb, String tableNameDynamoDB, String keyNameDynamoDB, String hmoList) {
        DynamoDB dynamoDB = new DynamoDB(ddb);
        Table table = dynamoDB.getTable(tableNameDynamoDB);

        int batchSize = 5000;
        String[] hmo = hmoList.split(",");
        for (int i = 0; i < hmo.length; i++) {
            System.out.println(hmo[i]);
            try {
                PreparedStatement pstmt = null;
                ResultSet rs = null;
                Connection rds = RDSConnection.RDS();
                pstmt = rds.prepareStatement("SELECT COUNT(msp_patient_id)  FROM `MSP_MASTER_MBRS`.`msp_global_unique_mbrs` WHERE msp_hmo = '" + hmo[i] + "'");
                rs = (ResultSet) pstmt.executeQuery();
                int totalRecord = 0;
                while (rs.next()) {
                    totalRecord = rs.getInt(1);
                }

                for (int j = 0; j < totalRecord; j += batchSize) {
                    pstmt = rds.prepareStatement(RDSQuery.GET_GLOBAL_UNIQUE_MBRS_BY_HMO(hmo[i], j, batchSize));
                    rs = (ResultSet) pstmt.executeQuery();
                    ResultSetMetaData metadata = rs.getMetaData();
                    int columnCount = metadata.getColumnCount();

                    boolean flag = true;

                    while (rs.next()) {
//                        for (String columnName : columns) {
                        HashMap<String, String> item_values = new HashMap<String, String>();
                        for (int k = 1; k < columnCount; k++) {
                            String columnName = metadata.getColumnName(k);
                            String value = String.valueOf(rs.getObject(columnName));
//                            System.out.println(columnName + ": " + value);
                            if (value != "" && value != null && value != "null" && !value.isEmpty() && !value.equals("")) {
                                item_values.put(columnName, value);
//                                System.out.println(columnName + ": " + value);
                            }
                        }
//                        new Thread(() -> ItemController.WriteItems(ddb, tableNameDynamoDB, keyNameDynamoDB, item_values)).start();
                        ItemController.WriteItems(dynamoDB, table, keyNameDynamoDB, item_values);
                    }
//                    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
//                    Date date = new Date();
                    System.out.println(/*dateFormat.format(date)*/new Date() + "      " + Math.addExact(j, batchSize) + " from: " + totalRecord);
                }
            } catch (SQLException ex) {
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            } catch (Exception ex) {
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

    public static void SendGlobalClaimsFunnelToDynamoDB(AmazonDynamoDB ddb, String tableNameDynamoDB, String keyNameDynamoDB, String hmoList) {
        DynamoDB dynamoDB = new DynamoDB(ddb);
        Table table = dynamoDB.getTable(tableNameDynamoDB);

        int batchSize = 5000;
        String[] hmo = hmoList.split(",");
        for (int i = 0; i < hmo.length; i++) {
            System.out.println(hmo[i]);
            try {
                PreparedStatement pstmt = null;
                ResultSet rs = null;
                Connection rds = RDSConnection.RDS();
                pstmt = rds.prepareStatement("SELECT COUNT(msp_frd_id)  FROM `MSP_MASTER_FUNNELS`.`msp_funnel_" + hmo[i].toLowerCase() + "`");
                rs = (ResultSet) pstmt.executeQuery();
                int totalRecord = 0;
                while (rs.next()) {
                    totalRecord = rs.getInt(1);
                }
                for (int j = 0; j < totalRecord; j += batchSize) {
                    pstmt = rds.prepareStatement(RDSQuery.GET_GLOBAL_CLAIMS_FUNNELS_BY_HMO(hmo[i], j, batchSize));
                    rs = (ResultSet) pstmt.executeQuery();
                    ResultSetMetaData metadata = rs.getMetaData();
                    int columnCount = metadata.getColumnCount();

                    boolean flag = true;

                    while (rs.next()) {
//                        for (String columnName : columns) {
                        HashMap<String, String> item_values = new HashMap<String, String>();
//                        Map<String, Object> item_values = new HashMap<String, Object>();
                        for (int k = 1; k < columnCount; k++) {
                            String columnName = metadata.getColumnName(k);
                            String value = String.valueOf(rs.getObject(columnName));
//                            System.out.println(columnName + ": " + value);
                            if (value != "" && value != null && value != "null" && !value.isEmpty() && !value.equals("")) {
                                item_values.put(columnName, value);
//                                System.out.println(columnName + ": " + value);
                            }
                        }
//                        System.out.println(Runtime.getRuntime().freeMemory());
//                        new Thread(() -> ItemController.WriteItems(ddb, tableNameDynamoDB, keyNameDynamoDB, item_values)).start();
//                        ItemController.WriteItems(ddb, tableNameDynamoDB, keyNameDynamoDB, item_values);
                        ItemController.WriteItems(dynamoDB, table, keyNameDynamoDB, item_values);

                    }
//                    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
//                    Date date = new Date();
                    System.out.println(/*dateFormat.format(date)*/new Date() + "      " + Math.addExact(j, batchSize) + " from: " + totalRecord);
                }
            } catch (SQLException ex) {
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            } catch (Exception ex) {
                Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

}
