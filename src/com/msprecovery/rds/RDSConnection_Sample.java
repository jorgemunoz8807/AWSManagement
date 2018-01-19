/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.msprecovery.rds;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 *
 * @author jmunoz
 */
public class RDSConnection_Sample {

    public final static Connection RDS() throws Exception {

        String nameDB = "";
        String usser = "";
        String pass = "";
        int port = 3306;
        String url = "";

        Connection connection = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            String servidor = "jdbc:mysql://" + url + ":" + port + "/" + nameDB;
            connection = DriverManager.getConnection(servidor, usser, pass);
//            System.out.println("Connected to: " + nameDB);
        } catch (Exception ex) {
            ex.getMessage();
//            System.out.println(ex.getMessage());
            connection = null;
        } finally {
            return connection;
        }
    }
}
