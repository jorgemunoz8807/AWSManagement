/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.msprecovery.s3;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import java.io.File;
import java.util.List;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 *
 * @author jmunoz
 */
public class S3Controller {

    private String accessKey = "";
    private String secretKey = "";

    public static AmazonS3 S3Connect(String accessKey, String secretKey) {
        //Creating a connection        
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        ClientConfiguration clientConfig = new ClientConfiguration();
        clientConfig.setProtocol(Protocol.HTTPS);
        AmazonS3 connSFTP = new AmazonS3Client(credentials, clientConfig);
        return connSFTP;
    }

    public static void S3BucketList(AmazonS3 connSFTP) {
        //Listing owned buckets
        List<Bucket> buckets = connSFTP.listBuckets();
        for (Bucket bucket : buckets) {
            System.out.println(bucket.getName());
        }
    }

    public static void S3CreateFolder(AmazonS3 connSFTP, String bucketName, String pathFolder, String foldername) {

        // Create metadata for your folder & set content-length to 0
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        // Create empty content
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
        // Create a PutObjectRequest passing the foldername suffixed by /
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, pathFolder + "/" + foldername + "/", emptyContent, metadata);
        // Send request to S3 to create folder
        connSFTP.putObject(putObjectRequest);
    }

    public static void S3UploadFiles(AmazonS3 connSFTP, String buketName, String s3PathFolder, String localFilePath) {
//        ByteArrayInputStream input = new ByteArrayInputStream("Test working!".getBytes());
        File file = new File(localFilePath);
        connSFTP.putObject(buketName + "/" + s3PathFolder, file.getName(), file);
//        connSFTP.putObject("rds2rsh", "JorgeMunoz.txt", input, new ObjectMetadata());
    }

    public static void S3DeleteFiles(AmazonS3 connSFTP, String buketName, String fileName) {
        //Delete an object       
        connSFTP.deleteObject(buketName, fileName);
    }

}
