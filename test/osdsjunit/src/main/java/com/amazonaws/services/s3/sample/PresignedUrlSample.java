package com.amazonaws.services.s3.sample;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;


import com.amazonaws.services.s3.sample.auth.AWS4SignerBase;
import com.amazonaws.services.s3.sample.auth.AWS4SignerForQueryParameterAuth;

/**
 * Sample code showing how to use Presigned Urls with Signature V4 authorization
 */
public class PresignedUrlSample {
     
    /**
     * Construct a basic presigned url to the object '/ExampleObject.txt' in the
     * given bucket and region using path-style object addressing. The signature
     * V4 authorization data is embedded in the url as query parameters.
     */
    public static void getPresignedUrlToS3Object(String bucketName, String regionName, String awsAccessKey, String awsSecretKey) {
        System.out.println("******************************************************");
        System.out.println("*    Executing sample 'GetPresignedUrlToS3Object'    *");
        System.out.println("******************************************************");
        
        URL endpointUrl;
        try {
            if (regionName.equals("us-east-1")) {
                endpointUrl = new URL("https://s3.amazonaws.com/" + bucketName + "/ExampleObject.txt");
            } else {
                endpointUrl = new URL("https://s3-" + regionName + ".amazonaws.com/" + bucketName + "/ExampleObject.txt");
            }
        } catch (MalformedURLException e) {
            throw new RuntimeException("Unable to parse service endpoint: " + e.getMessage());
        }
        
        // construct the query parameter string to accompany the url
        Map<String, String> queryParams = new HashMap<String, String>();
         
        // for SignatureV4, the max expiry for a presigned url is 7 days,
        // expressed in seconds
        int expiresIn = 7 * 24 * 60 * 60;
        queryParams.put("X-Amz-Expires", "" + expiresIn);
        
        // we have no headers for this sample, but the signer will add 'host'
        Map<String, String> headers = new HashMap<String, String>();
        
        AWS4SignerForQueryParameterAuth signer = new AWS4SignerForQueryParameterAuth(
                endpointUrl, "GET", "s3", regionName);
        String authorizationQueryParameters = signer.computeSignature(headers, 
                                                       queryParams,
                                                       AWS4SignerBase.UNSIGNED_PAYLOAD, 
                                                       awsAccessKey, 
                                                       awsSecretKey);
                
        // build the presigned url to incorporate the authorization elements as query parameters
        String presignedUrl = endpointUrl.toString() + "?" + authorizationQueryParameters;
        System.out.println("--------- Computed presigned url ---------");
        System.out.println(presignedUrl);
        System.out.println("------------------------------------------");
    }
}
