package com.amazonaws.services.s3.sample;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;


import com.amazonaws.services.s3.sample.auth.AWS4SignerForChunkedUpload;
import com.amazonaws.services.s3.sample.util.HttpUtils;

/**
 * Sample code showing how to PUT objects to Amazon S3 using chunked uploading
 * with Signature V4 authorization
 */
public class PutS3ObjectChunkedSample {
    
    private static final String contentSeed = 
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nunc tortor metus, sagittis eget augue ut,\n"
            + "feugiat vehicula risus. Integer tortor mauris, vehicula nec mollis et, consectetur eget tortor. In ut\n"
            + "elit sagittis, ultrices est ut, iaculis turpis. In hac habitasse platea dictumst. Donec laoreet tellus\n"
            + "at auctor tempus. Praesent nec diam sed urna sollicitudin vehicula eget id est. Vivamus sed laoreet\n"
            + "lectus. Aliquam convallis condimentum risus, vitae porta justo venenatis vitae. Phasellus vitae nunc\n"
            + "varius, volutpat quam nec, mollis urna. Donec tempus, nisi vitae gravida facilisis, sapien sem malesuada\n"
            + "purus, id semper libero ipsum condimentum nulla. Suspendisse vel mi leo. Morbi pellentesque placerat congue.\n"
            + "Nunc sollicitudin nunc diam, nec hendrerit dui commodo sed. Duis dapibus commodo elit, id commodo erat\n"
            + "congue id. Aliquam erat volutpat.\n";
    
    /**
     * Uploads content to an Amazon S3 object in a series of signed 'chunks' using Signature V4 authorization.
     */
    public static void putS3ObjectChunked(String bucketName, String regionName, String awsAccessKey, String awsSecretKey) {
        System.out.println("***************************************************");
        System.out.println("*      Executing sample 'PutS3ObjectChunked'      *");
        System.out.println("***************************************************");
        
        // this sample uses a chunk data length of 64K; this should yield one
        // 64K chunk, one partial chunk and the final 0 byte payload terminator chunk
        final int userDataBlockSize = 64 * 1024;
        String sampleContent = make65KPayload();
        
        URL endpointUrl;
        try {
            if (regionName.equals("us-east-1")) {
                endpointUrl = new URL("https://s3.amazonaws.com/" + bucketName + "/ExampleChunkedObject.txt");
            } else {
                endpointUrl = new URL("https://s3-" + regionName + ".amazonaws.com/" + bucketName + "/ExampleChunkedObject.txt");
            }
        } catch (MalformedURLException e) {
            throw new RuntimeException("Unable to parse service endpoint: " + e.getMessage());
        }
        
        // set the markers indicating we're going to send the upload as a series 
        // of chunks:
        //   -- 'x-amz-content-sha256' is the fixed marker indicating chunked
        //      upload
        //   -- 'content-length' becomes the total size in bytes of the upload 
        //      (including chunk headers), 
        //   -- 'x-amz-decoded-content-length' is used to transmit the actual 
        //      length of the data payload, less chunk headers
        
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("x-amz-storage-class", "REDUCED_REDUNDANCY");
        headers.put("x-amz-content-sha256", AWS4SignerForChunkedUpload.STREAMING_BODY_SHA256);
        headers.put("content-encoding", "" + "aws-chunked");
        headers.put("x-amz-decoded-content-length", "" + sampleContent.length());
        
        AWS4SignerForChunkedUpload signer = new AWS4SignerForChunkedUpload(
                endpointUrl, "PUT", "s3", regionName);
        
        // how big is the overall request stream going to be once we add the signature 
        // 'headers' to each chunk?
        long totalLength = AWS4SignerForChunkedUpload.calculateChunkedContentLength(sampleContent.length(), userDataBlockSize);
        headers.put("content-length", "" + totalLength);
        
        String authorization = signer.computeSignature(headers, 
                                                       null, // no query parameters
                                                       AWS4SignerForChunkedUpload.STREAMING_BODY_SHA256, 
                                                       awsAccessKey, 
                                                       awsSecretKey);
                
        // place the computed signature into a formatted 'Authorization' header 
        // and call S3
        headers.put("Authorization", authorization);
        
        // start consuming the data payload in blocks which we subsequently chunk; this prefixes
        // the data with a 'chunk header' containing signature data from the prior chunk (or header
        // signing, if the first chunk) plus length and other data. Each completed chunk is
        // written to the request stream and to complete the upload, we send a final chunk with
        // a zero-length data payload.
        
        try {
            // first set up the connection
            HttpURLConnection connection = HttpUtils.createHttpConnection(endpointUrl, "PUT", headers);
            
            // get the request stream and start writing the user data as chunks, as outlined
            // above;
            byte[] buffer = new byte[userDataBlockSize];
            DataOutputStream outputStream = new DataOutputStream(connection.getOutputStream());
            
            // get the data stream
            ByteArrayInputStream inputStream = new ByteArrayInputStream(sampleContent.getBytes("UTF-8"));
            
            int bytesRead = 0;
            while ( (bytesRead = inputStream.read(buffer, 0, buffer.length)) != -1 ) {
                // process into a chunk
                byte[] chunk = signer.constructSignedChunk(bytesRead, buffer);
                
                // send the chunk
                outputStream.write(chunk);
                outputStream.flush();
            }
            
            // last step is to send a signed zero-length chunk to complete the upload
            byte[] finalChunk = signer.constructSignedChunk(0, buffer);
            outputStream.write(finalChunk);
            outputStream.flush();
            outputStream.close();
            
            // make the call to Amazon S3
            String response = HttpUtils.executeHttpRequest(connection);
            System.out.println("--------- Response content ---------");
            System.out.println(response);
            System.out.println("------------------------------------");
        } catch (Exception e) {
            throw new RuntimeException("Error when sending chunked upload request. " + e.getMessage(), e);
        }
    }

    /**
     * Want sample to upload 3 chunks for our selected chunk size of 64K; one
     * full size chunk, one partial chunk and then the 0-byte terminator chunk.
     * This routine just takes 1K of seed text and turns it into a 65K-or-so
     * string for sample use.
     */
    private static String make65KPayload() {
        StringBuilder oneKSeed = new StringBuilder();
        while ( oneKSeed.length() < 1024 ) {
            oneKSeed.append(contentSeed);
        }
        
        // now scale up to meet/exceed our requirement
        StringBuilder output = new StringBuilder();
        for (int i = 0; i < 66; i++) {
            output.append(oneKSeed);
        }
        return output.toString();
    }
}
