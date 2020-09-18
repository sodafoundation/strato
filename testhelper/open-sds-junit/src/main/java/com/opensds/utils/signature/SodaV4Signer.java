package com.opensds.utils.signature;

import uk.co.lucasweb.aws.v4.signer.Header;
import uk.co.lucasweb.aws.v4.signer.HttpRequest;
import uk.co.lucasweb.aws.v4.signer.Signer;
import uk.co.lucasweb.aws.v4.signer.credentials.AwsCredentials;

import java.net.URI;
import java.net.URISyntaxException;

public class SodaV4Signer {

    /**
     * Generate v4 Signature.
     *
     * @param method     Method name e.g GET
     * @param url        URL e.g https://www.opensds.io/
     * @param accessKey  Access key get from AKSK API
     * @param secretKey  Secret key get from  AKSK API
     * @param payload    Request body converted in to SHA265
     * @param regionName Region name
     * @param headers    Headers
     * @return Signature
     */
    public static String getSignature(String method, String url,
                                      String accessKey, String secretKey,
                                      String payload, String regionName, Header... headers) {
        HttpRequest httpRequest = null;
        try {
            httpRequest = new HttpRequest(method, new URI(url));
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return Signer.builder()
                .awsCredentials(new AwsCredentials(accessKey, secretKey))
                .region(regionName)
                .headers(headers)
                .buildS3(httpRequest, payload)
                .getSignature();
    }
}
