package com.opensds.utils.okhttputils;

import com.opensds.jsonmodels.akskresponses.SignatureKey;
import com.opensds.utils.*;
import com.opensds.utils.signature.SodaV4Signer;
import okhttp3.*;
import uk.co.lucasweb.aws.v4.signer.Header;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.opensds.utils.HeadersName.*;

public abstract class OkHttpRequests {

    /**
     * Put Call
     *
     * @param client OkHttpClient object
     * @param url url
     * @param requestBody RequestBody object
     * @param headers Headers
     * @return response
     * @throws IOException io exception
     */
    protected static Response putCall(OkHttpClient client, String url, RequestBody requestBody,
                                      Headers headers) throws IOException {
        Request request = new Request.Builder()
                .url(url)
                .put(requestBody)
                .headers(headers)
                .build();
        Logger.logString("Request Details: " + request.headers() + " " + request.body() + " " + request.method() + ""
                + request.url());
        return client.newCall(request).execute();
    }

    /**
     * Post Call
     *
     * @param client OkHttpClient object
     * @param url url
     * @param requestBody RequestBody object
     * @param headers Headers
     * @return Response
     * @throws IOException IO Exception
     */
    protected static Response postCall(OkHttpClient client, String url, RequestBody requestBody,
                                       Headers headers) throws IOException {
        Request request = new Request.Builder()
                .url(url)
                .post(requestBody)
                .headers(headers)
                .build();
        Logger.logString("Request Details: " + request.headers() + " " + request.body() + " " + request.method() + ""
                + request.url());
        return client.newCall(request).execute();
    }

    /**
     * Get Call
     *
     * @param client OkHttpClient
     * @param url URL
     * @param headers Headers
     * @return Response
     * @throws IOException IO Exception
     */
    protected static Response getCall(OkHttpClient client, String url,
                                      Headers headers) throws IOException {
        Request request = new Request.Builder()
                .url(url)
                .get()
                .headers(headers)
                .build();
        Logger.logString("Request Details: " + request.headers() + " " + request.body() + " " + request.method() + ""
                + request.url());
        return client.newCall(request).execute();
    }

    /**
     * Delete Call
     *
     * @param client OkHttpClient
     * @param url URL
     * @param headers Headers
     * @return Response
     * @throws IOException IO Exception
     */
    protected static Response deleteCall(OkHttpClient client, String url,
                                      Headers headers) throws IOException {
        Request request = new Request.Builder()
                .url(url)
                .delete()
                .headers(headers)
                .build();
        Logger.logString("Request Details: " + request.headers() + " " + request.body() + " " + request.method() + ""
                + request.url());
        return client.newCall(request).execute();
    }

    protected static Response getCallResponse(OkHttpClient client, String url, SignatureKey signatureKey) {
        Response response = null;
        String payload = BinaryUtils.toHex(BinaryUtils.hash(""));
        String date = Utils.getDate();
        Logger.logString("URL: " + url);
        String host = Utils.getHost(url);
        Header[] signHeaders = new Header[]{new Header(HOST, host),
                new Header(X_AMZ_DATE, date),
                new Header(X_AMZ_CONTENT_SHA256, payload)};
        String authorization = SodaV4Signer.getSignature("GET", url, signatureKey.getAccessKey(),
                signatureKey.getSecretAccessKey(), payload, signatureKey.getRegionName(), signHeaders);
        Logger.logString("Authorization: " + authorization);
        Map<String, String> headersMap = new HashMap<>();
        headersMap.put(AUTHORIZATION, authorization);
        headersMap.put(X_AMZ_CONTENT_SHA256, payload);
        headersMap.put(X_AMZ_DATE, date);
        Headers headers = Headers.of(headersMap);
        try {
            response = getCall(client, url, headers);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
    }

    protected static Response putCallResponse(OkHttpClient client, String url,
                                              String payload, RequestBody requestBody,
                                              SignatureKey signatureKey) {
        Response response = null;

        String date = Utils.getDate();
        Logger.logString("URL: " + url);
        String host = Utils.getHost(url);
        Header[] signHeaders = new Header[]{new Header(HOST, host),
                new Header(X_AMZ_DATE, date),
                new Header(X_AMZ_CONTENT_SHA256, payload)};
        String authorization = SodaV4Signer.getSignature("PUT", url, signatureKey.getAccessKey(),
                signatureKey.getSecretAccessKey(), payload, signatureKey.getRegionName(), signHeaders);
        Logger.logString("Authorization: " + authorization);
        Map<String, String> headersMap = new HashMap<>();
        headersMap.put(HOST, host);
        headersMap.put(AUTHORIZATION, authorization);
        headersMap.put(X_AMZ_CONTENT_SHA256, payload);
        headersMap.put(CONTENT_TYPE, CONTENT_TYPE_XML);
        headersMap.put(X_AMZ_DATE, date);
        Headers headers = Headers.of(headersMap);
        try {
            response = putCall(client, url, requestBody, headers);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    protected static Response getCallWithXauth(OkHttpClient client, String url, String xAuthToken){
        Response response = null;
        Map<String, String> headersMap = new HashMap<>();
        headersMap.put(X_AUTH_TOKEN, xAuthToken);
        headersMap.put(CONTENT_TYPE, CONTENT_TYPE_JSON);
        Headers headers = Headers.of(headersMap);
        try {
            response = getCall(client, url, headers);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
    }

    protected static Response deleteCallWithXauth(OkHttpClient client, String url, String xAuthToken){
        Response response = null;
        Map<String, String> headersMap = new HashMap<>();
        headersMap.put(X_AUTH_TOKEN, xAuthToken);
        headersMap.put(CONTENT_TYPE, CONTENT_TYPE_JSON);
        Headers headers = Headers.of(headersMap);
        try {
            response = deleteCall(client, url, headers);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
    }

    protected static Response deleteCallWithV4Sign(OkHttpClient client, String url, SignatureKey signatureKey) {
        Response response = null;
        String payload = BinaryUtils.toHex(BinaryUtils.hash(""));
        String date = Utils.getDate();
        Logger.logString("URL: " + url);
        String host = Utils.getHost(url);
        Header[] signHeaders = new Header[]{new Header(HOST, host),
                new Header(X_AMZ_DATE, date),
                new Header(X_AMZ_CONTENT_SHA256, payload)};
        String authorization = SodaV4Signer.getSignature("DELETE", url, signatureKey.getAccessKey(),
                signatureKey.getSecretAccessKey(), payload, signatureKey.getRegionName(), signHeaders);
        Logger.logString("Authorization: " + authorization);
        Map<String, String> headersMap = new HashMap<>();
        headersMap.put(AUTHORIZATION, authorization);
        headersMap.put(X_AMZ_CONTENT_SHA256, payload);
        headersMap.put(X_AMZ_DATE, date);
        Headers headers = Headers.of(headersMap);
        try {
            response = deleteCall(client, url, headers);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
    }
}
