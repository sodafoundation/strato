package com.opensds;

import com.google.gson.Gson;
import com.opensds.jsonmodels.akskresponses.AKSKHolder;
import com.opensds.jsonmodels.akskresponses.SignatureKey;
import com.opensds.jsonmodels.authtokensrequests.Project;
import com.opensds.jsonmodels.authtokensrequests.Scope;
import com.opensds.jsonmodels.authtokensresponses.AuthTokenHolder;
import com.opensds.jsonmodels.inputs.addbackend.AddBackendInputHolder;
import com.opensds.jsonmodels.inputs.createbucket.CreateBucketFileInput;
import com.opensds.jsonmodels.logintokensrequests.*;
import com.opensds.jsonmodels.tokensresponses.TokenHolder;
import com.opensds.jsonmodels.typesresponse.TypesHolder;
import com.opensds.utils.*;
import com.opensds.utils.okhttputils.OkHttpRequests;
import com.opensds.utils.signature.SodaV4Signer;
import okhttp3.*;
import okio.BufferedSink;
import okio.Okio;
import uk.co.lucasweb.aws.v4.signer.Header;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static com.opensds.utils.HeadersName.*;

public class HttpHandler extends OkHttpRequests {
    private OkHttpClient client = new OkHttpClient();

    public SignatureKey getAkSkList(String xAuthToken, String userId) {
        SignatureKey signatureKey = new SignatureKey();
        String url = ConstantUrl.getInstance().getAksList(userId);
        Logger.logString("URL: " + url);
        try {
            Gson gson = new Gson();
            Map<String, String> headersMap = new HashMap<>();
            headersMap.put(CONTENT_TYPE, CONTENT_TYPE_JSON);
            headersMap.put(X_AUTH_TOKEN, xAuthToken);
            Headers  headers = Headers.of(headersMap);
            Response response = getCall(client, url, headers);
            String responseBody = response.body().string();
            Logger.logString("Response: " + responseBody);
            AKSKHolder akskHolder = gson.fromJson(responseBody, AKSKHolder.class);
            Logger.logObject(akskHolder);
            // build the SignatureKey struct and set the values
            new Runnable() {
                @Override
                public void run() {
                    signatureKey.setSecretAccessKey(akskHolder.getCredentials()[0].getBlobObj().getSecret());
                    signatureKey.setAccessKey(akskHolder.getCredentials()[0].getBlobObj().getAccess());
                    String regionName = "us-east-1";
                    signatureKey.setRegionName(regionName);
                    String serviceName = "s3";
                    signatureKey.setServiceName(serviceName);
                }
            }.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return signatureKey;
    }

    public TokenHolder loginAndGetToken() {
        TokenHolder tokenHolder = null;
        try {
            Auth auth = new Auth();
            auth.setIdentity(new Identity());
            auth.getIdentity().getMethods().add("password");
            auth.getIdentity().setPassword(new Password());
            auth.getIdentity().getPassword().setUser(new User());
            auth.getIdentity().getPassword().getUser().setName("admin");
            auth.getIdentity().getPassword().getUser().setPassword("opensds@123");
            auth.getIdentity().getPassword().getUser().setDomain(new Domain());
            auth.getIdentity().getPassword().getUser().getDomain().setName("Default");

            AuthHolder authHolder = new AuthHolder();
            authHolder.setAuth(auth);

            Gson gson = new Gson();
            RequestBody requestBody = RequestBody.create(gson.toJson(authHolder),
                    MediaType.parse("application/json; charset=utf-8"));
            String url = ConstantUrl.getInstance().getTokenLogin();
            Logger.logString("URL: " + url);
            Map<String, String> headersMap = new HashMap<>();
            headersMap.put(CONTENT_TYPE, CONTENT_TYPE_JSON);
            Headers  headers = Headers.of(headersMap);
            Response response = postCall(client, url, requestBody, headers);
            String responseBody = response.body().string();
            Logger.logString("Response code: " + response.code());
            Logger.logString("Response body: " + responseBody);
            tokenHolder = gson.fromJson(responseBody, TokenHolder.class);
            tokenHolder.setResponseHeaderSubjectToken(response.header(X_SUBJECT_TOKEN));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tokenHolder;
    }

    public AuthTokenHolder getAuthToken(String x_auth_token) {
        AuthTokenHolder tokenHolder = null;
        try {
            com.opensds.jsonmodels.authtokensrequests.Auth auth = new com.opensds.jsonmodels.authtokensrequests.Auth();
            auth.setIdentity(new com.opensds.jsonmodels.authtokensrequests.Identity());
            auth.getIdentity().getMethods().add("token");
            auth.getIdentity().setToken(new com.opensds.jsonmodels.authtokensrequests.Token(x_auth_token));

            auth.setScope(new Scope());
            auth.getScope().setProject(new Project());
            auth.getScope().getProject().setName("admin");
            auth.getScope().getProject().setDomain(new com.opensds.jsonmodels.authtokensrequests.Domain());
            auth.getScope().getProject().getDomain().setId("default");
            com.opensds.jsonmodels.authtokensrequests.AuthHolder authHolder = new com.opensds.jsonmodels
                    .authtokensrequests.AuthHolder();
            authHolder.setAuth(auth);

            Gson gson = new Gson();
            RequestBody requestBody = RequestBody.create(gson.toJson(authHolder),
                    MediaType.parse(CONTENT_TYPE_JSON_CHARSET));
            String url = ConstantUrl.getInstance().getTokenLogin();
            Logger.logString("URL: " + url);
            Map<String, String> headersMap = new HashMap<>();
            headersMap.put(CONTENT_TYPE, CONTENT_TYPE_JSON);
            Headers  headers = Headers.of(headersMap);
            Response response = postCall(client, url, requestBody, headers);
            String responseBody = response.body().string();
            Logger.logString("Response code: " + response.code());
            Logger.logString("Response body: " + responseBody);
            tokenHolder = new com.opensds.jsonmodels.authtokensresponses.AuthTokenHolder();
            tokenHolder = gson.fromJson(responseBody, com.opensds.jsonmodels.authtokensresponses.AuthTokenHolder.class);
            tokenHolder.setResponseHeaderSubjectToken(response.header(X_SUBJECT_TOKEN));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tokenHolder;
    }

    public TypesHolder getTypes(String x_auth_token, String projId) {
        TypesHolder typesHolder = null;
        try {
            Gson gson = new Gson();
            String url = ConstantUrl.getInstance().getTypesUrl(projId);
            Logger.logString("URL: " + url);
            Map<String, String> headersMap = new HashMap<>();
            headersMap.put(CONTENT_TYPE, CONTENT_TYPE_JSON);
            headersMap.put(X_AUTH_TOKEN, x_auth_token);
            Headers headers = Headers.of(headersMap);
            Response response = getCall(client, url, headers);
            String responseBody = response.body().string();
            Logger.logString("Response code: " + response.code());
            Logger.logString("Response body: " + responseBody);
            typesHolder = gson.fromJson(responseBody, TypesHolder.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return typesHolder;
    }

    public int addBackend(String x_auth_token, String projId, AddBackendInputHolder inputHolder) {
        int code = -1;
        try {
            Gson gson = new Gson();
            RequestBody requestBody = RequestBody.create(gson.toJson(inputHolder),
                    MediaType.parse(CONTENT_TYPE_JSON_CHARSET));
            String url = ConstantUrl.getInstance().getAddBackendUrl(projId);
            Logger.logString("URL: " + url);
            Map<String, String> headersMap = new HashMap<>();
            headersMap.put(CONTENT_TYPE, CONTENT_TYPE_JSON);
            headersMap.put(X_AUTH_TOKEN, x_auth_token);
            Headers headers = Headers.of(headersMap);
            Response response = postCall(client, url, requestBody, headers);
            code = response.code();
            Logger.logString("Response code: " + code);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return code;
    }

    public int createBucket(CreateBucketFileInput input, String bucketName, SignatureKey signatureKey) {
        int code = -1;
        try {
            String url = ConstantUrl.getInstance().getCreateBucketUrl(bucketName);
            RequestBody requestBody = RequestBody.create(input.getXmlPayload(),
                    MediaType.parse(CONTENT_TYPE_XML));
            String payload = BinaryUtils.toHex(BinaryUtils.hash(input.getXmlPayload()));
            Response response = putCallResponse(client, url, payload, requestBody, signatureKey);
            code = response.code();
            Logger.logString("Response Code: " + code);
            Logger.logString("Response: " + response.body().string());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return code;
    }

    public Response getBuckets(SignatureKey signatureKey) {
        String url = ConstantUrl.getInstance().getListBucketUrl();
        return getCallResponse(client, url, signatureKey);
    }

    public int uploadObject(SignatureKey signatureKey, String bucketName, String fileName, File mFilePath) {
        int code = -1;
        try {
            RequestBody requestBody = RequestBody.create(mFilePath,
                    MediaType.parse(CONTENT_TYPE_XML));
            String url = ConstantUrl.getInstance().getUploadObjectUrl(bucketName, fileName);
            String payload = BinaryUtils.toHex(BinaryUtils.computeSHA256TreeHash(mFilePath));
            Response response = putCallResponse(client, url, payload, requestBody, signatureKey);
            code = response.code();
            Logger.logString("Response Code: " + code);
            Logger.logString("Response: " + response.body().string());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return code;
    }

    public Response getBucketObjects(String bucketName, SignatureKey signatureKey) {
        String url = ConstantUrl.getInstance().getListOfBucketObjectUrl(bucketName);
        return getCallResponse(client, url, signatureKey);
    }

    public Response downloadObject(SignatureKey signatureKey, String bucketName, String fileName, String downloadFileName) {
        Response response = null;
        try {
            String url = ConstantUrl.getInstance().getDownloadObjectUrl(bucketName, fileName);
            response = getCallResponse(client, url, signatureKey);
            int code = response.code();
            if (code == 200) {
                BufferedSink sink = Okio.buffer(Okio.sink(new File(Constant.DOWNLOAD_FILES_PATH, downloadFileName)));
                sink.writeAll(response.body().source());
                sink.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
    }
}