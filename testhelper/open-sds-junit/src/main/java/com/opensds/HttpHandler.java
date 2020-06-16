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
import com.opensds.utils.signature.SodaV4Signer;
import okhttp3.*;
import uk.co.lucasweb.aws.v4.signer.Header;

public class HttpHandler {
    private OkHttpClient client = new OkHttpClient();

    public SignatureKey getAkSkList(String x_auth_token, String userId) {
        SignatureKey signatureKey = new SignatureKey();
        String url = ConstantUrl.getInstance().getAksList(userId);
        Logger.logString("URL: " + url);
        try {
            Gson gson = new Gson();
            Request request = new Request.Builder()
                    .url(url)
                    .get()
                    .addHeader(HeadersName.CONTENT_TYPE, HeadersName.CONTENT_TYPE_JSON)
                    .addHeader(HeadersName.X_AUTH_TOKEN, x_auth_token)
                    .build();
            Logger.logString("Request Details: " + request.headers() + " " + request.body() + " " + request.method() + ""
                    + request.url());
            Response response = client.newCall(request).execute();
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
            RequestBody body = RequestBody.create(gson.toJson(authHolder),
                    MediaType.parse("application/json; charset=utf-8"));
            String url = ConstantUrl.getInstance().getTokenLogin();
            Logger.logString("URL: " + url);
            Request request = new Request.Builder()
                    .url(url)
                    .post(body)
                    .addHeader(HeadersName.CONTENT_TYPE, HeadersName.CONTENT_TYPE_JSON)
                    .build();
            Logger.logString("Request Details: " + request.headers() + " " + request.body() + " " + request.method() + ""
                    + request.url());
            Response response = client.newCall(request).execute();
            String responseBody = response.body().string();
            Logger.logString("Response code: " + response.code());
            Logger.logString("Response body: " + responseBody);
            tokenHolder = gson.fromJson(responseBody, TokenHolder.class);
            tokenHolder.setResponseHeaderSubjectToken(response.header("X-Subject-Token"));
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
            RequestBody body = RequestBody.create(gson.toJson(authHolder),
                    MediaType.parse("application/json; charset=utf-8"));
            String url = ConstantUrl.getInstance().getTokenLogin();
            Logger.logString("URL: " + url);
            Request request = new Request.Builder()
                    .url(url)
                    .post(body)
                    .addHeader(HeadersName.CONTENT_TYPE, HeadersName.CONTENT_TYPE_JSON)
                    .build();
            Logger.logString("Request Details: " + request.headers() + " " + request.body() + " " + request.method() + ""
                    + request.url());
            Response response = client.newCall(request).execute();
            String responseBody = response.body().string();
            Logger.logString("Response code: " + response.code());
            Logger.logString("Response body: " + responseBody);
            tokenHolder = new com.opensds.jsonmodels.authtokensresponses.AuthTokenHolder();
            tokenHolder = gson.fromJson(responseBody, com.opensds.jsonmodels.authtokensresponses.AuthTokenHolder.class);
            tokenHolder.setResponseHeaderSubjectToken(response.header("X-Subject-Token"));
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
            Request request = new Request.Builder()
                    .url(url)
                    .addHeader(HeadersName.CONTENT_TYPE, HeadersName.CONTENT_TYPE_JSON)
                    .addHeader(HeadersName.X_AUTH_TOKEN, x_auth_token)
                    .build();
            Logger.logString("Request Details: " + request.headers() + " " + request.body() + " " + request.method() + ""
                    + request.url());
            Response response = client.newCall(request).execute();
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
            RequestBody body = RequestBody.create(gson.toJson(inputHolder),
                    MediaType.parse("application/json; charset=utf-8"));
            String url = ConstantUrl.getInstance().getAddBackendUrl(projId);
            Logger.logString("URL: " + url);
            Request request = new Request.Builder()
                    .url(url)
                    .post(body)
                    .addHeader(HeadersName.CONTENT_TYPE, HeadersName.CONTENT_TYPE_JSON)
                    .addHeader(HeadersName.X_AUTH_TOKEN, x_auth_token)
                    .build();
            Logger.logString("Request Details: " + request.headers() + " " + request.body() + " " + request.method() + ""
                    + request.url());
            Response response = client.newCall(request).execute();
            code = response.code();
            Logger.logString("Response code: " + code);
            Logger.logString("Response body: " + response.body().string());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return code;
    }

    public int createBucket(CreateBucketFileInput input, String bucketName, SignatureKey signatureKey) {
        int code = -1;
        try {
            RequestBody body = RequestBody.create(input.getXmlPayload(),
                    MediaType.parse("application/xml"));
            String payload = BinaryUtils.toHex(BinaryUtils.hash(input.getXmlPayload()));
            String date = Utils.getDate();
            String url = ConstantUrl.getInstance().getCreateBucketUrl(bucketName);
            Logger.logString("URL: " + url);
            String host = Utils.getHost(url);
            Header[] headerList = new Header[]{new Header(HeadersName.HOST, host),
                    new Header(HeadersName.X_AMZ_DATE, date),
                    new Header(HeadersName.X_AMZ_CONTENT_SHA256, payload)};
            String authorization = SodaV4Signer.getSignature("PUT", url, signatureKey.getAccessKey(),
                    signatureKey.getSecretAccessKey(), payload, signatureKey.getRegionName(), headerList);
            Logger.logString("Authorization: " + authorization);

            Request request = new Request.Builder()
                    .url(url)
                    .put(body)
                    .header(HeadersName.HOST, host)
                    .header(HeadersName.AUTHORIZATION, authorization)
                    .header(HeadersName.X_AMZ_CONTENT_SHA256, payload)
                    .header(HeadersName.CONTENT_TYPE, HeadersName.CONTENT_TYPE_XML)
                    .header(HeadersName.X_AMZ_DATE, date)
                    .build();

            Logger.logString("Request Details: " + request.headers() + " " + request.body() + " " + request.method() + ""
                    + request.url());
            Response response = client.newCall(request).execute();
            code = response.code();
            Logger.logString("Response Code: " + code);
            Logger.logString("Response: " + response.body().string());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return code;
    }

    public Response getBuckets(SignatureKey signatureKey) {
        Response response = null;
        String url = ConstantUrl.getInstance().getListBucketUrl();
        String payload = BinaryUtils.toHex(BinaryUtils.hash(""));
        String date = Utils.getDate();
        Logger.logString("URL: " + url);
        String host = Utils.getHost(url);
        Header[] headerList = new Header[]{new Header(HeadersName.HOST, host),
                new Header(HeadersName.X_AMZ_DATE, date),
                new Header(HeadersName.X_AMZ_CONTENT_SHA256, payload)};
        String authorization = SodaV4Signer.getSignature("GET", url, signatureKey.getAccessKey(),
                signatureKey.getSecretAccessKey(), payload, signatureKey.getRegionName(), headerList);
        Logger.logString("Authorization: " + authorization);
        try {
            Request request = new Request.Builder()
                    .url(url)
                    .get()
                    .addHeader(HeadersName.AUTHORIZATION, authorization)
                    .addHeader(HeadersName.X_AMZ_CONTENT_SHA256, payload)
                    .addHeader(HeadersName.X_AMZ_DATE, date)
                    .build();
            Logger.logString("Request Details: " + request.headers() + " " + request.body() + " " + request.method() + ""
                    + request.url());
            response = client.newCall(request).execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return response;
    }
}