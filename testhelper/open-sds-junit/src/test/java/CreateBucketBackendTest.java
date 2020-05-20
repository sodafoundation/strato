import com.google.gson.Gson;
import com.opensds.HttpHandler;
import com.opensds.jsonmodels.akskresponses.SignatureKey;
import com.opensds.jsonmodels.authtokensresponses.AuthTokenHolder;
import com.opensds.jsonmodels.inputs.addbackend.AddBackendInputHolder;
import com.opensds.jsonmodels.inputs.createbucket.CreateBucketFileInput;
import com.opensds.jsonmodels.tokensresponses.TokenHolder;
import com.opensds.jsonmodels.typesresponse.Type;
import com.opensds.jsonmodels.typesresponse.TypesHolder;
import com.opensds.utils.Constant;
import com.opensds.utils.Logger;
import com.opensds.utils.TextUtils;
import com.opensds.utils.Utils;
import okhttp3.Response;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.XML;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.*;

// how to get POJO from any response JSON, use this site
// http://pojo.sodhanalibrary.com/

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CreateBucketBackendTest {

    public static AuthTokenHolder getAuthTokenHolder() {
        return mAuthTokenHolder;
    }

    public TypesHolder getTypesHolder() {
        return mTypesHolder;
    }

    public static HttpHandler getHttpHandler() {
        return mHttpHandler;
    }

    private static AuthTokenHolder mAuthTokenHolder = null;
    private static TypesHolder mTypesHolder = null;
    private static HttpHandler mHttpHandler = new HttpHandler();

    @org.junit.jupiter.api.BeforeAll
    static void setUp() {
        TokenHolder tokenHolder = getHttpHandler().loginAndGetToken();
        mAuthTokenHolder = getHttpHandler().getAuthToken(tokenHolder.getResponseHeaderSubjectToken());
        mTypesHolder = getHttpHandler().getTypes(getAuthTokenHolder().getResponseHeaderSubjectToken(),
                getAuthTokenHolder().getToken().getProject().getId());
    }

    @Test
    @Order(1)
    @DisplayName("Test creating bucket and backend on OPENSDS")
    public void testCreateBucketAndBackend() throws IOException, JSONException {
        // load input files for each type and create the backend
        for (Type t : getTypesHolder().getTypes()) {
            List<File> listOfIInputsForType =
                    Utils.listFilesMatchingBeginsWithPatternInPath(t.getName(),
                            Constant.CREATE_BUCKET_PATH);
            Gson gson = new Gson();
            // add the backend specified in each file
            for (File file : listOfIInputsForType) {
                String content = Utils.readFileContentsAsString(file);
                assertNotNull(content);

                AddBackendInputHolder inputHolder = gson.fromJson(content, AddBackendInputHolder.class);
                int code = getHttpHandler().addBackend(getAuthTokenHolder().getResponseHeaderSubjectToken(),
                        getAuthTokenHolder().getToken().getProject().getId(),
                        inputHolder);
                assertEquals(code, 200);

                // backend added, now create buckets
                List<File> listOfIBucketInputs =
                        Utils.listFilesMatchingBeginsWithPatternInPath("bucket",
                                Constant.CREATE_BUCKET_PATH);
                SignatureKey signatureKey = getHttpHandler().getAkSkList(getAuthTokenHolder().getResponseHeaderSubjectToken(),
                        getAuthTokenHolder().getToken().getProject().getId());
                // create the bucket specified in each file
                for (File bucketFile : listOfIBucketInputs) {
                    String bucketContent = Utils.readFileContentsAsString(bucketFile);
                    assertNotNull(bucketContent);

                    CreateBucketFileInput bfi = gson.fromJson(bucketContent, CreateBucketFileInput.class);

                    // filename format is "bucket_<bucketname>.json", get the bucket name here
                    String bName = Utils.getBucketName(bucketFile);

                    // now create buckets
                    int cbCode = getHttpHandler().createBucket(bfi, bName, signatureKey);
                    assertEquals(cbCode, 200);
                    boolean isBucketExist = testGetListBuckets(bName, signatureKey);
                    assertTrue(isBucketExist, "Bucket is not exist: ");
                }
            }
        }
    }

    public boolean testGetListBuckets(String bName, SignatureKey signatureKey)
            throws JSONException, IOException {
        Response listBucketResponse = getHttpHandler().getBuckets(signatureKey);
        int resCode = listBucketResponse.code();
        String responseBody = listBucketResponse.body().string();
        Logger.logString("Response Code: " + resCode);
        Logger.logString("Response: " + responseBody);
        JSONObject jsonObject = XML.toJSONObject(responseBody);
        JSONArray jsonObjectBucketList = jsonObject.getJSONObject("ListAllMyBucketsResult")
                .getJSONObject("Buckets").getJSONArray("Bucket");
        boolean isBucketExist = false;
        for (int i = 0; i < jsonObjectBucketList.length(); i++) {
            String bucketName = jsonObjectBucketList.getJSONObject(i).get("Name").toString();
            if (!TextUtils.isEmpty(bucketName)) {
                if (bucketName.equals(bName)) {
                    isBucketExist = true;
                }
            }
        }
        return isBucketExist;
    }

    @Test
    @Order(2)
    @DisplayName("Test creating bucket using Invalid name")
    public void testCreateBucketUsingCapsName() {
        // load input files for each type and create the backend
        for (Type t : getTypesHolder().getTypes()) {
            List<File> listOfIInputsForType =
                    Utils.listFilesMatchingBeginsWithPatternInPath(t.getName(),
                            Constant.CREATE_BUCKET_PATH);
            Gson gson = new Gson();
            // add the backend specified in each file
            for (File file : listOfIInputsForType) {
                String content = Utils.readFileContentsAsString(file);
                assertNotNull(content);

                // backend added, now create buckets
                List<File> listOfIBucketInputs =
                        Utils.listFilesMatchingBeginsWithPatternInPath("bucket",
                                Constant.CREATE_BUCKET_PATH);
                SignatureKey signatureKey = getHttpHandler().getAkSkList(getAuthTokenHolder().getResponseHeaderSubjectToken(),
                        getAuthTokenHolder().getToken().getProject().getId());
                // create the bucket specified in each file
                for (File bucketFile : listOfIBucketInputs) {
                    String bucketContent = Utils.readFileContentsAsString(bucketFile);
                    assertNotNull(bucketContent);

                    // filename format is "bucket_<bucketname>.json", get the bucket name here
                    CreateBucketFileInput bfi = gson.fromJson(bucketContent, CreateBucketFileInput.class);

                    // now create buckets
                    int cbCode = getHttpHandler().createBucket(bfi, "RATR_@#", signatureKey);
                    assertEquals(cbCode, 400);
                }
            }
        }
    }

    @Test
    @Order(3)
    @DisplayName("Test re-creating backend with same name on OPENSDS")
    public void testReCreateBackend() {
        // load input files for each type and create the backend
        for (Type t : getTypesHolder().getTypes()) {
            List<File> listOfIInputsForType =
                    Utils.listFilesMatchingBeginsWithPatternInPath(t.getName(),
                            Constant.CREATE_BUCKET_PATH);
            Gson gson = new Gson();
            // Re-create backend specified in each file
            for (File file : listOfIInputsForType) {
                String content = Utils.readFileContentsAsString(file);
                assertNotNull(content);

                AddBackendInputHolder inputHolder = gson.fromJson(content, AddBackendInputHolder.class);
                int code = getHttpHandler().addBackend(getAuthTokenHolder().getResponseHeaderSubjectToken(),
                        getAuthTokenHolder().getToken().getProject().getId(),
                        inputHolder);
                assertEquals("Re-create backend with same name:Response code not matched:",code, 409);
            }
        }
    }

    @Test
    @Order(4)
    @DisplayName("Test re-creating bucket with same name on OPENSDS")
    public void testReCreateBucket() {
        // load input files for each type and create the backend
        for (Type t : getTypesHolder().getTypes()) {
            List<File> listOfIInputsForType =
                    Utils.listFilesMatchingBeginsWithPatternInPath(t.getName(),
                            Constant.CREATE_BUCKET_PATH);
            Gson gson = new Gson();
            // Re-create backend specified in each file
            for (File file : listOfIInputsForType) {
                String content = Utils.readFileContentsAsString(file);
                assertNotNull(content);
                List<File> listOfIBucketInputs =
                        Utils.listFilesMatchingBeginsWithPatternInPath("bucket",
                                Constant.CREATE_BUCKET_PATH);
                SignatureKey signatureKey = getHttpHandler().getAkSkList(getAuthTokenHolder().getResponseHeaderSubjectToken(),
                        getAuthTokenHolder().getToken().getProject().getId());
                // create the bucket specified in each file
                for (File bucketFile : listOfIBucketInputs) {
                    String bucketContent = Utils.readFileContentsAsString(bucketFile);
                    assertNotNull(bucketContent);

                    CreateBucketFileInput bfi = gson.fromJson(bucketContent, CreateBucketFileInput.class);

                    // filename format is "bucket_<bucketname>.json", get the bucket name here
                    String bName = bucketFile.getName().substring(bucketFile.getName().indexOf("_") + 1,
                            bucketFile.getName().indexOf("."));

                    // now create buckets
                    int cbCode = getHttpHandler().createBucket(bfi, bName, signatureKey);
                    assertEquals("Re-create bucket with same name failed:Response code not matched: "
                            , cbCode, 409);
                }
            }
        }
    }

    @Test
    @Order(5)
    @DisplayName("Test create bucket with empty name")
    public void testCreateBucketWithEmptyName() throws IOException, JSONException {
        System.out.println("Verifying response code: Input (Backend name) with empty value in payload and bucket" +
                " name is empty");
        SignatureKey signatureKey = getHttpHandler().getAkSkList(getAuthTokenHolder().getResponseHeaderSubjectToken(),
                getAuthTokenHolder().getToken().getProject().getId());
        File bucketFile = new File(Constant.EMPTY_FIELD_PATH, "bucket_emptyvalue.json");
        String bucketContent = Utils.readFileContentsAsString(bucketFile);
        assertNotNull(bucketContent);
        Gson gson = new Gson();
        CreateBucketFileInput bfi = gson.fromJson(bucketContent, CreateBucketFileInput.class);
        int cbCode = getHttpHandler().createBucket(bfi, "", signatureKey);
        assertEquals("Bucket name and backend name is empty in payload :Response code not matched: "
                , cbCode, 405);
        boolean isBucketExist = testGetListBuckets("", signatureKey);
        assertFalse(isBucketExist);

        File file = new File(Constant.EMPTY_FIELD_PATH, "bucket_b1324.json");
        String content = Utils.readFileContentsAsString(file);
        assertNotNull(content);
        String bName = file.getName().substring(bucketFile.getName().indexOf("_") + 1,
                bucketFile.getName().indexOf("."));

        CreateBucketFileInput input = gson.fromJson(content, CreateBucketFileInput.class);
        int code = getHttpHandler().createBucket(input, bName, signatureKey);
        System.out.println("Verifying response code: In input (Backend name) with not valid value but bucket name is valid");
        assertEquals("Backend does not exist:Response code not matched: "
                , code, 404);
    }

    @Test
    @Order(6)
    @DisplayName("Test request body with empty value,try to create backend")
    public void testRequestBodyWithEmptyFieldBackend() {
        Gson gson = new Gson();
        File file = new File(Constant.EMPTY_FIELD_PATH+"ibm-cos_b1321.json");
        String content = Utils.readFileContentsAsString(file);
        assertNotNull(content);
        AddBackendInputHolder inputHolder = gson.fromJson(content, AddBackendInputHolder.class);
        int code = getHttpHandler().addBackend(getAuthTokenHolder().getResponseHeaderSubjectToken(),
                getAuthTokenHolder().getToken().getProject().getId(),
                inputHolder);
        Logger.logObject("Backend Input: "+content);
        assertEquals("Request body with empty value:Response code not matched:",code, 400);
    }
}


