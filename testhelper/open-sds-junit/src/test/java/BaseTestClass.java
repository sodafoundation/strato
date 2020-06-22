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
import org.junit.jupiter.api.Assertions;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.opensds.utils.Constant.*;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.*;

public class BaseTestClass {
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

    public void testCreateBucketAndBackend(String path) throws IOException, JSONException {
        // load input files for each type and create the backend
        for (Type t : getTypesHolder().getTypes()) {
            List<File> listOfIInputsForType =
                    Utils.listFilesMatchingBeginsWithPatternInPath(t.getName(), path);
            Gson gson = new Gson();
            // add the backend specified in each file
            for (File file : listOfIInputsForType) {
                String content = Utils.readFileContentsAsString(file);
                Assertions.assertNotNull(content);

                AddBackendInputHolder inputHolder = gson.fromJson(content, AddBackendInputHolder.class);
                int code = getHttpHandler().addBackend(getAuthTokenHolder().getResponseHeaderSubjectToken(),
                        getAuthTokenHolder().getToken().getProject().getId(),
                        inputHolder);
                assertEquals(code, 200);

                // backend added, now create buckets
                List<File> listOfIBucketInputs =
                        Utils.listFilesMatchingBeginsWithPatternInPath("bucket", path);
                SignatureKey signatureKey = getHttpHandler().getAkSkList(getAuthTokenHolder().getResponseHeaderSubjectToken(),
                        getAuthTokenHolder().getToken().getProject().getId());
                // create the bucket specified in each file
                for (File bucketFile : listOfIBucketInputs) {
                    String bucketContent = Utils.readFileContentsAsString(bucketFile);
                    Assertions.assertNotNull(bucketContent);

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

    public void testDownloadObject(String path, String objectName) throws IOException {
        List<File> listOfIBucketInputs =
                Utils.listFilesMatchingBeginsWithPatternInPath("bucket", path);
        for (File bucketFile : listOfIBucketInputs) {
            String bucketContent = Utils.readFileContentsAsString(bucketFile);
            assertNotNull(bucketContent);
            String bucketName = Utils.getBucketName(bucketFile);
            // Get object for upload.
            File fileRawData = new File(RAW_DATA_PATH);
            File[] files = fileRawData.listFiles();
            String mFileName = null;
            for (File fileName : files) {
                mFileName = fileName.getName();
            }
            String fileName = bucketName+objectName;
            File filePath = new File(DOWNLOAD_FILES_PATH);
            File downloadedFile = new File(DOWNLOAD_FILES_PATH, fileName);
            if (filePath.exists()) {
                if (downloadedFile.exists()) {
                    boolean isDownloadedFileDeleted = downloadedFile.delete();
                    assertTrue(isDownloadedFileDeleted, "Image deleting is failed");
                } else {
                    assertFalse(downloadedFile.exists());
                }
            } else {
                filePath.mkdirs();
            }
            SignatureKey signatureKey = getHttpHandler().getAkSkList(getAuthTokenHolder().getResponseHeaderSubjectToken(),
                    getAuthTokenHolder().getToken().getProject().getId());
            Response response = getHttpHandler().downloadObject(signatureKey,
                    bucketName, mFileName, fileName);
            int code = response.code();
            String body = response.body().string();
            Logger.logString("Response Code: " + code);
            Logger.logString("Response: " + body);
            assertEquals("Downloading failed", code, 200);
            assertTrue(downloadedFile.isFile(), "Downloaded Image is not available");
        }
    }

    public void testUploadObject(String path) throws IOException, JSONException {
        List<File> listOfIBucketInputs =
                Utils.listFilesMatchingBeginsWithPatternInPath("bucket", path);
        // Get bucket name.
        for (File bucketFile : listOfIBucketInputs) {
            String bucketContent = Utils.readFileContentsAsString(bucketFile);
            assertNotNull(bucketContent);
            String bucketName = Utils.getBucketName(bucketFile);
            // Get object for upload.
            File fileRawData = new File(Constant.RAW_DATA_PATH);
            File[] files = fileRawData.listFiles();
            String mFileName;
            File mFilePath;
            for (File fileName : files) {
                mFileName = fileName.getName();
                mFilePath = fileName;
                SignatureKey signatureKey = getHttpHandler().getAkSkList(getAuthTokenHolder().getResponseHeaderSubjectToken(),
                        getAuthTokenHolder().getToken().getProject().getId());
                int cbCode = getHttpHandler().uploadObject(signatureKey,
                        bucketName, mFileName, mFilePath);
                assertEquals("Uploaded object failed", cbCode, 200);
                //Verifying object is uploaded in bucket.
                boolean isUploaded = testGetListOfObjectFromBucket(bucketName, mFileName, signatureKey);
                assertTrue(isUploaded,"Object is not uploaded");
            }
        }
    }

    /**
     * Get bucket list.
     *
     * @param bName Bucket name
     * @param signatureKey Signature key object
     * @return boolean If bucket is exist return true else false
     * @throws JSONException Json exception
     * @throws IOException Io exception
     */
    protected boolean testGetListBuckets(String bName, SignatureKey signatureKey)
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

    /**
     * Get List of object from bucket
     *
     * @param bucketName   bucket name
     * @param fileName     file name
     * @param signatureKey signature key object
     * @return boolean object is upload then true else false
     * @throws JSONException json exception
     * @throws IOException   io exception
     */
    protected boolean testGetListOfObjectFromBucket(String bucketName, String fileName,
                                                    SignatureKey signatureKey)
            throws JSONException, IOException {
        Response listObjectResponse = getHttpHandler().getBucketObjects(bucketName, signatureKey);
        int resCode = listObjectResponse.code();
        String resBody = listObjectResponse.body().string();
        Logger.logString("Response Code: " + resCode);
        Logger.logString("Response: " + resBody);
        assertEquals("Get list of object failed", resCode, 200);
        JSONObject jsonObject = XML.toJSONObject(resBody);
        JSONObject jsonObjectListBucket = jsonObject.getJSONObject("ListBucketResult");
        boolean isUploaded = false;
        if (jsonObjectListBucket.has("Contents")) {
            if (jsonObjectListBucket.get("Contents") instanceof JSONArray) {
                JSONArray objects = jsonObjectListBucket.getJSONArray("Contents");
                for (int i = 0; i < objects.length(); i++) {
                    if (!TextUtils.isEmpty(objects.getJSONObject(i).get("Key").toString())) {
                        if (objects.getJSONObject(i).get("Key").toString().equals(fileName)) {
                            isUploaded = true;
                        }
                    }
                }
            } else {
                if (!TextUtils.isEmpty(jsonObjectListBucket.getJSONObject("Contents")
                        .get("Key").toString())) {
                    if (jsonObjectListBucket.getJSONObject("Contents").get("Key").toString()
                            .equals(fileName)) {
                        isUploaded = true;
                    }
                }
            }
        }
        return isUploaded;
    }
}
