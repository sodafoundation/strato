import com.google.gson.Gson;
import com.opensds.jsonmodels.akskresponses.SignatureKey;
import com.opensds.jsonmodels.inputs.createmigration.DestConnInput;
import com.opensds.jsonmodels.inputs.createmigration.Filter;
import com.opensds.jsonmodels.inputs.createmigration.PlaneRequestInput;
import com.opensds.jsonmodels.inputs.createmigration.SourceConnInput;
import com.opensds.utils.Logger;
import com.opensds.utils.Utils;
import okhttp3.Response;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.jupiter.api.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.opensds.utils.Constant.*;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.*;

//how to get POJO from any response JSON, use this site
//http://pojo.sodhanalibrary.com/
//UTC time conversion
// https://savvytime.com/converter/utc-to-ist
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MigrationTests extends BaseTestClass {

    @Test
    @Order(1)
    @DisplayName("Test creating bucket and backend on OPENSDS")
    public void testCreateBucketAndBackend() throws IOException, JSONException {
        testCreateBucketAndBackend(CREATE_MIGRATION_PATH);
    }

    @Test
    @Order(2)
    @DisplayName("Test uploading object in a 1st bucket")
    public void testUploadObject() throws IOException, JSONException {
        List<File> listOfIBucketInputs = Utils.listFilesMatchingBeginsWithPatternInPath("bucket",
                        CREATE_MIGRATION_PATH);
        assertNotNull(listOfIBucketInputs);
        File bucketFile = listOfIBucketInputs.get(0);
        Logger.logObject(bucketFile);
        // Get bucket name.
        String bucketContent = Utils.readFileContentsAsString(bucketFile);
        assertNotNull(bucketContent);
        String bucketName =Utils.getBucketName(bucketFile);
        // Get object for upload.
        File fileRawData = new File(RAW_DATA_PATH);
        File[] files = fileRawData.listFiles();
        String mFileName;
        File mFilePath;
        for (File fileName : files) {
            mFileName = fileName.getName();
            mFilePath = fileName;

            SignatureKey signatureKey = getHttpHandler().getAkSkList(getAuthTokenHolder()
                            .getResponseHeaderSubjectToken(), getAuthTokenHolder().getToken()
                    .getProject().getId());
            int cbCode = getHttpHandler().uploadObject(signatureKey,
                    bucketName, mFileName, mFilePath);
            assertEquals("Uploaded object failed", cbCode, 200);

            //Verifying object is uploaded in bucket.
            boolean isUploaded = testGetListOfObjectFromBucket(bucketName, mFileName, signatureKey);
            assertTrue(isUploaded,"Object is not uploaded");
        }
    }

    @Test
    @Order(3)
    @DisplayName("Test creating plan with immediately")
    public void testCreatePlan() throws IOException, JSONException {
        Gson gson = new Gson();
        List<File> listOfIBucketInputs =
                Utils.listFilesMatchingBeginsWithPatternInPath("bucket", CREATE_MIGRATION_PATH);
        assertNotNull(listOfIBucketInputs);
        SourceConnInput sourceConnInput = new SourceConnInput();
        sourceConnInput.setBucketName(Utils.getBucketName(listOfIBucketInputs.get(0)));
        sourceConnInput.setStorType("opensds-obj");
        DestConnInput destConnInput = new DestConnInput();
        destConnInput.setBucketName(Utils.getBucketName(listOfIBucketInputs.get(1)));
        destConnInput.setStorType("opensds-obj");
        Filter filter = new Filter();
        PlaneRequestInput planeRequestInput = new PlaneRequestInput();
        planeRequestInput.setName(listOfIBucketInputs.get(0).getName()+"-Plan");
        planeRequestInput.setDescription("for test");
        planeRequestInput.setType("migration");
        planeRequestInput.setSourceConn(sourceConnInput);
        planeRequestInput.setDestConn(destConnInput);
        planeRequestInput.setFilter(filter);
        planeRequestInput.setRemainSource(true);
        String json = gson.toJson(planeRequestInput);
        Logger.logString("Request Json: "+json);

        Response response = getHttpHandler().createPlans(getAuthTokenHolder()
                .getResponseHeaderSubjectToken(), json, getAuthTokenHolder().getToken()
                .getProject().getId());
        String jsonRes = response.body().string();
        int code = response.code();
        Logger.logString("Response: "+jsonRes);
        Logger.logString("Response code: "+code);
        assertEquals("Plan creation failed: Response code not matched: ", code, 200);
        JSONObject jsonObject = new JSONObject(jsonRes);

        String id  = jsonObject.getJSONObject("plan").get("id").toString();
        assertNotNull(id,"Id is null: ");

        Response responseRun = getHttpHandler().runPlans(getAuthTokenHolder()
                .getResponseHeaderSubjectToken(), id, getAuthTokenHolder().getToken()
                .getProject().getId());
        String jsonResRun = responseRun.body().string();
        int codeRun = responseRun.code();
        Logger.logString("Response: "+jsonResRun);
        Logger.logString("Response code: "+codeRun);
        assertEquals("Run plan creation failed: Response code not matched: ", codeRun, 200);
        String jobId = new JSONObject(jsonResRun).get("jobId").toString();

        Response responseGetJob = getHttpHandler().getJob(getAuthTokenHolder()
                .getResponseHeaderSubjectToken(), jobId, getAuthTokenHolder().getToken()
                .getProject().getId());
        String jsonResGetJob = responseGetJob.body().string();
        int codeGetJob = responseGetJob.code();
        Logger.logString("Response: "+jsonResGetJob);
        Logger.logString("Response code: "+codeGetJob);
        assertEquals("Get job id failed: Response code not matched: ", codeGetJob, 200);
        String status = new JSONObject(jsonResGetJob).getJSONObject("job").get("status").toString();
        Logger.logString("Status: "+ status);
    }

    @Test
    @Order(4)
    @DisplayName("Test creating plan with immediately using empty request body")
    public void testCreatePlanUsingEmptyRequestBody() throws IOException {
        Gson gson = new Gson();
        List<File> listOfIBucketInputs =
                Utils.listFilesMatchingBeginsWithPatternInPath("bucket", CREATE_MIGRATION_PATH);
        assertNotNull(listOfIBucketInputs);
        SourceConnInput sourceConnInput = new SourceConnInput();
        sourceConnInput.setBucketName("");
        sourceConnInput.setStorType("");
        DestConnInput destConnInput = new DestConnInput();
        destConnInput.setBucketName("");
        destConnInput.setStorType("");
        Filter filter = new Filter();
        PlaneRequestInput planeRequestInput = new PlaneRequestInput();
        planeRequestInput.setName("");
        planeRequestInput.setDescription("");
        planeRequestInput.setType("");
        planeRequestInput.setSourceConn(sourceConnInput);
        planeRequestInput.setDestConn(destConnInput);
        planeRequestInput.setFilter(filter);
        planeRequestInput.setRemainSource(true);
        String json = gson.toJson(planeRequestInput);
        Logger.logString("Request Json: "+json);

        Response response = getHttpHandler().createPlans(getAuthTokenHolder()
                .getResponseHeaderSubjectToken(), json, getAuthTokenHolder().getToken()
                .getProject().getId());
        String jsonRes = response.body().string();
        int code = response.code();
        Logger.logString("Response: "+jsonRes);
        Logger.logString("Response code: "+code);
        assertEquals("Plan creation failed request body empty: Response code not matched: ", code, 400);
    }

    @Test
    @Order(5)
    @DisplayName("Test after migration download image from source and destination bucket")
    public void testSourceDesBucketDownloadObject() throws IOException {
        testDownloadObject(CREATE_MIGRATION_PATH, "Migration_obj.jpg");
    }

    @Test
    @Order(6)
    @DisplayName("Test re-creating plan with immediately using same name")
    public void testReCreatePlan() throws IOException, JSONException {
        Gson gson = new Gson();
        List<File> listOfIBucketInputs =
                Utils.listFilesMatchingBeginsWithPatternInPath("bucket", CREATE_MIGRATION_PATH);
        assertNotNull(listOfIBucketInputs);
        SourceConnInput sourceConnInput = new SourceConnInput();
        sourceConnInput.setBucketName(Utils.getBucketName(listOfIBucketInputs.get(0)));
        sourceConnInput.setStorType("opensds-obj");
        DestConnInput destConnInput = new DestConnInput();
        destConnInput.setBucketName(Utils.getBucketName(listOfIBucketInputs.get(1)));
        destConnInput.setStorType("opensds-obj");
        Filter filter = new Filter();
        PlaneRequestInput planeRequestInput = new PlaneRequestInput();
        planeRequestInput.setName(listOfIBucketInputs.get(0).getName()+"-Plan");
        planeRequestInput.setDescription("for test");
        planeRequestInput.setType("migration");
        planeRequestInput.setSourceConn(sourceConnInput);
        planeRequestInput.setDestConn(destConnInput);
        planeRequestInput.setFilter(filter);
        planeRequestInput.setRemainSource(true);
        String json = gson.toJson(planeRequestInput);

        Response response = getHttpHandler().createPlans(getAuthTokenHolder()
                .getResponseHeaderSubjectToken(), json, getAuthTokenHolder().getToken()
                .getProject().getId());
        String jsonRes = response.body().string();
        int code = response.code();
        testGetPlansListAndDelete();
        Logger.logString("Response: "+jsonRes);
        Logger.logString("Response code: "+code);
        assertEquals("Plan already created: Response code not matched: ", code, 409);
    }

    @Test
    @Order(7)
    @DisplayName("Test creating plan with immediately using invalid plan id")
    public void testCreatePlanInvalidPlanId() throws IOException, JSONException {
        Gson gson = new Gson();
        List<File> listOfIBucketInputs =
                Utils.listFilesMatchingBeginsWithPatternInPath("bucket", CREATE_MIGRATION_PATH);
        assertNotNull(listOfIBucketInputs);
        SourceConnInput sourceConnInput = new SourceConnInput();
        sourceConnInput.setBucketName(Utils.getBucketName(listOfIBucketInputs.get(0)));
        sourceConnInput.setStorType("opensds-obj");
        DestConnInput destConnInput = new DestConnInput();
        destConnInput.setBucketName(Utils.getBucketName(listOfIBucketInputs.get(1)));
        destConnInput.setStorType("opensds-obj");
        Filter filter = new Filter();
        PlaneRequestInput planeRequestInput = new PlaneRequestInput();
        planeRequestInput.setName(listOfIBucketInputs.get(0).getName()+"-Plan");
        planeRequestInput.setDescription("for test");
        planeRequestInput.setType("migration");
        planeRequestInput.setSourceConn(sourceConnInput);
        planeRequestInput.setDestConn(destConnInput);
        planeRequestInput.setFilter(filter);
        planeRequestInput.setRemainSource(true);
        String json = gson.toJson(planeRequestInput);

        Response response = getHttpHandler().createPlans(getAuthTokenHolder()
                .getResponseHeaderSubjectToken(), json, getAuthTokenHolder().getToken()
                .getProject().getId());
        String jsonRes = response.body().string();
        int code = response.code();
        Logger.logString("Response: "+jsonRes);
        Logger.logString("Response code: "+code);
        assertEquals("Plan creation failed: Response code not matched: ", code, 200);
        JSONObject jsonObject = new JSONObject(jsonRes);

        String id  = jsonObject.getJSONObject("plan").get("id").toString();
        assertNotNull(id,"Id is null: ");
        id = "234567887"; // Intercept with this value

        Response responseRun = getHttpHandler().runPlans(getAuthTokenHolder()
                .getResponseHeaderSubjectToken(), id, getAuthTokenHolder().getToken()
                .getProject().getId());
        String jsonResRun = responseRun.body().string();
        int codeRun = responseRun.code();
        testGetPlansListAndDelete();
        Logger.logString("Response: "+jsonResRun);
        Logger.logString("Response code: "+codeRun);
        assertEquals("Run plan creation failed with invalid id: Response code not matched: ", codeRun, 403);
    }

    @Test
    @Order(8)
    @DisplayName("Test creating plan with immediately using invalid job id")
    public void testCreatePlanInvalidJobId() throws IOException, JSONException {
        Gson gson = new Gson();
        List<File> listOfIBucketInputs =
                Utils.listFilesMatchingBeginsWithPatternInPath("bucket", CREATE_MIGRATION_PATH);
        assertNotNull(listOfIBucketInputs);
        SourceConnInput sourceConnInput = new SourceConnInput();
        sourceConnInput.setBucketName(Utils.getBucketName(listOfIBucketInputs.get(0)));
        sourceConnInput.setStorType("opensds-obj");
        DestConnInput destConnInput = new DestConnInput();
        destConnInput.setBucketName(Utils.getBucketName(listOfIBucketInputs.get(1)));
        destConnInput.setStorType("opensds-obj");
        Filter filter = new Filter();
        PlaneRequestInput planeRequestInput = new PlaneRequestInput();
        planeRequestInput.setName(listOfIBucketInputs.get(0).getName()+"-Plan");
        planeRequestInput.setDescription("for test");
        planeRequestInput.setType("migration");
        planeRequestInput.setSourceConn(sourceConnInput);
        planeRequestInput.setDestConn(destConnInput);
        planeRequestInput.setFilter(filter);
        planeRequestInput.setRemainSource(true);
        String json = gson.toJson(planeRequestInput);

        Response response = getHttpHandler().createPlans(getAuthTokenHolder()
                .getResponseHeaderSubjectToken(), json, getAuthTokenHolder().getToken()
                .getProject().getId());
        String jsonRes = response.body().string();
        int code = response.code();
        Logger.logString("Response: "+jsonRes);
        Logger.logString("Response code: "+code);
        assertEquals("Plan creation failed: Response code not matched: ", code, 200);
        JSONObject jsonObject = new JSONObject(jsonRes);

        String id  = jsonObject.getJSONObject("plan").get("id").toString();
        assertNotNull(id,"Id is null: ");

        Response responseRun = getHttpHandler().runPlans(getAuthTokenHolder()
                .getResponseHeaderSubjectToken(), id, getAuthTokenHolder().getToken()
                .getProject().getId());
        String jsonResRun = responseRun.body().string();
        int codeRun = responseRun.code();
        Logger.logString("Response: "+jsonResRun);
        Logger.logString("Response code: "+codeRun);
        assertEquals("Run plan creation failed: Response code not matched: ", codeRun, 200);
        String jobId = new JSONObject(jsonResRun).get("jobId").toString();
        assertNotNull(jobId,"Job id is null: ");
        jobId= "0384756565";

        Response responseGetJob = getHttpHandler().getJob(getAuthTokenHolder()
                .getResponseHeaderSubjectToken(), jobId, getAuthTokenHolder().getToken()
                .getProject().getId());
        String jsonResGetJob = responseGetJob.body().string();
        int codeGetJob = responseGetJob.code();
        testGetPlansListAndDelete();
        Logger.logString("Response: "+jsonResGetJob);
        Logger.logString("Response code: "+codeGetJob);
        assertEquals("Job id may be valid: Response code not matched: ", codeGetJob, 403);
    }

    @Test
    @Order(9)
    @DisplayName("Test creating plan with immediately and delete the source objects after the migration is completed")
    public void testCreatePlanDeleteSourceObject() throws IOException, JSONException {
        Gson gson = new Gson();
        List<File> listOfIBucketInputs =
                Utils.listFilesMatchingBeginsWithPatternInPath("bucket", CREATE_MIGRATION_PATH);
        assertNotNull(listOfIBucketInputs);
        SourceConnInput sourceConnInput = new SourceConnInput();
        sourceConnInput.setBucketName(Utils.getBucketName(listOfIBucketInputs.get(0)));
        sourceConnInput.setStorType("opensds-obj");
        DestConnInput destConnInput = new DestConnInput();
        destConnInput.setBucketName(Utils.getBucketName(listOfIBucketInputs.get(1)));
        destConnInput.setStorType("opensds-obj");
        Filter filter = new Filter();
        PlaneRequestInput planeRequestInput = new PlaneRequestInput();
        planeRequestInput.setName(listOfIBucketInputs.get(0).getName()+"-Plan");
        planeRequestInput.setDescription("for test");
        planeRequestInput.setType("migration");
        planeRequestInput.setSourceConn(sourceConnInput);
        planeRequestInput.setDestConn(destConnInput);
        planeRequestInput.setFilter(filter);
        planeRequestInput.setRemainSource(false);
        String json = gson.toJson(planeRequestInput);
        Logger.logString("Source bucket: "+Utils.getBucketName(listOfIBucketInputs.get(0)));
        Logger.logString("Destination bucket: "+Utils.getBucketName(listOfIBucketInputs.get(1)));

        Response response = getHttpHandler().createPlans(getAuthTokenHolder()
                .getResponseHeaderSubjectToken(), json, getAuthTokenHolder().getToken()
                .getProject().getId());
        String jsonRes = response.body().string();
        int code = response.code();
        Logger.logString("Response: "+jsonRes);
        Logger.logString("Response code: "+code);
        assertEquals("Plan creation failed: Response code not matched: ", code, 200);
        JSONObject jsonObject = new JSONObject(jsonRes);

        String id  = jsonObject.getJSONObject("plan").get("id").toString();
        assertNotNull(id,"Id is null: ");

        Response responseRun = getHttpHandler().runPlans(getAuthTokenHolder()
                .getResponseHeaderSubjectToken(), id, getAuthTokenHolder().getToken()
                .getProject().getId());
        String jsonResRun = responseRun.body().string();
        int codeRun = responseRun.code();
        Logger.logString("Response: "+jsonResRun);
        Logger.logString("Response code: "+codeRun);
        assertEquals("Run plan creation failed: Response code not matched: ", codeRun, 200);
        String jobId = new JSONObject(jsonResRun).get("jobId").toString();

        Response responseGetJob = getHttpHandler().getJob(getAuthTokenHolder()
                .getResponseHeaderSubjectToken(), jobId, getAuthTokenHolder().getToken()
                .getProject().getId());
        String jsonResGetJob = responseGetJob.body().string();
        int codeGetJob = responseGetJob.code();
        Logger.logString("Response: "+jsonResGetJob);
        Logger.logString("Response code: "+codeGetJob);
        assertEquals("Get job id failed: Response code not matched: ", codeGetJob, 200);
        String status = new JSONObject(jsonResGetJob).getJSONObject("job").get("status").toString();
        Logger.logString("Status: "+ status);
    }

    @Test
    @Order(10)
    @DisplayName("Test delete the source objects after migration download image from destination bucket")
    public void testDesBucketDownloadObject() throws IOException {
        List<File> listOfIBucketInputs = Utils.listFilesMatchingBeginsWithPatternInPath("bucket",
                CREATE_MIGRATION_PATH);
        assertNotNull(listOfIBucketInputs);
        for (File bucketFile: listOfIBucketInputs) {
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
            String fileName = bucketName+"Migration_obj.jpg";
            File filePath = new File(DOWNLOAD_FILES_PATH);
            File downloadedFile = new File(DOWNLOAD_FILES_PATH, fileName);
            if (filePath.exists()) {
                if (downloadedFile.exists()) {
                    Logger.logString(" Download Image Path: "+downloadedFile);
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
            Response response = getHttpHandler().downloadObject(signatureKey, bucketName, mFileName, fileName);
            int code = response.code();
            String body = response.body().string();
            Logger.logString("Response Code: " + code);
            Logger.logString("Response: " + body);
            assertTrue(code == 200 || code == 404, "Downloading failed: ");
            Logger.logString("Bucket Name: "+bucketName+" Response Code: "+code);
            if (code == 200) {
                assertTrue(downloadedFile.isFile(), "Downloaded Image is not available");
            }
            if (code == 404){
                assertFalse(downloadedFile.isFile(), "Downloaded Image is available");
            }
        }
    }
}
