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
}
