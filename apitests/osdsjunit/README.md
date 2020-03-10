# OpenSDS multicloud API test automation framework

This is a framework build in JAVA. It leverages JUNIT for running and reporting.
For multi-cloud (mc), our test sequence is fairly similar for all tests, like below, with an example of create bucket and ## upload object

> Create bucket

1. Call add backend API, it should return success
2. Call the create bucket API on OSDS
3. Check for success
4. Now, call the list buckets API on OSDS
5. Response should include the newly created bucket
6. Declare success/failure

> Upload object

1. Call add backend API, it should return success
2. Call the create bucket API on OSDS
3. Check for success
4. Now, call Upload Object API on OSDS
5. Check for success
6. If success, check for the object in the cloud backend, using cloud backend API
7. It should exist on the backend
8. If 5 is success and 7 is success, declare success
9. else declare failure


## Now, this framework will let us

- Use our multicloud API as is to create test cases
- Use JSON/XML inputs and responses, convert them to POJO so results can be checked
- Read test inputs from .json files, run the same test case many times with different data
- Generate basic HTML reports and Google sheets reports to persist
- Can run on any build as long as <IP>:8089 is known
- Once we have enough test cases, we can also get code coverage
- Can be integrated with CI at a later date

