# Changelog

## [v1.1.0](https://github.com/sodafoundation/multi-cloud/tree/v1.1.0) (2020-09-29)

[Full Changelog](https://github.com/sodafoundation/multi-cloud/compare/v1.0.1...v1.1.0)

**Closed issues:**

- \[AWS Block Support\] Invalid Error message in the response for the failed operations [\#1116](https://github.com/sodafoundation/multi-cloud/issues/1116)
- \[AWS Block Support\] Invalid memory address or nil pointer dereference for AWS Volume Updation [\#1114](https://github.com/sodafoundation/multi-cloud/issues/1114)
- \[AWS Block Support\] Invalid memory address or nil pointer dereference for AWS Volume Creation [\#1113](https://github.com/sodafoundation/multi-cloud/issues/1113)
- \[GCP FileShare Support\] Invalid memory address or nil pointer dereference for GCP Fileshare Updation [\#1106](https://github.com/sodafoundation/multi-cloud/issues/1106)
- \[GCP FileShare Support\] Invalid memory address or nil pointer dereference for GCP Fileshare Creation [\#1104](https://github.com/sodafoundation/multi-cloud/issues/1104)
- \[Multi-Cloud Utility Support\] Need to encrypt plain text Cloud Backend Credentials [\#1102](https://github.com/sodafoundation/multi-cloud/issues/1102)
- List Cloud volumes from AWS backend. Size is returned in GiB but expecting in bytes in API response [\#1092](https://github.com/sodafoundation/multi-cloud/issues/1092)
- Add UT for multi-cloud api , block [\#1084](https://github.com/sodafoundation/multi-cloud/issues/1084)
- \[GCP FileShare Support\] Implement GCP FileShare Update API [\#1080](https://github.com/sodafoundation/multi-cloud/issues/1080)
- \[GCP FileShare Support\] Implement GCP FileShare Delete API [\#1079](https://github.com/sodafoundation/multi-cloud/issues/1079)
- \[GCP FileShare Support\] Implement GCP FileShare GET API [\#1078](https://github.com/sodafoundation/multi-cloud/issues/1078)
- \[GCP FileShare Support\] Implement GCP FileShare List API [\#1077](https://github.com/sodafoundation/multi-cloud/issues/1077)

**Merged pull requests:**

- \[Multi-Cloud API Documentation\] Update Swagger for Block CRUD APIs [\#1122](https://github.com/sodafoundation/multi-cloud/pull/1122) ([himanshuvar](https://github.com/himanshuvar))
- \[AWS Block Support\] Fix to Update Error return Typos for failed Volume operation at Backend [\#1118](https://github.com/sodafoundation/multi-cloud/pull/1118) ([himanshuvar](https://github.com/himanshuvar))
- Extend the travis wait time if the docker commands taking more time. … [\#1117](https://github.com/sodafoundation/multi-cloud/pull/1117) ([kumarashit](https://github.com/kumarashit))
- \[AWS Block Service\] Bug-Fix: Added check for nil Volume return for Creation/Updation [\#1115](https://github.com/sodafoundation/multi-cloud/pull/1115) ([himanshuvar](https://github.com/himanshuvar))
- \[GCP FileShare Support\] Fix for Invalid memory address or nil pointer dereference for GCP Fileshare Updation [\#1112](https://github.com/sodafoundation/multi-cloud/pull/1112) ([himanshuvar](https://github.com/himanshuvar))
- \[GCP FileShare Support\] Fix for Invalid memory address or nil pointer dereference for GCP Fileshare Creation [\#1105](https://github.com/sodafoundation/multi-cloud/pull/1105) ([himanshuvar](https://github.com/himanshuvar))
- \[Multi-Cloud Utility Support\] Encrypter API Support for Multi-cloud backend credentials [\#1101](https://github.com/sodafoundation/multi-cloud/pull/1101) ([himanshuvar](https://github.com/himanshuvar))
- Add multi-cloud block unit tests [\#1096](https://github.com/sodafoundation/multi-cloud/pull/1096) ([rajat-soda](https://github.com/rajat-soda))

## [v1.0.1](https://github.com/sodafoundation/multi-cloud/tree/v1.0.1) (2020-09-21)

[Full Changelog](https://github.com/sodafoundation/multi-cloud/compare/v1.0.0...v1.0.1)

**Closed issues:**

- \[GCP FileShare Support\] Implement Crypter for GCP Fileshare Credentials [\#1087](https://github.com/sodafoundation/multi-cloud/issues/1087)
- \[GCP FileShare Support\] Implement GCP Authentication Framework Support [\#1086](https://github.com/sodafoundation/multi-cloud/issues/1086)
- Add UT for multi-cloud/fileshare , backend [\#1083](https://github.com/sodafoundation/multi-cloud/issues/1083)
- \[GCP FileShare Support\] Implement GCP FileShare Create API [\#1076](https://github.com/sodafoundation/multi-cloud/issues/1076)
- \[GCP FileShare Support\] Add GCP Driver Adapter Framework [\#1075](https://github.com/sodafoundation/multi-cloud/issues/1075)
- \[GCP FileShare Support\] Explore GCP Authentication mechanism [\#1074](https://github.com/sodafoundation/multi-cloud/issues/1074)
- Documentation issue in Multi-cloud Local Cluster Installation and Testing [\#1069](https://github.com/sodafoundation/multi-cloud/issues/1069)
- Fileshare support for GCP [\#1065](https://github.com/sodafoundation/multi-cloud/issues/1065)
- Add UTs and Code coverage as part of CI [\#1057](https://github.com/sodafoundation/multi-cloud/issues/1057)
- Request TimeOut for FileShare APIs [\#1052](https://github.com/sodafoundation/multi-cloud/issues/1052)
- For Alibaba Multi-part object \( Object size \> 5MB\), lifecycle transition failed [\#1045](https://github.com/sodafoundation/multi-cloud/issues/1045)
- Alibaba upload object sdk do not support Create folder [\#1044](https://github.com/sodafoundation/multi-cloud/issues/1044)
- DELETE Volume API Support for AWS [\#1043](https://github.com/sodafoundation/multi-cloud/issues/1043)
- PUT Volume API Support for AWS [\#1042](https://github.com/sodafoundation/multi-cloud/issues/1042)
- Enhancements to Create Volume API Support for AWS [\#1041](https://github.com/sodafoundation/multi-cloud/issues/1041)
- Enhancements to GET Volume API Support for AWS [\#1040](https://github.com/sodafoundation/multi-cloud/issues/1040)
- Enhancements to List Volumes API Support for AWS [\#1039](https://github.com/sodafoundation/multi-cloud/issues/1039)
- AWS Block Driver Support [\#1038](https://github.com/sodafoundation/multi-cloud/issues/1038)
- Get fileshare detail in Azure [\#995](https://github.com/sodafoundation/multi-cloud/issues/995)
- Create Fileshare in Azure [\#992](https://github.com/sodafoundation/multi-cloud/issues/992)
- Update the metadata or Tag of the Fileshare created in AWS [\#991](https://github.com/sodafoundation/multi-cloud/issues/991)
- Get particular fileshare details from AWS [\#990](https://github.com/sodafoundation/multi-cloud/issues/990)
- Add Create Fileshare for AWS [\#988](https://github.com/sodafoundation/multi-cloud/issues/988)
- DB Framework Support for Block Services. [\#963](https://github.com/sodafoundation/multi-cloud/issues/963)
- Define Data Model i.e. Volume for Block Support [\#962](https://github.com/sodafoundation/multi-cloud/issues/962)
- Unable to init multi-part upload [\#955](https://github.com/sodafoundation/multi-cloud/issues/955)
- List API for Block devices in AWS \[GET\] [\#944](https://github.com/sodafoundation/multi-cloud/issues/944)
- S3 APIs are not AWS S3 compatible. [\#893](https://github.com/sodafoundation/multi-cloud/issues/893)

**Merged pull requests:**

- \[AWS Block Support\] Update AWS Volume Size to take inputs as bytes [\#1100](https://github.com/sodafoundation/multi-cloud/pull/1100) ([himanshuvar](https://github.com/himanshuvar))
- \[GCP FileShare Support\] Implement Fileshare CRUD API Support for GCP Fileshare Driver [\#1099](https://github.com/sodafoundation/multi-cloud/pull/1099) ([himanshuvar](https://github.com/himanshuvar))
- \[GCP FileShare Support\]  Implement Create Fileshare API for GCP Fileshare Driver [\#1097](https://github.com/sodafoundation/multi-cloud/pull/1097) ([himanshuvar](https://github.com/himanshuvar))
- \[GCP FileShare Support\] Implement GCP FileShare Framework [\#1089](https://github.com/sodafoundation/multi-cloud/pull/1089) ([himanshuvar](https://github.com/himanshuvar))
- Add UT coverage for multi-cloud/api pkg [\#1085](https://github.com/sodafoundation/multi-cloud/pull/1085) ([rajat-soda](https://github.com/rajat-soda))
- Add test and code coverage scripts for file , backend [\#1082](https://github.com/sodafoundation/multi-cloud/pull/1082) ([rajat-soda](https://github.com/rajat-soda))
- Documentation issue fix  in Multi-cloud Local Cluster Installation and Testing [\#1071](https://github.com/sodafoundation/multi-cloud/pull/1071) ([vineela1999](https://github.com/vineela1999))
- Update README.md [\#1068](https://github.com/sodafoundation/multi-cloud/pull/1068) ([kumarashit](https://github.com/kumarashit))
- Add Code coverage for pkg with tests [\#1067](https://github.com/sodafoundation/multi-cloud/pull/1067) ([rajat-soda](https://github.com/rajat-soda))
- File Share Create & Update API Refactoring for Sync DB using go routine [\#1055](https://github.com/sodafoundation/multi-cloud/pull/1055) ([himanshuvar](https://github.com/himanshuvar))
- Enhancement to Multi-Cloud Block Support for AWS Create/List/Get/Update/Delete [\#1034](https://github.com/sodafoundation/multi-cloud/pull/1034) ([himanshuvar](https://github.com/himanshuvar))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
