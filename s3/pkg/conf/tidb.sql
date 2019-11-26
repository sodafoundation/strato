-- MySQL dump 10.14  Distrib 5.5.56-MariaDB, for Linux (x86_64)
--
-- Host: 10.5.0.17    Database: s3
-- ------------------------------------------------------
-- Server version	5.7.10-TiDB-v2.0.0-rc.1-71-g7f958c5

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `buckets`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE IF NOT EXISTS `buckets` (
  `bucketname` varchar(255) NOT NULL DEFAULT '',
  `tenantid` varchar(255) DEFAULT NULL,
  `userid` varchar(255) DEFAULT NULL,
  `createtime` datetime DEFAULT NULL,
  `usages` bigint(20) DEFAULT NULL,
  `location` varchar(255) DEFAULT NULL,
  `deleted` boolean DEFAULT FALSE,
  `tier` int(11) DEFAULT 1,
  `acl` JSON DEFAULT NULL,
  `cors` JSON DEFAULT NULL,
  `lc` JSON DEFAULT NULL,
  `policy` JSON DEFAULT NULL,
  `versioning` varchar(255) DEFAULT NULL,
  `replication` JSON DEFAULT NULL,
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`bucketname`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `cluster`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE IF NOT EXISTS `cluster` (
  `fsid` varchar(255) DEFAULT NULL,
  `pool` varchar(255) DEFAULT NULL,
  `weight` int(11) DEFAULT NULL,
   UNIQUE KEY `rowkey` (`fsid`,`pool`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `gc`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE IF NOT EXISTS `gc` (
  `bucketname` varchar(255) DEFAULT NULL,
  `objectname` varchar(255) DEFAULT NULL,
  `version` bigint(20) UNSIGNED DEFAULT NULL,
  `location` varchar(255) DEFAULT NULL,
  `storagemeta` varchar(255) DEFAULT NULL,
  `objectid` varchar(255) DEFAULT NULL,
  `status` varchar(255) DEFAULT NULL,
  `mtime` datetime DEFAULT NULL,
  `part` tinyint(1) DEFAULT NULL,
  `triedtimes` int(11) DEFAULT NULL,
   UNIQUE KEY `rowkey` (`bucketname`,`objectname`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `gcpart`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE IF NOT EXISTS `gcpart` (
  `partnumber` int(11) DEFAULT NULL,
  `size` bigint(20) DEFAULT NULL,
  `objectid` varchar(255) DEFAULT NULL,
  `offset` bigint(20) DEFAULT NULL,
  `etag` varchar(255) DEFAULT NULL,
  `lastmodified` datetime DEFAULT NULL,
  `initializationvector` blob DEFAULT NULL,
  `bucketname` varchar(255) DEFAULT NULL,
  `objectname` varchar(255) DEFAULT NULL,
  `version` bigint(20) UNSIGNED DEFAULT NULL,
   KEY `rowkey` (`bucketname`,`objectname`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `multiparts`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE IF NOT EXISTS `multiparts` (
  `bucketname` varchar(255) DEFAULT NULL,
  `objectname` varchar(255) DEFAULT NULL,
  `uploadid` varchar(255) DEFAULT NULL,
  `uploadtime` datetime DEFAULT NULL,
  `initiatorid` varchar(255) DEFAULT NULL,
  `tenantid` varchar(255) DEFAULT NULL,
  `userid` varchar(255) DEFAULT NULL,
  `contenttype` varchar(255) DEFAULT NULL,
  `location` varchar(255) DEFAULT NULL,
  `acl` JSON DEFAULT NULL,
  `attrs` JSON DEFAULT NULL,
  `objectid` varchar(255) NOT NULL DEFAULT "",
  `storageMeta` varchar(255) NOT NULL DEFAULT "",
  `storageclass` tinyint(1) DEFAULT 0,
  UNIQUE KEY `rowkey` (`bucketname`,`objectname`,`uploadid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `objects`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE IF NOT EXISTS `objects` (
  `bucketname` varchar(255) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `version` bigint(20) UNSIGNED DEFAULT NULL,
  `location` varchar(255) DEFAULT NULL,
  `tenantid` varchar(255) DEFAULT NULL,
  `userid` varchar(255) DEFAULT NULL,
  `size` bigint(20) DEFAULT NULL,
  `objectid` varchar(255) NOT NULL DEFAULT "",
  `lastmodifiedtime` datetime DEFAULT NULL,
  `etag` varchar(255) DEFAULT NULL,
  `contenttype` varchar(255) DEFAULT NULL,
  `customattributes` JSON DEFAULT NULL,
  `acl` JSON DEFAULT NULL,
  `nullversion` tinyint(1) DEFAULT NULL,
  `deletemarker` tinyint(1) DEFAULT NULL,
  `ssetype` varchar(255) DEFAULT NULL,
  `encryptionkey` blob DEFAULT NULL,
  `initializationvector` blob DEFAULT NULL,
  `type` tinyint(1) DEFAULT 0,
  `tier` int(11) DEFAULT 1,
  `storageMeta` varchar(255) NOT NULL DEFAULT "",
   UNIQUE KEY `rowkey` (`bucketname`,`name`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `gcobjs`
--
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE IF NOT EXISTS `gcobjs` (
  `bucketname` varchar(255) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `version` bigint(20) UNSIGNED DEFAULT NULL,
  `location` varchar(255) DEFAULT NULL,
  `tenantid` varchar(255) DEFAULT NULL,
  `userid` varchar(255) DEFAULT NULL,
  `size` bigint(20) DEFAULT NULL,
  `objectid` varchar(255) DEFAULT NULL,
  `lastmodifiedtime` datetime DEFAULT NULL,
  `storageMeta` varchar(255) DEFAULT NULL,
   UNIQUE KEY `rowkey` (`bucketname`,`name`,`version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `objmap`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE IF NOT EXISTS `objmap` (
  `bucketname` varchar(255) DEFAULT NULL,
  `objectname` varchar(255) DEFAULT NULL,
  `nullvernum` bigint(20) DEFAULT NULL,
  UNIQUE KEY `objmap` (`bucketname`,`objectname`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `users`
--

/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE IF NOT EXISTS `users` (
  `userid` varchar(255) DEFAULT NULL,
  `bucketname` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-03-20 18:26:36


CREATE TABLE IF NOT EXISTS `lifecycle` (
                       `bucketname` varchar(255) DEFAULT NULL,
                       `status` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;

CREATE TABLE IF NOT EXISTS `bucket_versionopts` (
                                  `bucketname` varchar(255) NOT NULL,
                                  `versionstatus` varchar(255) DEFAULT NULL,
                                  PRIMARY KEY (`bucketname`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
