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


--
-- Table structure for table `cluster`
--

DROP TABLE IF EXISTS `cluster`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cluster` (
  `fsid` varchar(255) DEFAULT NULL,
  `pool` varchar(255) DEFAULT NULL,
  `weight` int(11) DEFAULT NULL,
   UNIQUE KEY `rowkey` (`fsid`,`pool`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Table structure for table `multiparts`
--

DROP TABLE IF EXISTS `multiparts`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `multiparts` (
  `upload_id` bigint(20) UNSIGNED NOT NULL DEFAULT 0,
  `part_num`  bigint(20)  NOT NULL DEFAULT 0,
  `object_id` varchar(255) NOT NULL DEFAULT '',
  `location` varchar(255) NOT NULL DEFAULT '',
  `pool` varchar(255) NOT NULL DEFAULT '',
  `offset` bigint(20) UNSIGNED NOT NULL DEFAULT 0, 
  `size` bigint(20) UNSIGNED NOT NULL DEFAULT 0,
  `etag` varchar(255) NOT NULL DEFAULT '',
  `flag` tinyint(1) UNSIGNED NOT NULL DEFAULT 0,
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`upload_id`, `part_num`),
  UNIQUE KEY `rowkey` (`upload_id`,`part_num`,`object_id`, `flag`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
/*!40101 SET character_set_client = @saved_cs_client */;

DROP TABLE IF EXISTS `gc`; 
CREATE TABLE `gc` (
	`id` bigint(20) PRIMARY KEY AUTO_INCREMENT,
	`location` varchar(255) NOT NULL DEFAULT '',
	`pool` varchar(255) NOT NULL DEFAULT '',
	`object_id` varchar(255) NOT NULL DEFAULT '',
	`create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
	UNIQUE KEY (`object_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
