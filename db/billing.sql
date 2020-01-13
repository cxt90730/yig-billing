/*
 Navicat Premium Data Transfer

 Source Server         : 10.252.10.11
 Source Server Type    : MySQL
 Source Server Version : 50725
 Source Host           : 10.252.10.11:4000
 Source Schema         : billing

 Target Server Type    : MySQL
 Target Server Version : 50725
 File Encoding         : 65001

 Date: 13/01/2020 09:44:00
*/

SET NAMES utf8;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for userbilling
-- ----------------------------
DROP TABLE IF EXISTS `userbilling`;
CREATE TABLE `userbilling`  (
  `ownerid` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `bucketname` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `objectname` varchar(255) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL,
  `version` bigint(20) NOT NULL,
  `storageclass` tinyint(1) NOT NULL,
  `size` bigint(20) NOT NULL,
  `countsize` bigint(20) NOT NULL,
  `lastmodifiedtime` datetime(0) NOT NULL,
  `deleted` tinyint(1) NULL DEFAULT 0,
  `deltime` datetime(0) NULL DEFAULT NULL,
  PRIMARY KEY (`ownerid`, `bucketname`, `objectname`, `version`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_bin;

SET FOREIGN_KEY_CHECKS = 1;
