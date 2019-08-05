# Yig Billing

该项目提供了使用yig时计费的方式

## 概要

计费的内容目前分为容量(Usage)、外网流量(Traffic)、API调用次数(API)。

Usage 目前需要yig关闭usage统计，并利用TiSpark导出`.csv`文件，然后逐行解析获取。

Traffic 和 API 目前采用 `mtail` + `Prometheus` 方式获取。

## Usage

Usage 分为标准(STANDARD), 低频存储(STANDARD_IA), 归档存储(GLACIER)

## Traffic

Traffic 目前指外网流量且非CDN流量。

## API

API 指API调用次数
