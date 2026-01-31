---
title: Forma Engineering Blog
description: Engineering blog series documenting our journey building a flexible data storage engine for the AI era
layout: doc
---

# Forma Engineering Blog

<div class="tip custom-block" style="padding-top: 8px">

Choose your language: **[中文](#中文系列)** | **[English](#english-series)**

</div>

---

## English Series

A three-part engineering blog series explaining how Forma solves the challenges of building flexible, high-performance data storage for AI applications.

### [Part 1: Why EAV is the Most Underrated Data Model for AI](/blog-series/01-ai-ready-en)
**JSON Schema + Hot Table = AI-Ready Infrastructure**

JSON Schema isn't just a validation tool—it's the core of AI-Ready infrastructure. Learn how to achieve: AI output → instant validation → zero-DDL storage.

### [Part 2: Killing N+1](/blog-series/02-performance-en)
**How One SQL Trick Cut Our Latency by 40x**

We reduced database round-trips from 101 to 1, and latency from 1000ms to 25ms—a 97% improvement. The secret is PostgreSQL's CTE + JSON_AGG.

### [Part 3: Zero Dirty Reads Lakehouse](/blog-series/03-lakehouse-en)
**Building a Trustworthy Lakehouse with DuckDB**

PostgreSQL handles "the present," DuckDB + Parquet handles "the past." Learn how Anti-Join + Dirty Set mechanisms ensure zero dirty reads.

---

## 中文系列

三篇工程博客，讲透一个为 AI 时代设计的灵活数据存储引擎。

### [第一篇：为什么 EAV 是 AI 时代最被低估的数据模型](/blog-series/01-ai-ready-cn)
**JSON Schema + 热表 = AI-Ready 基础设施**

JSON Schema 不只是一个校验工具——它是 AI-Ready 基础设施的核心。实现：AI 输出 → 即时校验 → 零 DDL 入库。

### [第二篇：杀死 N+1](/blog-series/02-performance-cn)
**一次 SQL 优化如何让延迟从 1 秒降到 25 毫秒**

我们将数据库查询次数从 101 次减少到 1 次，延迟从 1000ms 降至 25ms。秘诀是 PostgreSQL 的 CTE + JSON_AGG。

### [第三篇：零脏读的 Serverless 湖仓](/blog-series/03-lakehouse-cn)
**我们如何用 DuckDB 解决一致性难题**

PostgreSQL 负责"当下"，DuckDB + Parquet 负责"历史"。Anti-Join + Dirty Set 机制确保联邦查询零脏读。

---

## Quick Reference

| Problem | Solution | Key Metric |
|---------|----------|------------|
| Schema flexibility | EAV + JSON Schema + Hot Table | Zero DDL, instant field changes |
| N+1 queries | CTE + JSON_AGG | 101→1 queries, 97% latency reduction |
| Historical data scale | DuckDB + CDC + Parquet | Zero dirty reads, Serverless cost |
