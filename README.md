# Forma

## Why Develop Forma?

Forma is a general-purpose data management system designed to address the limitations of traditional relational databases (RDBMS) when handling highly dynamic and diverse data structures. As modern applications increasingly demand flexibility and scalability, Forma provides an efficient and easily extensible solution that allows developers to store, query, and manage various types of data without the need for frequent database schema modifications.

Forma chooses **JSON Schema** as the core method for data definition, enabling users to flexibly define and adjust data structures without worrying about the complexity of table structure changes in traditional databases. By storing data in a dual storage structure of **"Hot Fields Table + EAV Table"**, Forma supports complex query and sorting operations while maintaining high performance.

## Why Choose Forma?

### Forma vs RDBMS

*   **Schema Evolution**: Modifying table structures (Schema Migration) in traditional RDBMS often involves table locking and downtime risks. Forma uses **JSON Schema** to define logical structures, utilizing a **"Hot Fields Table (Entity Main) + EAV Table"** dual storage pattern underneath. Adding or modifying attributes only requires updating metadata without touching the physical table structure, achieving zero-downtime evolution.
*   **Complex Structure Support**: Traditional RDBMS usually requires multi-table joins to handle nested objects or arrays. Forma's **Transformer** layer automatically flattens nested JSON for storage while maintaining the ability to query arrays and deep attributes.

### Forma vs MongoDB

*   **ACID Transaction Guarantee**: Built on top of **PostgreSQL**, Forma naturally inherits strict ACID transaction properties, ensuring strong consistency for core business data and avoiding the shortcomings of some NoSQL databases in strong consistency scenarios.
*   **Power of SQL Optimizer**: Forma doesn't just store JSON; its built-in `SQLGenerator` compiles complex JSON filter conditions into efficient SQL queries (utilizing CTEs and Exists subqueries). This fully leverages Postgres's powerful query optimizer and avoids the common N+1 query problem found in EAV models.

### Forma vs KV Store

*   **Multi-dimensional Retrieval Capabilities**: KV Stores excel at retrieving Values by Key but struggle with complex conditional filtering (e.g., `age > 20 AND status = 'active'`). Forma supports combination filtering, sorting, and pagination on arbitrary attributes, and even cross-schema search.
*   **Data Validation & Type Safety**: KV Stores typically treat Values as black boxes. Forma strictly follows JSON Schema for data validation (types, formats, required fields), ensuring the quality of incoming data.

## Use Cases

### OLTP Applications

*   **Dynamic Business Systems**: Ideal for systems like CRM, ERP, or CMS that require frequent data model adjustments or support for user-defined fields (Custom Fields).
*   **Automated RESTful API**: Developers only need to define the JSON Schema, and Forma automatically provides standard CRUD interfaces (Create, Read, Update, Delete), significantly shortening the backend development cycle.
*   **High-Performance Read/Write**: Core high-frequency fields are stored in the "Hot Fields Table", ensuring read/write performance on critical paths is comparable to native SQL tables, while supporting high-concurrency transaction processing.

### OLAP Applications

*   **Real-time Operational Analysis**: Leverages Postgres's query capabilities to support real-time statistics and aggregation analysis of business data.
*   **Data Lake Integration**: Supports exporting structured data to columnar formats like Parquet and storing it in S3, enabling integration with big data platforms for offline analysis and report generation. Parquet files can be partitioned by `schema_id` and `row_id` for efficient updates and queries.
