# Implementation Plan: Federated Query Engine

## Overview

This implementation plan builds upon the existing PostgreSQL EAV query infrastructure to add federated query capabilities with DuckDB. The approach extends the current `PostgresPersistentRecordRepository` with a new federated query engine that can seamlessly query both PostgreSQL (hot data) and DuckDB/S3 (warm/cold data) simultaneously.

## Tasks

- [ ] 1. Set up DuckDB integration and core interfaces
  - Create DuckDB connection management and configuration
  - Define federated query interfaces extending existing AttributeQuery
  - Set up DuckDB Go driver and S3 extensions
  - _Requirements: 6.1, 6.4_

- [ ] 2. Implement Query Translation Layer
  - [ ] 2.1 Create dual-path query translator
    - Extend existing condition parsing to generate both PostgreSQL and DuckDB SQL fragments
    - Handle schema metadata mapping between EAV and columnar formats
    - _Requirements: 2.1, 2.2_

  - [ ]* 2.2 Write property test for query translation
    - **Property 1: Query translation consistency**
    - **Validates: Requirements 2.1, 2.2**

  - [ ] 2.3 Implement predicate pushdown optimization
    - Generate PostgreSQL-specific WHERE clauses for entity_main filtering
    - Create DuckDB logical fragments for parquet column filtering
    - _Requirements: 2.3, 4.1_

  - [ ]* 2.4 Write unit tests for predicate pushdown
    - Test main table vs EAV condition separation
    - Test date/time encoding conversion
    - _Requirements: 2.4, 2.5_

- [ ] 3. Build Federated Query Coordinator
  - [ ] 3.1 Implement query routing logic
    - Create data tier determination (Hot/Warm/Cold)
    - Implement optimal data source selection based on freshness requirements
    - _Requirements: 1.1, 1.2_

  - [ ] 3.2 Create merge-on-read implementation
    - Build result merging with last-write-wins semantics
    - Implement proper deduplication across data tiers
    - _Requirements: 1.3, 1.4, 3.2_

  - [ ]* 3.3 Write property test for merge-on-read
    - **Property 2: Last-write-wins consistency**
    - **Validates: Requirements 1.4, 3.2**

  - [ ] 3.4 Implement Change_Log_Buffer integration
    - Extend existing change_log queries for real-time data inclusion
    - Handle flushed_at timestamp logic for tier boundaries
    - _Requirements: 1.5, 3.1, 3.4_

- [ ] 4. Checkpoint - Core federated query functionality complete
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 5. Implement DuckDB SQL Template Engine
  - [ ] 5.1 Create SQL template rendering system
    - Build parameterized SQL template for the unified federated query
    - Implement safe parameter interpolation and SQL injection prevention
    - _Requirements: 2.1, 7.4_

  - [ ] 5.2 Implement anti-join logic for dirty data handling
    - Create dirty_ids CTE for PostgreSQL buffer identification
    - Implement S3 data exclusion based on dirty set
    - _Requirements: 3.1, 3.2, 7.1_

  - [ ]* 5.3 Write property test for anti-join logic
    - **Property 3: Dirty data exclusion**
    - **Validates: Requirements 3.2, 7.1**

  - [ ] 5.4 Add type casting and schema alignment
    - Handle PostgreSQL to DuckDB type conversions
    - Ensure consistent data types across federated results
    - _Requirements: 7.4, 8.1_

- [ ] 6. Build Streaming Result Processor
  - [ ] 6.1 Implement streaming result iteration
    - Create row-by-row result processing to manage memory usage
    - Extend existing PersistentRecord scanning for federated results
    - _Requirements: 4.2, 8.4_

  - [ ] 6.2 Add result format consistency
    - Ensure federated results match existing PersistentRecord format
    - Merge OtherAttributes arrays with proper deduplication
    - _Requirements: 8.1, 8.2_

  - [ ]* 6.3 Write unit tests for result processing
    - Test streaming memory management
    - Test PersistentRecord format consistency
    - _Requirements: 8.1, 8.4_

  - [ ] 6.4 Implement pagination support
    - Add accurate total count calculation across federated sources
    - Maintain consistent ordering across data tiers
    - _Requirements: 8.3, 8.5_

- [ ] 7. Add Error Handling and Resilience
  - [ ] 7.1 Implement circuit breaker pattern
    - Create failure detection and automatic fallback mechanisms
    - Handle PostgreSQL and DuckDB unavailability scenarios
    - _Requirements: 5.1, 5.2, 5.3_

  - [ ] 7.2 Add degraded mode operations
    - Implement PostgreSQL-only fallback when S3/DuckDB fails
    - Add S3-only mode when PostgreSQL is unavailable (with warnings)
    - _Requirements: 5.1, 5.2_

  - [ ]* 7.3 Write property test for error handling
    - **Property 4: Graceful degradation**
    - **Validates: Requirements 5.1, 5.2, 5.3**

  - [ ] 7.4 Add comprehensive error reporting
    - Provide detailed error messages for query translation failures
    - Log data inconsistencies between tiers with appropriate warnings
    - _Requirements: 5.4, 5.5_

- [ ] 8. Implement Monitoring and Observability
  - [ ] 8.1 Add performance metrics collection
    - Implement query execution time tracking by stage
    - Add data source utilization and result set size metrics
    - _Requirements: 6.2, 6.3_

  - [ ] 8.2 Create health check endpoints
    - Add health monitoring for PostgreSQL and DuckDB connections
    - Implement system health status reporting
    - _Requirements: 6.5_

  - [ ]* 8.3 Write unit tests for monitoring
    - Test metrics collection accuracy
    - Test health check endpoint responses
    - _Requirements: 6.2, 6.5_

- [ ] 9. Integration and API Layer
  - [ ] 9.1 Extend existing repository interface
    - Add federated query methods to PostgresPersistentRecordRepository
    - Maintain backward compatibility with existing OLTP-only queries
    - _Requirements: 1.1, 8.1_

  - [ ] 9.2 Implement configuration management
    - Add DuckDB connection parameters and resource limits
    - Create configurable query routing policies
    - _Requirements: 6.1, 6.4_

  - [ ]* 9.3 Write integration tests
    - Test end-to-end federated query execution
    - Test configuration parameter handling
    - _Requirements: 1.1, 6.1_

  - [ ] 9.4 Add query execution plan reporting
    - Provide detailed execution plans showing data source selection
    - Include merge strategy information for debugging
    - _Requirements: 6.3_

- [ ] 10. Final checkpoint - Complete federated query system
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties
- Unit tests validate specific examples and edge cases
- The implementation builds incrementally on existing PostgreSQL query infrastructure
- DuckDB integration is designed to be non-intrusive to existing OLTP operations