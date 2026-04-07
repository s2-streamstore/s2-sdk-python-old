# Changelog

All notable changes to this project will be documented in this file.

## [5.0.1] - 2026-04-07

### Miscellaneous Tasks

- Migrate submodule from protos to s2-specs ([#106](https://github.com/s2-streamstore/s2-sdk-python/issues/106))
- Update `.gitignore` ([#107](https://github.com/s2-streamstore/s2-sdk-python/issues/107))
- Update `LICENSE` ([#108](https://github.com/s2-streamstore/s2-sdk-python/issues/108))
- Prep for deprecation and archiving ([#110](https://github.com/s2-streamstore/s2-sdk-python/issues/110))

## [5.0.0] - 2025-09-03

### Refactor

- [**breaking**] Replace `retention_age` with `retention_policy`  ([#104](https://github.com/s2-streamstore/s2-sdk-python/issues/104))

## [4.0.0] - 2025-08-11

### Features

- Add access token APIs ([#86](https://github.com/s2-streamstore/s2-sdk-python/issues/86))
- [**breaking**] Support timestamped records ([#88](https://github.com/s2-streamstore/s2-sdk-python/issues/88))
- Support `clamp` in `read_session`, `until` in `read` and `read_session` ([#90](https://github.com/s2-streamstore/s2-sdk-python/issues/90))
- Support `delete-on-empty` config for streams  ([#95](https://github.com/s2-streamstore/s2-sdk-python/issues/95))

### Bug Fixes

- Missing return statement in `read_session` ([#92](https://github.com/s2-streamstore/s2-sdk-python/issues/92))
- Missing `timestamp` in `AppendRecord` msg   ([#96](https://github.com/s2-streamstore/s2-sdk-python/issues/96))
- Mappings between schema types and proto types  ([#97](https://github.com/s2-streamstore/s2-sdk-python/issues/97))

### Refactor

- [**breaking**] Rename `auth_token` as `access_token` in `S2` init  ([#83](https://github.com/s2-streamstore/s2-sdk-python/issues/83))
- [**breaking**] Consolidate config params into a single param ([#87](https://github.com/s2-streamstore/s2-sdk-python/issues/87))
- [**breaking**] Change fencing token type from `bytes` to `str`  ([#89](https://github.com/s2-streamstore/s2-sdk-python/issues/89))
- [**breaking**] Change return types of `check_tail`, `read`, and `read_session` ([#91](https://github.com/s2-streamstore/s2-sdk-python/issues/91))

### Documentation

- Remove `gRPC` mentions  ([#82](https://github.com/s2-streamstore/s2-sdk-python/issues/82))
- Consistent wording and styling  ([#102](https://github.com/s2-streamstore/s2-sdk-python/issues/102))

### Styling

- Apply fixes from `uv run poe checker` ([#85](https://github.com/s2-streamstore/s2-sdk-python/issues/85))

## [3.0.0] - 2025-03-11

### Refactor

- [**breaking**] Modify `BasinInfo` schema ([#43](https://github.com/s2-streamstore/s2-sdk-python/issues/43))

### Features

- Support `create_stream_on_append` basin config ([#44](https://github.com/s2-streamstore/s2-sdk-python/issues/44))

## [2.1.0] - 2025-02-13

### Features

- Support Gzip compression for read* and append* ops ([#36](https://github.com/s2-streamstore/s2-sdk-python/issues/36))

## [2.0.0] - 2025-02-11

### Features

- [**breaking**] Accept `Endpoints` instead of `Cloud` in `S2` init  ([#32](https://github.com/s2-streamstore/s2-sdk-python/issues/32))

### Bug Fixes

- [**breaking**] Retry logic for append session and read session ([#33](https://github.com/s2-streamstore/s2-sdk-python/issues/33))

### Miscellaneous Tasks

- Consistent err kind and style in read session example  ([#34](https://github.com/s2-streamstore/s2-sdk-python/issues/34))

## [1.1.0] - 2025-01-25

### Features

- Expose `Basin` and `Stream` so they can used as type hints ([#30](https://github.com/s2-streamstore/s2-sdk-python/issues/30))

## [1.0.0] - 2025-01-21

### Features

- Add `append_inputs_gen` util for automatic batching of records ([#19](https://github.com/s2-streamstore/s2-sdk-python/issues/19))

### Refactor

- Make `fence` and `trim` APIs behave like mentioned in note ([#18](https://github.com/s2-streamstore/s2-sdk-python/issues/18))
- Move utils into a separate `utils` mod ([#21](https://github.com/s2-streamstore/s2-sdk-python/issues/21))

### Documentation

- Consistent vocab and format ([#23](https://github.com/s2-streamstore/s2-sdk-python/issues/23))
- Fix note in `append_inputs_gen` util fn  ([#24](https://github.com/s2-streamstore/s2-sdk-python/issues/24))

### Styling

- Format previously missed files in `_lib` using `ruff` ([#22](https://github.com/s2-streamstore/s2-sdk-python/issues/22))

### Miscellaneous Tasks

- [**breaking**] Remove trim and fence helper methods ([#20](https://github.com/s2-streamstore/s2-sdk-python/issues/20))

## [0.2.0] - 2025-01-17

### Bug Fixes

- Limits in read session resumption  ([#14](https://github.com/s2-streamstore/s2-sdk-python/issues/14))

### Documentation

- Update `README` ([#10](https://github.com/s2-streamstore/s2-sdk-python/issues/10))

### Miscellaneous Tasks

- At-least-once need not be scary ([#11](https://github.com/s2-streamstore/s2-sdk-python/issues/11))

## [0.1.4] - 2025-01-15

### Miscellaneous Tasks

- Organize imports and add a few external links in docs ([#7](https://github.com/s2-streamstore/s2-sdk-python/issues/7))
- Validate `AppendInput`, expose `metered_bytes`, misc. docs polish ([#8](https://github.com/s2-streamstore/s2-sdk-python/issues/8))

## [0.1.3] - 2025-01-14

### Refactor

- Swap gRPC dependency i.e. betterproto -> grpcio ([#5](https://github.com/s2-streamstore/s2-sdk-python/issues/5))

## [0.1.2] - 2025-01-14

### Features

- Async S2 client with core functionalities ([d05d55a](https://github.com/s2-streamstore/s2-sdk-python/commit/d05d55a8396ab276055c3aacd00b8d1951d15e7c))

<!-- generated by git-cliff -->
