# cranlike (development version)

- Development version bumped to 1.0.3.9002.
  The version had stayed at 1.0.3.9001 across every commit since 2026-06-14,
  including the one that added the `built` argument. Resolvers that decide
  whether an installed copy is current by comparing the `DESCRIPTION` version
  cannot tell those commits apart, so a library holding an older build is never
  refreshed and callers fail with `unused argument (built = built)`.
  Bump this on every change that others install from.

- `update_PACKAGES()` and `add_PACKAGES()` gain a `built` argument.
  When set, it fills the `Built` field of every entry parsed in the call.
  On S3 repos the DESCRIPTION is read from the CRAN source mirror, which never
  carries `Built:`, so a binary repo would otherwise advertise every package as
  source-only and binary-aware clients (e.g. uvr) would compile from source.
  Pass the build R version plus platform triple to advertise the binaries.
- `update_PACKAGES()` on an S3 repo no longer fails with "table packages already
  exists" when several repos are updated in the same process/CWD. `create_db()`
  now clears any stale local `PACKAGES.db` before creating a fresh one.
- `update_PACKAGES()` on an S3 repo now reads file etags from a single bucket
  listing instead of one HEAD request per file, so refreshing a large index is
  no longer O(n) S3 requests (previously minutes-long / throttled on big repos).

# cranlike 1.0.3

* cranlike now adds the size of the file to the metadata, in the `Filesize`
  column.

* cranlike can now add custom metadata. It has to be scalar currently,
  so it will be the same for all packages added at the same time.

# cranlike 1.0.2

* `create_empty_PACKAGES()`, `add_PACKAGES()`, `update_PACKAGES()` and
  `remove_PACKAGES()` lock the DB, to avoid potential concurrency issues.

# cranlike 1.0.1

* `package_versions()` can list extra columns from the database now

# cranlike 1.0.0

First public release.
