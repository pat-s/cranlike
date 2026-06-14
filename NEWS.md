# cranlike (development version)

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
