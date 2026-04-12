
context("DB")

test_that("with_db", {
  expect_equal(
    with_db(":memory:", c(class(db))),
    "SQLiteConnection"
  )

  xxx <- NULL
  expect_null(with_db(":memory:", xxx))
})

test_that("db_all_packages", {
  db_file <- tempfile()
  on.exit(unlink(db_file))
  with_db(db_file, {
    dbExecute(db, "CREATE TABLE packages (Package TEXT, Version TEXT)")
    dbExecute(db, "INSERT INTO packages VALUES ('igraph', '1.0.0')")
    dbExecute(db, "INSERT INTO packages VALUES ('wegraph', '1.0.1')")
  })
  expect_equal(
    db_all_packages(db_file),
    data.frame(
      stringsAsFactors = FALSE,
      Package = c("igraph", "wegraph"),
      Version = c("1.0.0", "1.0.1")
    )
  )
})

test_that("db_get_fields", {
  db_file <- tempfile()
  on.exit(unlink(db_file))
  with_db(db_file, {
    dbExecute(db, "CREATE TABLE packages (Package TEXT, Version TEXT)")
    dbExecute(db, "INSERT INTO packages VALUES ('igraph', '1.0.0')")
    dbExecute(db, "INSERT INTO packages VALUES ('wegraph', '1.0.1')")
  })
  expect_equal(
    db_get_fields(db_file),
    c("Package", "Version")
  )
})

test_that("create_db", {
  db_file <- tempfile()
  on.exit(unlink(db_file))
  create_db(dirname(db_file), db_file,
            fields = c("Package", "Version", "foo", "bar", "MD5sum"))
  expect_silent(
    with_db(db_file, dbGetQuery(db, "SELECT * from packages"))
  )
})

test_that("db_create_text_table", {
  res <- NULL
  mockery::stub(db_create_text_table, 'dbExecute', function(x, y) res <<- y)
  db_create_text_table(NULL, "table", c("a", "b"), "b")
  expect_match(res, paste0(
    'CREATE TABLE table[(]\\s*"a" TEXT,\\s*"b" TEXT,',
    '\\s*PRIMARY KEY [(]"b")\\s*);'
  ))
})

test_that("update_db", {
  dir.create(dir <- tempfile())
  on.exit(unlink(dir, recursive = TRUE), add = TRUE)

  foo2 <- make_tmp_pkg(dir, "foobar2")
  foo  <- make_tmp_pkg(dir, "foobar")
  foo3 <- make_tmp_pkg(dir, "foobar3")

  db_file <- get_db_file(dir)
  fields <- get_fields(NULL)
  create_db(dir, db_file, fields)
  update_db(dir, db_file, fields, type = "source")

  all_fields <- c(fields, extra_columns())
  tab <- db_all_packages(db_file)
  expect_equal(names(tab), all_fields)
  expect_equal(tab$Package, c("foobar", "foobar2", "foobar3"))
  expect_equal(tab$File, basename(c(foo, foo2, foo3)))

  ## This file was removed
  unlink(file.path(dir, "foobar3_1.0.0.tar.gz"))
  update_db(dir, db_file, fields, type = "source")

  tab <- db_all_packages(db_file)
  expect_equal(names(tab), all_fields)
  expect_equal(tab$Package, c("foobar", "foobar2"))
  expect_equal(tab$File, basename(c(foo, foo2)))
})

test_that("md5sum mismatch fixing uses hash lookup correctly", {
  db_file <- tempfile()
  on.exit(unlink(db_file), add = TRUE)

  # Set up a DB with two packages that have known md5sums
  with_db(db_file, {
    DBI::dbExecute(db, "CREATE TABLE packages (Package TEXT, Version TEXT, File TEXT, MD5sum TEXT, PRIMARY KEY (MD5sum))")
    DBI::dbExecute(db, "INSERT INTO packages VALUES ('pkgA', '1.0.0', 'pkgA_1.0.0.tar.gz', 'aaa111')")
    DBI::dbExecute(db, "INSERT INTO packages VALUES ('pkgB', '2.0.0', 'pkgB_2.0.0.tar.gz', 'bbb222')")
  })

  # Simulate dir_md5 from S3 where pkgA has a new etag but pkgB is unchanged
  dir_md5 <- c(
    "s3://bucket/pkgA_1.0.0.tar.gz" = "aaa999",
    "s3://bucket/pkgB_2.0.0.tar.gz" = "bbb222"
  )

  # Run the hash-based mismatch fix logic (extracted from update_db)
  with_db_lock(db_file, {
    pkg_data <- DBI::dbGetQuery(db, "SELECT File, MD5sum FROM packages ORDER BY File")
    db_md5 <- setNames(pkg_data$MD5sum, pkg_data$File)

    s3_by_name <- setNames(dir_md5, basename(names(dir_md5)))
    s3_by_name <- s3_by_name[!is.na(names(s3_by_name))]

    common <- intersect(names(s3_by_name), names(db_md5))
    mismatched <- common[s3_by_name[common] != db_md5[common]]

    expect_equal(mismatched, "pkgA_1.0.0.tar.gz")

    for (file in mismatched) {
      sql <- "UPDATE OR REPLACE packages SET MD5sum = ?md5sum WHERE File = ?file"
      sql_query <- DBI::sqlInterpolate(db, sql, md5sum = s3_by_name[file], file = file)
      DBI::dbExecute(db, sql_query)
    }
  })

  # Verify: pkgA md5 was updated, pkgB unchanged
  result <- with_db(db_file, {
    DBI::dbGetQuery(db, "SELECT Package, MD5sum FROM packages ORDER BY Package")
  })
  expect_equal(result$MD5sum[result$Package == "pkgA"], "aaa999")
  expect_equal(result$MD5sum[result$Package == "pkgB"], "bbb222")
})

test_that("md5sum hash lookup handles no mismatches", {
  db_file <- tempfile()
  on.exit(unlink(db_file), add = TRUE)

  with_db(db_file, {
    DBI::dbExecute(db, "CREATE TABLE packages (Package TEXT, Version TEXT, File TEXT, MD5sum TEXT, PRIMARY KEY (MD5sum))")
    DBI::dbExecute(db, "INSERT INTO packages VALUES ('pkgA', '1.0.0', 'pkgA_1.0.0.tar.gz', 'aaa111')")
  })

  dir_md5 <- c("s3://bucket/pkgA_1.0.0.tar.gz" = "aaa111")

  with_db_lock(db_file, {
    pkg_data <- DBI::dbGetQuery(db, "SELECT File, MD5sum FROM packages ORDER BY File")
    db_md5 <- setNames(pkg_data$MD5sum, pkg_data$File)

    s3_by_name <- setNames(dir_md5, basename(names(dir_md5)))
    s3_by_name <- s3_by_name[!is.na(names(s3_by_name))]

    common <- intersect(names(s3_by_name), names(db_md5))
    mismatched <- common[s3_by_name[common] != db_md5[common]]

    expect_length(mismatched, 0)
  })

  # DB should be unchanged
  result <- with_db(db_file, {
    DBI::dbGetQuery(db, "SELECT MD5sum FROM packages")
  })
  expect_equal(result$MD5sum, "aaa111")
})

test_that("md5sum hash lookup handles new S3 files not in DB", {
  db_file <- tempfile()
  on.exit(unlink(db_file), add = TRUE)

  with_db(db_file, {
    DBI::dbExecute(db, "CREATE TABLE packages (Package TEXT, Version TEXT, File TEXT, MD5sum TEXT, PRIMARY KEY (MD5sum))")
    DBI::dbExecute(db, "INSERT INTO packages VALUES ('pkgA', '1.0.0', 'pkgA_1.0.0.tar.gz', 'aaa111')")
  })

  # S3 has pkgA (same) plus pkgC (new, not in DB)
  dir_md5 <- c(
    "s3://bucket/pkgA_1.0.0.tar.gz" = "aaa111",
    "s3://bucket/pkgC_1.0.0.tar.gz" = "ccc333"
  )

  with_db_lock(db_file, {
    pkg_data <- DBI::dbGetQuery(db, "SELECT File, MD5sum FROM packages ORDER BY File")
    db_md5 <- setNames(pkg_data$MD5sum, pkg_data$File)

    s3_by_name <- setNames(dir_md5, basename(names(dir_md5)))
    s3_by_name <- s3_by_name[!is.na(names(s3_by_name))]

    common <- intersect(names(s3_by_name), names(db_md5))
    mismatched <- common[s3_by_name[common] != db_md5[common]]

    # No mismatches — pkgC is new, not a mismatch
    expect_length(mismatched, 0)
    # pkgC should show up in setdiff (the "added" path)
    expect_true("ccc333" %in% setdiff(dir_md5, db_md5))
  })
})
