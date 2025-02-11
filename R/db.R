#' @importFrom utils globalVariables

globalVariables("db")

extra_columns <- function() c("Filesize")

get_db_file <- function(dir) {
  file.path(dir, "PACKAGES.db")
}

get_fields <- function(fields) {
  if (is.null(fields)) {
    fields <- unique(c(
      ("tools" %:::% ".get_standard_repository_db_fields")(),
      "MD5sum",
      "SystemRequirements",
      "Built",
      "Published"
    ))
  }

  unique(c(fields, "File"))
}

db_env <- new.env()

#' Perform a DB query, without explicit locking
#'
#' This is for read operations. They can also be called from
#' within a transaction. In this case the database handle will
#' be reused.
#'
#' @param db_file File of the DB.
#' @param expr Expression to evaluate, it can refer to the connection
#'   handle as `db`.
#'
#' @importFrom DBI dbConnect dbDisconnect dbWithTransaction dbIsValid
#' @importFrom RSQLite SQLite
#'
#' @keywords internal

with_db <- function(db_file, expr) {
  con <- db_env$con
  if (is.null(con) || !dbIsValid(con)) {
    con <- dbConnect(SQLite(), db_file, synchronous = NULL)
    dbExecute(con, "PRAGMA busy_timeout = 60000")
    on.exit(dbDisconnect(con), add = TRUE)
  }
  eval(substitute(expr), envir = list(db = con), enclos = parent.frame())
}

#' Perform a DB query, with locking
#'
#' This creates a transaction, and an exclusive lock.
#' It always creates a new DB connection, and closes it on exit.
#'
#' @inheritParams with_db
#'
#' @keywords internal

with_db_lock <- function(db_file, expr) {
  on.exit(dbDisconnect(con), add = TRUE)
  if (is.null(db_env$con) || !dbIsValid(db_env$con)) {
    db_env$con <- dbConnect(SQLite(), db_file, synchronous = NULL)
  }
  con <- db_env$con
  pnt <- parent.frame()
  dbExecute(con, "PRAGMA busy_timeout = 60000")
  dbExecute(con, "BEGIN EXCLUSIVE")
  withCallingHandlers(
    {
      res <- eval(substitute(expr), envir = list(db = con), enclos = pnt)
      dbExecute(con, "COMMIT")
      res
    },
    error = function(e) dbExecute(con, "ROLLBACK")
  )
}

#' @importFrom DBI dbGetQuery dbExecute

db_all_packages <- function(db_file) {
  with_db(db_file, {
    dbGetQuery(db, "SELECT * FROM packages ORDER BY Package, Version")
  })
}

db_get_fields <- function(db_file) {
  with_db(db_file, {
    names(dbGetQuery(db, "SELECT * FROM PACKAGES LIMIT 1"))
  })
}

#' @importFrom DBI dbWriteTable

create_db <- function(dir, db_file, fields, xcolumns = NULL) {
  fields <- c(fields, extra_columns(), names(xcolumns))
  "!DEBUG Creating DB in `basename(db_file)`"
  dir.create(dirname(db_file), showWarnings = FALSE, recursive = TRUE)
  with_db_lock(db_file, {
    db_create_text_table(db, "packages", fields, key = "MD5sum")
    write_packages_files(dir, db_file)
  })
}

db_create_text_table <- function(db, name, columns, key) {
  sql <- paste0(
    "CREATE TABLE ", name, "(\n",
    paste0('  "', columns, '" ', "TEXT", collapse = ",\n"),
    if (!is.null(key)) paste0(',\n  PRIMARY KEY ("', key, '")\n'),
    ");"
  )
  dbExecute(db, sql)
}

#' @importFrom tools md5sum
#' @importFrom DBI sqlInterpolate dbSendQuery
#' @importFrom s3fs s3_file_info

update_db <- function(dir, db_file, fields, type, xcolumns = NULL) {
  "!DEBUG Updating DB in `basename(db_file)`"

  ## Current packages
  files <- list_package_files(dir, type)

  if (!grepl("s3://", files[1])) {
    dir_md5 <- md5sum(files)
  } else {
    message("cranlike: Starting querying md5sum from S3")
    dir_md5_info <- s3fs::s3_file_info(files)[, c("uri", "etag")]
    dir_md5 = gsub('^"|"$', '', dir_md5_info$etag)
    dir_md5 <- setNames(dir_md5, dir_md5_info$uri)
    message("cranlike: Finished querying md5sum from S3")
    message(sprintf("cranlike: S3 pkgs count: %s", length(dir_md5)))
  }

  with_db_lock(db_file, {
    ## Housekeeping - delete incomplete entries (e.g. version missing)
    no_version <- dbGetQuery(db, "SELECT * FROM packages WHERE Version IS NULL")
    if (nrow(no_version) > 0) {
      message("cranlike: Removing packages without version from DB")
      cat(no_version$Package)
      dbSendQuery(db, "DELETE FROM packages WHERE Version IS NULL")
    }

    ## Packages in the DB
    message("cranlike: Starting querying md5sum from DB")
    pkg_data <- dbGetQuery(db, "SELECT File, MD5sum FROM packages ORDER BY File")
    db_md5 <- setNames(pkg_data$MD5sum, pkg_data$File)
    message(sprintf("cranlike: DB pkgs count: %s", length(db_md5)))
    message("cranlike: Finished querying md5sum from DB")

    message("cranlike: Updating mismatched md5sum packages with remote etag")
    # browser()
    purrr::imap(dir_md5, ~ {
      # .x is the MD5 value; .y is the file name (e.g. "A3_1.0.0.tar.gz")

      # setting this here to keep the s3:// prefix outside of this loop (for later use in parse_package_files)
      .y = basename(.y)
      # .x <- setNames(.x, names(basename(.x)))

      # Find possible match in DB using the file name stored in .y
      obj_db_ind <- which(grepl(sprintf("^%s$", .y), names(db_md5)))

      if (is.na(.y)) {
        return(NULL)
      }

      if (length(obj_db_ind) > 0) {
        if (length(obj_db_ind) > 1) {
          warning(sprintf("Multiple matches (%s) for package %s. Matches: %s. Keeping %s.\n", length(obj_db_ind), .y, names(db_md5[obj_db_ind]), names(db_md5[obj_db_ind[1]])))
          obj_db_ind <- obj_db_ind[1]
        }
        if (.x != db_md5[obj_db_ind]) {
          sql <- "UPDATE OR REPLACE packages SET MD5sum = ?md5sum WHERE File = ?file"
          sql_query <- sqlInterpolate(db, sql, md5sum = .x, file = .y)
          dbExecute(db, sql_query)
          message(sprintf("cranlike: Fixing wrong etag for package %s. New: %s", .y, .x))
        }
      }
    })
  })

  with_db_lock(db_file, {
    db_md5 <- dbGetQuery(db, "SELECT MD5sum FROM packages")$MD5sum
    db_names <- dbGetQuery(db, "SELECT File FROM packages")$File
    db_md5 <- setNames(db_md5, db_names)

    message("cranlike: Checking for removed files in DB")
    ## Files removed?
    if (length(removed <- setdiff(db_md5, dir_md5)) > 0) {
      sql <- "DELETE FROM packages WHERE MD5sum = ?md5sum"
      for (rem in removed) {
        "!DEBUG Removing `rem`"
        dbExecute(db, sqlInterpolate(db, sql, md5sum = rem))
      }
    }
    message("cranlike: Finished checking for removed files in DB")

    message("cranlike: Processing new files to be added to DB")
    ## Any files added?
    if (length(added <- setdiff(dir_md5, db_md5)) > 0) {
      added_files <- names(dir_md5)[match(added, dir_md5)]
      added <- added[which(!is.na(added_files))]
      added_files <- na.omit(added_files)
      pkgs <- parse_package_files(added_files, added, fields)
      if (length(xcolumns)) {
        pkgs <- cbind(pkgs, xcolumns)
      }
      insert_packages(db, pkgs)
      message(sprintf("cranlike: Added %s new packages", nrow(pkgs)))
    }
    message("cranlike: Finished adding new files to DB")

    ## Housekeeping - delete incomplete entries (e.g. version missing)
    no_version <- dbGetQuery(db, "SELECT * FROM packages WHERE Version IS NULL")
    if (nrow(no_version) > 0) {
      message("cranlike: Removing packages without version from DB")
      cat(no_version$Package)
      dbSendQuery(db, "DELETE FROM packages WHERE Version IS NULL")
    }

    message("cranlike: Started writing packages file")
    if (!grepl("s3://", files[1])) {
      write_packages_files(dir, db_file)
    } else {
      write_packages_files(".", db_file)
    }
    message("cranlike: Finished writing packages file")
  })
}

insert_packages <- function(db, pkgs) {
  dbWriteTable(db, "packages", pkgs, append = TRUE)
}
