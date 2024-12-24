parse_package_files <- function(files, md5s, fields) {
  ## We work in temp dir
  dir.create(tmp <- tempfile())
  on.exit(unlink(tmp, recursive = TRUE), add = TRUE)

  ## Extract and parse DESCRIPTION. If there is a warning, then
  ## this is probably because uncompressing the file has failed.
  ## If there is an error, then we could not extract DESCRIPTION,
  ## but even in this case there will be a warning as well...
  message("cranlike: Started parsing DESCRIPTION files")
  pkgs <- lapply(files, function(file) {
    # p <- progressr::progressor(along = files)
    "!DEBUG Parsing `basename(file)`"

    message(sprintf("Parsing %s", file)) 

    # p(sprintf("x=%s", file))
    if (grepl("s3://", file)) {
      # browser()
      # s3://devxy-arm64-r-binaries/amd64/noble/latest/src/contrib/BSW_0.1.1.tar.gz
      package_and_tag <- strsplit(basename(file), ".tar.gz")[[1]]
      package <- strsplit(package_and_tag, "_")[[1]][1]
      tag <- strsplit(package_and_tag, "_")[[1]][2]
      desc <- get_desc(sprintf("https://raw.githubusercontent.com/cran/%s/%s/DESCRIPTION", package, tag))
    } else {
      desc <- get_desc(file)
    }
    message(sprintf("cranlike: Finished parsing %s", file)) 

    if (is.null(desc)) {
      return(NULL)
    }
    row <- desc$get(fields)
    if (is.na(row["Package"])) message("No package name in ", sQuote(file))
    if (is.na(row["Version"])) message("No version number in ", sQuote(file))
    row
  })
  message("cranlike: Finished parsing DESCRIPTION files")
  valid <- !vapply(pkgs, is.null, TRUE)

  ## Make it into a DF
  pkgs <- drop_nulls(pkgs)
  pkgs <- vapply(pkgs, c, FUN.VALUE = fields)
  df <- as.data.frame(t(pkgs))
  names(df) <- fields

  ## Stick in MD5
  df$MD5sum <- md5s[valid]

  ## Add file names
  df$File <- basename(files[valid])

  ## Some extra fields
  # message("Started querying file sizes")
  # if (grepl("s3://", files[1])) {
  #   df$Filesize <- s3fs::s3_file_size(files)
  # } else {
  #   df$Filesize <- as.character(file.size(files[valid]))
  # }
  # message("Finished querying file sizes")

  ## Standardize licenses, or NA, like in tools
  license_info <- analyze_licenses(df$License)
  df$License <- ifelse(license_info$is_standardizable,
    license_info$standardization,
    NA_character_
  )

  df
}

#' @importFrom desc description

get_desc <- function(file) {
  tryCatch(
    {
      desc <- description$new(file)
      v <- desc$get("Version")
      if (!is.na(v)) desc$set("Version", str_trim(v))
      desc
    },
    error = function(e) {
      warning(
        "Cannot extract valid DESCRIPTION, ", sQuote(file),
        " will be ignored ",
        conditionMessage(e)
      )
      NULL
    }
  )
}

#' @importFrom utils untar unzip

choose_uncompress_function <- function(file) {
  if (grepl("_.*\\.zip$", file)) {
    function(...) unzip(..., unzip = "internal")
  } else if (grepl("_.*\\.tar\\..*$", file) || grepl("_.*\\.tgz$", file)) {
    function(...) untar(..., tar = "internal")
  } else {
    stop("Don't know how to handle file: ", file)
  }
}
