
.onLoad = function(libname, pkgname) {
  s3fs::s3_file_system(
    aws_access_key_id = Sys.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
    endpoint = "https://s3.eu-central-003.backblazeb2.com",
    region_name = "eu-central-003",
  )
} 

