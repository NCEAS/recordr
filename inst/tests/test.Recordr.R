testthat::context("Recordr tests")


test_that("Recordr library loads", {
  library(recordr)
  expect_true(length(ls("package:recordr")) > 1)
})

test_that("Can create Recordr instance", {
  recordr <- new("Recordr")
  expect_that(recordr@class[1], matches("Recordr"))
})

# Create an R script that has no dependencies on external files
createLocalRWScript <- function(scriptPath, inFile, outFile) {
  sink(scriptPath)
  cat(sprintf('df <- read.csv(file = \"%s\")\n', inFile))
  cat(sprintf('write.csv(df, file = \"%s\")\n\n', outFile))
  sink()
}

# Testing setup.
# Create temporary R script file that will read the temp input file and
# write the temp output file. Using temp files removes dependance on external
# files for which the testing harness must know the locations.
# The uuid tag is used so that different tests can manipulate the same run, so that
# each test performs one operation (i.e. create, list, delete a run)
library(uuid)
uuidTag <- UUIDgenerate()
scriptPath <- tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".R")
inFile  <- tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".csv")
outFile <- tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".csv")
createLocalRWScript(scriptPath, inFile, outFile)
write.csv(data.frame(x=1:10, y=11:20), file = inFile)

test_that("Can record a script execution", {
  # Check that tests have been setup
  expect_that(uuidTag, is_a("character"))
  recordr <- new("Recordr")
  pkg <- record(recordr, scriptPath, tag=uuidTag)
  expect_that(class(pkg)[1], equals("DataPackage"))
  # Check the D1 package created by the record() call
  pkgIdNull <- pkg@sysmeta@identifier == ""
  expect_that(pkgIdNull, is_false())
  expect_that(pkg, is_a("DataPackage"))
})

test_that("Can list a script execution", {
  recordr <- new("Recordr")
  # List the single run
  mdf <- listRuns(recordr, tag=uuidTag, quiet = TRUE)
  # If we successfully listed the run, then the returned data
  # frame of deleted runs will have one row
  oneRow <- nrow(mdf) == 1
  expect_that(oneRow, is_true())
})

test_that("Can delete a script execution", {
  recordr <- new("Recordr")
  # Delete the single run
  mdf <- deleteRuns(recordr, tag=uuidTag, quiet = TRUE)
  # If we deleted the run, then the returned data
  # frame of deleted runs will have one row
  oneRow <- nrow(mdf) == 1
  expect_that(oneRow, is_true())
})
