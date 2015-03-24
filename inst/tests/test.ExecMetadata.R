testthat::context("ExecMetadata tests")

library(recordr)
library(uuid)

scriptPath <- tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".R")

test_that("Can create an ExecMetadaa object", {
  # Check that tests have been setup
  execMeta <- ExecMetadata(scriptPath)
  expect_that(execMeta, is_a("ExecMetadata"))
  expect_that(execMeta@softwareApplication, equals(scriptPath))
})

test_that("Can write/read an ExecMetadaa object", {
  uniqueTag <- sprintf("Test run %s", UUIDgenerate())
  rc <- new("Recordr")
  execMeta <- ExecMetadata(scriptPath, tag=uniqueTag)
  filePath <- writeExecMeta(rc, execMeta)
  expect_that(file.exists(filePath), is_true())
  expect_that(file.info(filePath)["size"], is_more_than(0))
  
  mdf <- listRuns(rc, tag=uniqueTag, quiet = TRUE)
  if (nrow(mdf) == 0) {
    warning("Unable to test ExecMetadata retrieval because no runs have been recorded")
  } else {
    newExecId <- mdf[1, "executionId"]
    newExecMeta <- readExecMeta(rc, newExecId)
    expect_equal(newExecMeta@executionId, newExecId)
    deleteRuns(rc, tag=uniqueTag, quiet = TRUE)
  }
})
