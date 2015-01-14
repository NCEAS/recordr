testthat::context("ExecMetadata tests")

library(recordr)

scriptPath <- tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".R")

test_that("Can create an ExecMetadaa object", {
  # Check that tests have been setup
  execMeta <- ExecMetadata(scriptPath)
  expect_that(execMeta, is_a("ExecMetadata"))
  expect_that(execMeta@softwareApplication, equals(scriptPath))
})

test_that("Can persist an ExecMetadaa object", {
  rc <- new("Recordr")
  execMeta <- ExecMetadata(scriptPath)
  filePath <- writeExecMeta(rc, execMeta)
  expect_that(file.exists(filePath), is_true())
  expect_that(file.info(filePath)["size"], is_more_than(0))
})

test_that("Can retrieve an ExecMetadata object", {
  rc <- new("Recordr")
  mdf <- listRuns(rc, quiet = TRUE)
  if (nrow(mdf) == 0) {
    warning("Unable to test ExecMetadata retrieval because no runs have been recorded")
  } else {
    execId <- mdf[1, "executionId"]
    execMeta <- readExecMeta(rc, execId)
    expect_equal(execMeta@executionId, execId)
  }
})
