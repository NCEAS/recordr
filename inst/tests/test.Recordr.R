testthat::context("Recordr tests")

test_that("Recordr library loads", {
  library(recordr)
  expect_true(length(ls("package:recordr")) > 1)
})

test_that("Can create Recordr instance", {
  recordr <- new("Recordr")
  expect_that(rcdr@class[1], matches("Recordr"))
})

test_that("Can record a script execution", {
  recordr <- new("Recordr")
  print(getwd())
  record(recordr, "../testfiles/sampleUserScript.R")
})