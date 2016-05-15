testthat::context("Test tracing R functions")

test_that("Recordr library loads", {
  library(recordr)
  expect_true(length(ls("package:recordr")) > 1)
})

test_that("Can create Recordr instance", {
  rc <- new("Recordr")
  expect_that(rc@class[1], matches("Recordr"))
})

test_that("Can trace readLines, writeLines", {
  library(uuid)
  skip_on_cran()
  # 
  # Test overriding writeLines
  #
  rc <- new("Recordr")
  tagNum <- UUIDgenerate()
  executionId <- startRecord(rc, tag=tagNum)
  # Write out to a file
  sbuf <- paste(LETTERS, collapse="")
  tfile <- sprintf("%s/letters.dat", tempdir())
  writeLines(sbuf, tfile)
  status <- endRecord(rc)
  mdf <- listRuns(rc, tag=tagNum, quiet=T)
  oneRow <- nrow(mdf) == 1
  expect_that(oneRow, is_true())
  expect_that(mdf[mdf$tag == tagNum, 'executionId'], matches(executionId))
  # Delete the single run
  mdf <- deleteRuns(rc, tag=tagNum, quiet=T)
  # If we deleted the run, then the returned data
  # frame of deleted runs will have one row
  oneRow <- nrow(mdf) == 1
  expect_that(oneRow, is_true())
  #
  # Test overriding writeLines when using a connectoin 
  #
  rc <- new("Recordr")
  tagNum <- UUIDgenerate()
  executionId <- startRecord(rc, tag=tagNum)
  # Write out to a file
  sbuf <- paste(LETTERS, collapse="")
  tfile <- sprintf("%s/letters.dat", tempdir())
  fcon <- file(description=tfile, open="w")
  writeLines(sbuf, fcon)
  close(fcon)
  status <- endRecord(rc)
  mdf <- listRuns(rc, tag=tagNum, quiet=T)
  oneRow <- nrow(mdf) == 1
  expect_that(oneRow, is_true())
  expect_that(mdf[mdf$tag == tagNum, 'executionId'], matches(executionId))
  # Delete the single run
  mdf <- deleteRuns(rc, tag=tagNum, quiet=T)
  # If we deleted the run, then the returned data
  # frame of deleted runs will have one row
  oneRow <- nrow(mdf) == 1
  expect_that(oneRow, is_true())
  #
  # Test overriding readLines()
  #
  rc <- new("Recordr")
  tagNum <- UUIDgenerate()
  executionId <- startRecord(rc, tag=tagNum)
  # Read from the file just created
  newBuf <- readLines(tfile) 
  status <- endRecord(rc)
  mdf <- listRuns(rc, tag=tagNum, quiet=T)
  oneRow <- nrow(mdf) == 1
  expect_that(oneRow, is_true())
  expect_that(mdf[mdf$tag == tagNum, 'executionId'], matches(executionId))
  expect_equal(sbuf, newBuf)
  # Delete the single run
  mdf <- deleteRuns(rc, tag=tagNum, quiet=T)
  # If we deleted the run, then the returned data
  # frame of deleted runs will have one row
  oneRow <- nrow(mdf) == 1
  expect_that(oneRow, is_true())
  #  
  # Test overriding readLines when a connection is used
  #
  rc <- new("Recordr")
  tagNum <- UUIDgenerate()
  executionId <- startRecord(rc, tag=tagNum)
  # Open a connection to the file just created and read from the connection
  fcon <- file(tfile, open="r")
  newBuf <- readLines(fcon) 
  close(fcon)
  status <- endRecord(rc)
  mdf <- listRuns(rc, tag=tagNum, quiet=T)
  oneRow <- nrow(mdf) == 1
  expect_that(oneRow, is_true())
  expect_that(mdf[mdf$tag == tagNum, 'executionId'], matches(executionId))
  expect_equal(sbuf, newBuf)
  # Delete the single run
  mdf <- deleteRuns(rc, tag=tagNum, quiet=T)
  # If we deleted the run, then the returned data
  # frame of deleted runs will have one row
  oneRow <- nrow(mdf) == 1
  expect_that(oneRow, is_true())
})
