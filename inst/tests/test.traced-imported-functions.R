
testthat::context("Test tracing imported functions")

test_that("Recordr library loads", {
  library(recordr)
  expect_true(length(ls("package:recordr")) > 1)
})

test_that("Can create Recordr instance", {
  rc <- new("Recordr")
  expect_that(rc@class[1], matches("Recordr"))
})

test_that("Can trace readPNG, writePNG", {
  library(uuid)
  skip_on_cran()
  #  
  # Test overriding writePNG, readPNG
  #
  if(require(png)) {
    rc <- new("Recordr")
    tagNum <- UUIDgenerate()
    
    executionId <- startRecord(rc, tag=tagNum)
    # read a sample file (R logo)
    img <- readPNG(system.file("img","Rlogo.png",package="png"))
    # Test writing a PNG image to a file
    tf <- tempfile(fileext=".png")
    writePNG(img, target = tf)
    status <- endRecord(rc)
    
    mdf <- listRuns(rc, tag=tagNum)
    oneRow <- nrow(mdf) == 1
    expect_that(oneRow, is_true())
    expect_that(mdf[mdf$tag == tagNum, 'executionId'], matches(executionId))
    # Delete the single run
    mdf <- deleteRuns(rc, tag=tagNum)
    # If we deleted the run, then the returned data
    # frame of deleted runs will have one row
    oneRow <- nrow(mdf) == 1
    expect_that(oneRow, is_true())
  } else {
    skip("png package required for this test")
  }
})