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
  expect_that(class(uuidTag), equals("character"))
  recordr <- new("Recordr")  
  
  # Check that package metadata can be retrieved and updated
  executionId <- record(recordr, scriptPath, tag=uuidTag)
  metadata <- getMetadata(recordr, id=executionId)
  metadata <- gsub("<surName>.*</surName>", "<surName>Hubbell</surName>", metadata)
  putMetadata(recordr, id=executionId, metadata=metadata, asText=TRUE)
  newMeta <- getMetadata(recordr, id=executionId)
  expect_true(any(grepl("Hubbell", metadata)))
  # Check the D1 package created by the record() call  
  #expect_that(is.null(pkg@sysmeta@identifier), is_false())
  #expect_that(pkg, is_a("DataPackage"))
  
  # Test startRecord() / endRecord()
  newTag <- UUIDgenerate()
  executionId <- startRecord(recordr, tag=newTag)
  myData <- read.csv(file=system.file("extdata/testData.csv", package="recordr"), sep=",", header=TRUE)
  outData <- myData[myData$percent_cover > 35.0, ]
  outFile <- tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".csv")
  write.csv(file=outFile, outData)
  endRecord(recordr)
  # Record this run and check the resulting package
  #pkg <- endRecord(recordr)
  #expect_that(length(getIdentifiers(pkg)), equals(2))
  
  #expect_that(class(pkg@sysmeta)[1], equals("SystemMetadata"))
  mdf <- listRuns(recordr, tag=newTag)
  oneRow <- nrow(mdf) == 1
  expect_that(oneRow, is_true())
  expect_that(mdf[mdf$tag == newTag, 'executionId'], matches(executionId))
  # Delete the single run
  
  mdf <- deleteRuns(recordr, tag=newTag)
  # If we deleted the run, then the returned data
  # frame of deleted runs will have one row
  oneRow <- nrow(mdf) == 1
  expect_that(oneRow, is_true())
})

test_that("Can list a script execution", {
  recordr <- new("Recordr")
  # List the single run
  mdf <- listRuns(recordr, tag=uuidTag)
  # If we successfully listed the run, then the returned data
  # frame of deleted runs will have one row
  oneRow <- nrow(mdf) == 1
  expect_that(oneRow, is_true())
})

test_that("Can delete a script execution", {
  recordr <- new("Recordr")
  # Delete the single run
  mdf <- deleteRuns(recordr, tag=uuidTag)
  # If we deleted the run, then the returned data
  # frame of deleted runs will have one row
  oneRow <- nrow(mdf) == 1
  expect_that(oneRow, is_true())
})
