testthat::context("Recordr tests")

test_that("Recordr library loads", {
  library(recordr)
  expect_true(length(ls("package:recordr")) > 1)
})

test_that("Can create Recordr instance", {
  recordr <- new("Recordr")
  expect_match(recordr@class[1], "Recordr")
})

# Create an R script that has no dependencies on external files
createLocalRWScript <- function(scriptPath, inFile, outFile) {
  sink(scriptPath)
  cat(sprintf("df <- read.csv(file = normalizePath(\"%s\", mustWork=FALSE))\n", inFile))
  cat(sprintf("df <- transform(df, x=x*y)\n"))
  cat(sprintf("write.csv(df, file = normalizePath(\"%s\", mustWork=FALSE))\n\n", outFile))
  sink()
}
createLocalRWScript2 <- function(scriptPath, inFile, outFile) {
  sink(scriptPath)
  cat(sprintf("df <- read.csv(file = normalizePath(\"%s\", mustWork=FALSE))\n", inFile))
  cat(sprintf("df <- transform(df, x=log(x))\n"))
  cat(sprintf("write.csv(df, file = normalizePath(\"%s\", mustWork=FALSE))\n\n", outFile))
  sink()
}

createLocalRWScript3 <- function(scriptPath, inFile, outFile) {
  sink(scriptPath)
  cat(sprintf("df <- read.csv(file = normalizePath(\"%s\", mustWork=FALSE))\n", inFile))
  cat(sprintf("df <- transform(df, y=log(y))\n"))
  cat(sprintf("write.csv(df, file = normalizePath(\"%s\", mustWork=FALSE))\n\n", outFile))
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

test_that("Can record a script execution", {
  # If we are on Windoes, the double backslashes in the filenames will be
  # written out as single backslashes to our temp file. When this file is
  # sourced, this will cause an error. So, convert these to forward slashes,
  # write those to the temp file, then have the script convert then to the
  # appropriate directory separator for the platform. It would be really nice if
  # R had a way to ignore backslashes in a character string! See the script
  # 'createLocalRWScript' above.
  scriptPath <- gsub("\\\\", "/", tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".R"))
  inFile  <- gsub("\\\\", "/", tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".csv"))
  outFile <- gsub("\\\\", "/", tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".csv"))
  createLocalRWScript(scriptPath, inFile, outFile)
  write.csv(data.frame(x=1:10, y=11:20), file = inFile)
  # Check that tests have been setup
  expect_that(class(uuidTag), equals("character"))
  library(uuid)
  library(recordr)
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

  library(uuid)
  library(recordr)
  recordr <- new("Recordr")
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
  mdf <- listRuns(recordr, tag=newTag, quiet=T)
  oneRow <- nrow(mdf) == 1
  expect_that(oneRow, is_true())
  expect_match(mdf[mdf$tag == newTag, 'executionId'], executionId)
  # Delete the single run

  mdf <- deleteRuns(recordr, tag=newTag, quiet=T)
  # If we deleted the run, then the returned data
  # frame of deleted runs will have one row
  oneRow <- nrow(mdf) == 1
  expect_that(oneRow, is_true())
  unlink(inFile)
  unlink(outFile)
})

test_that("Can list a script execution", {
  recordr <- new("Recordr")
  # List the single run
  mdf <- listRuns(recordr, tag=uuidTag, quiet=T)
  # If we successfully listed the run, then the returned data
  # frame of deleted runs will have one row
  oneRow <- nrow(mdf) == 1
  expect_that(oneRow, is_true())
})

test_that("Can delete a script execution", {
  recordr <- new("Recordr")
  # Delete the single run
  mdf <- deleteRuns(recordr, tag=uuidTag, quiet=T)
  # If we deleted the run, then the returned data
  # frame of deleted runs will have one row
  oneRow <- nrow(mdf) == 1
  expect_that(oneRow, is_true())
})

test_that("Can trace lineage.", {
  library(hash)
  uuidTag2 <- UUIDgenerate()
  uuidTag3 <- UUIDgenerate()
  # Script 1 in a linked processing chain
  scriptPath <- gsub("\\\\", "/", tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".R"))
  inFile  <- gsub("\\\\", "/", tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".csv"))
  outFile <- gsub("\\\\", "/", tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".csv"))
  createLocalRWScript(scriptPath, inFile, outFile)
  write.csv(data.frame(x=1:10, y=11:20), file = inFile)
  # Script 2 in a linked processing chain. This script reads the file generated by script 1
  scriptPath2 <- gsub("\\\\", "/", tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".R"))
  inFile2 <- outFile
  outFile2 <- gsub("\\\\", "/", tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".csv"))
  createLocalRWScript2(scriptPath2, inFile2, outFile2)

  # Script 3 in a linked processing chain. This script reads the file generated by script 2
  scriptPath3 <- gsub("\\\\", "/", tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".R"))
  inFile3 <- outFile2
  outFile3 <- gsub("\\\\", "/", tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".csv"))
  createLocalRWScript3(scriptPath3, inFile3, outFile3)

  recordr <- new("Recordr")
  # Check that package metadata can be retrieved and updated
  # These 3 scripts are linked via their common files, i.e.
  # script1 wrote a file that script2 read, script2 wrote
  # a file that script3 read.
  executionId <- record(recordr, scriptPath, tag=uuidTag)
  executionId2 <- record(recordr, scriptPath2, tag=uuidTag2)
  executionId3 <- record(recordr, scriptPath3, tag=uuidTag3)

  # Should return 3 exection ids
  results <- traceRuns(recordr, direction="forward", id=executionId)
  execIds <- keys(results$execMetas)
  expect_equal(length(execIds), 3)
  expect_true(is.element(executionId, execIds))
  expect_true(is.element(executionId2, execIds))
  expect_true(is.element(executionId3, execIds))
  # Should return 3 exection ids
  results <- traceRuns(recordr, direction="backward", id=executionId3)
  execIds <- keys(results$execMetas)
  expect_equal(length(execIds), 3)
  expect_true(is.element(executionId, execIds))
  expect_true(is.element(executionId2, execIds))
  expect_true(is.element(executionId3, execIds))
  # Should return 3 exection ids
  results <- traceRuns(recordr, direction="both", id=executionId2)
  execIds <- keys(results$execMetas)
  expect_equal(length(execIds), 3)
  expect_true(is.element(executionId, execIds))
  expect_true(is.element(executionId2, execIds))
  expect_true(is.element(executionId3, execIds))

  # Should return 2 execution ids
  results <- traceRuns(recordr, direction="forward", id=executionId2)
  execIds <- keys(results$execMetas)
  expect_equal(length(execIds), 2)
  expect_false(is.element(executionId, execIds))
  expect_true(is.element(executionId2, execIds))
  expect_true(is.element(executionId3, execIds))

  # Should return 1 execution id (itself)
  results <- traceRuns(recordr, direction="forward", id=executionId3)
  execIds <- keys(results$execMetas)
  expect_equal(length(execIds), 1)
  expect_false(is.element(executionId, execIds))
  expect_false(is.element(executionId2, execIds))
  expect_true(is.element(executionId3, execIds))

  # Should return 1 execution id (itself)
  results <- traceRuns(recordr, direction="backward", id=executionId)
  execIds <- keys(results$execMetas)
  expect_equal(length(execIds), 1)
  expect_true(is.element(executionId, execIds))
  expect_false(is.element(executionId2, execIds))
  expect_false(is.element(executionId3, execIds))

  # Cleanup
  mdf <- deleteRuns(recordr, tag=uuidTag, quiet=T)
  mdf <- deleteRuns(recordr, tag=uuidTag2, quiet=T)
  mdf <- deleteRuns(recordr, tag=uuidTag3, quiet=T)
  unlink(scriptPath)
  unlink(inFile)
  unlink(outFile)
  unlink(scriptPath2)
  unlink(outFile2)
  unlink(scriptPath3)
  unlink(outFile3)
})
