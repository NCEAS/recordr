testthat::context("Test tracing DataONE methods")

test_that("Recordr library loads", {
  library(recordr)
  expect_true(length(ls("package:recordr")) > 1)
})

test_that("Can create Recordr instance", {
  rc <- new("Recordr")
  expect_that(rc@class[1], matches("Recordr"))
})

test_that("Can trace dataone::create(), getObject(), update()", {
  library(digest)
  library(datapackage)
  library(XML)
  library(uuid)
  skip_on_cran()
  if(!require(dataone)) skip("dataone package required for this test")
  cn <- CNode("STAGING")
  mnId <- "urn:node:mnStageUCSB2"
  mn <- getMNode(cn, mnId)
  am <- AuthenticationManager()
  # Suppress PKIplus, cert missing warnings
  suppressMessages(authValid <- dataone:::isAuthValid(am, mn))
  if (authValid) {
    if(dataone:::getAuthMethod(am, mn) == "cert" && grepl("apple-darwin", sessionInfo()$platform)) skip("Skip authentication w/cert on Mac OS X")
    # 
    # Test overriding dataone::create
    #
    rc <- new("Recordr")
    tagNum <- UUIDgenerate()
    executionId <- startRecord(rc, tag=tagNum)
    newid <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data object, and convert it to csv format
    testdf <- data.frame(x=1:10,y=11:20)
    csvfile <- paste(tempfile(), ".csv", sep="")
    write.csv(testdf, csvfile, row.names=FALSE)
    
    # Create SystemMetadata for the object
    format <- "text/csv"
    size <- file.info(csvfile)$size
    sha1 <- digest(csvfile, algo="sha1", serialize=FALSE, file=TRUE)
    sysmeta <- new("SystemMetadata", identifier=newid, formatId=format, size=size, checksum=sha1,
                   originMemberNode=mn@identifier, authoritativeMemberNode=mn@identifier)
    sysmeta <- addAccessRule(sysmeta, "public", "read")
    expect_that(sysmeta@checksum, equals(sha1))
    expect_that(sysmeta@originMemberNode, equals(mn@identifier))
    expect_that(sysmeta@authoritativeMemberNode, equals(mn@identifier))
    
    # Upload the data to the MN using create(), checking for success and a returned identifier
    response <- create(mn, newid, csvfile, sysmeta)
    expect_that(response, not(is_null()))
    expect_that(xmlValue(xmlRoot(response)), matches(newid))
    endRecord(rc)
    
    mdf <- listRuns(rc, tag=tagNum, quiet = TRUE)
    oneRow <- nrow(mdf) == 1
    expect_that(oneRow, is_true())
    expect_that(mdf[mdf$tag == tagNum, 'executionId'], matches(executionId))
    # Delete the single run
    mdf <- deleteRuns(rc, tag=tagNum, quiet = TRUE)
    # If we deleted the run successfully, then the returned data
    # frame of deleted runs will have one row
    oneRow <- nrow(mdf) == 1
    expect_that(oneRow, is_true())
    # 
    # Test overriding dataone::getObject
    # 
    tagNum <- UUIDgenerate()
    executionId <- startRecord(rc, tag=tagNum)
    obj <- getObject(mn, newid)
    endRecord(rc)
    
    df <- read.csv(text=rawToChar(obj))
    expect_equal(testdf, df)
    mdf <- listRuns(rc, tag=tagNum, quiet = TRUE)
    oneRow <- nrow(mdf) == 1
    expect_that(oneRow, is_true())
    expect_that(mdf[mdf$tag == tagNum, 'executionId'], matches(executionId))
    mdf <- deleteRuns(rc, tag=tagNum, quiet = TRUE)
    oneRow <- nrow(mdf) == 1
    expect_that(oneRow, is_true())
    # 
    # Test overriding dataone::update
    # 
    # Start a new recording session for the DataONE update
    tagNum <- UUIDgenerate()
    executionId <- startRecord(rc, tag=tagNum)
    
    # Update the object with a new version
    updateid <- sprintf("urn:uuid:%s", UUIDgenerate())
    testdf <- data.frame(x=1:20,y=11:30)
    csvfile <- paste(tempfile(), ".csv", sep="")
    write.csv(testdf, csvfile, row.names=FALSE)
    size <- file.info(csvfile)$size
    sha1 <- digest(csvfile, algo="sha1", serialize=FALSE, file=TRUE)
    sysmeta@identifier <- updateid
    sysmeta@size <- size
    sysmeta@checksum <- sha1
    sysmeta@obsoletes <- newid
    
    # Update the object that was previously created by this test.
    response <- updateObject(mn, newid, csvfile, updateid, sysmeta)
    expect_that(xmlValue(xmlRoot(response)), matches(updateid))
    updsysmeta <- getSystemMetadata(mn, updateid)
    expect_that(class(updsysmeta)[1], matches("SystemMetadata"))
    expect_that(updsysmeta@obsoletes, matches(newid))
    endRecord(rc)
    
    mdf <- listRuns(rc, tag=tagNum, quiet = TRUE)
    oneRow <- nrow(mdf) == 1
    expect_that(oneRow, is_true())
    expect_that(mdf[mdf$tag == tagNum, 'executionId'], matches(executionId))
    # Delete the single run
    mdf <- deleteRuns(rc, tag=tagNum, quiet = TRUE)
    oneRow <- nrow(mdf) == 1
    expect_that(oneRow, is_true())
  }
})
