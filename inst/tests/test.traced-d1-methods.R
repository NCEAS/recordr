testthat::context("Test tracing dataone functions")

test_that("Recordr library loads", {
  library(recordr)
  expect_true(length(ls("package:recordr")) > 1)
})

test_that("Can create Recordr instance", {
  rc <- new("Recordr")
  expect_match(rc@class[1], "Recordr")
})

test_that("Can trace dataone::createObject(), getObject(), updateObject()", {
  library(digest)
  library(datapack)
  library(XML)
  library(uuid)
  # This test requires valid authentication, as it writes content to a DataONE member node.
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
    cat(sprintf("csvfile from test.traced-d1-methods: %s\n", csvfile))
    write.csv(testdf, csvfile, row.names=FALSE)
    cat(sprintf("done with write.csv\n"))
    
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
    response <- createObject(mn, newid, csvfile, sysmeta)
    expect_false(is.null(response))
    expect_match(response, newid)
    endRecord(rc)
    
    mdf <- listRuns(rc, tag=tagNum, quiet=T)
    oneRow <- nrow(mdf) == 1
    expect_that(oneRow, is_true())
    expect_match(mdf[mdf$tag == tagNum, 'executionId'], executionId)
    # Delete the single run
    mdf <- deleteRuns(rc, tag=tagNum, quiet=T)
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
    mdf <- listRuns(rc, tag=tagNum, quiet=T)
    oneRow <- nrow(mdf) == 1
    expect_that(oneRow, is_true())
    expect_match(mdf[mdf$tag == tagNum, 'executionId'], executionId)
    mdf <- deleteRuns(rc, tag=tagNum, quiet=T)
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
    expect_match(response, updateid)
    updsysmeta <- getSystemMetadata(mn, updateid)
    expect_match(class(updsysmeta)[1], "SystemMetadata")
    expect_match(updsysmeta@obsoletes, newid)
    endRecord(rc)
    
    mdf <- listRuns(rc, tag=tagNum, quiet=T)
    oneRow <- nrow(mdf) == 1
    expect_that(oneRow, is_true())
    expect_match(mdf[mdf$tag == tagNum, 'executionId'], executionId)
    # Delete the single run
    mdf <- deleteRuns(rc, tag=tagNum, quiet=T)
    oneRow <- nrow(mdf) == 1
    expect_that(oneRow, is_true())
  }
})
