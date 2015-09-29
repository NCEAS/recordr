#
#   This work was created by participants in the DataONE project, and is
#   jointly copyrighted by participating institutions in DataONE. For
#   more information on DataONE, see our web site at http://dataone.org.
#
#     Copyright 2014
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

#' A class representing a script execution with the run manager
#' @author slaughter
#' @slot executionId
#' @slot metadataId
#' @slot tag
#' @slot datapackageId
#' @slot user
#' @slot subject
#' @slot hostId
#' @slot startTime
#' @slot operatingSystem
#' @slot runtime
#' @slot softwareApplication
#' @slot moduleDependencies
#' @slot endTime
#' @slot errorMessage
#' @slot publishTime
#' @slot publishNodeId
#' @slot publishId
#' @slot console
#' @slot seq
#' @include Recordr.R
#' @section Methods:
#' \itemize{
#'  \item{\code{\link[=initialize-ExecMetadata]{initialize}}}{: Initialize an execution metadata object}
#'  \item{\code{\link{readExecMeta}}}{: Retrieve saved Execution metadata.}
#'  \item{\code{\link{writeExecMeta}}}{: Save a single execution metadat.}
#'  \item{\code{\link{updateExecMeta}}}{: Update saved execution metadata.}
#' }
#' @seealso \code{\link{recordr}}{ package description.}
#' @export
setClass("ExecMetadata", slots = c(executionId      = "character",
                                   metadataId       = "character",
                                   tag              = "character",
                                   datapackageId    = "character",
                                   user             = "character",
                                   subject          = "character",
                                   hostId           = "character",
                                   startTime        = "character",
                                   operatingSystem  = "character",
                                   runtime          = "character",
                                   softwareApplication = "character",
                                   moduleDependencies  = "character",
                                   endTime             = "character",
                                   errorMessage        = "character",
                                   publishTime         = "character",
                                   publishNodeId       = "character",
                                   publishId           = "character",
                                   console             = "logical",
                                   seq      = "integer"))

############################
## ExecMetadata constructors
############################

#' Initialize an execution metadata object
#' @param .Object The ExecMetada object
#' @param programName The name of the program that is being run.
#' @param tag A character string that describes this execution.
#' @rdname initialize-ExecMetadata
#' @aliases initialize-ExecMetadata
#' @seealso \code{\link[=ExecMetadata-class]{ExecMetadata}} { class description}
setMethod("initialize", signature = "ExecMetadata", definition = function(.Object, programName=as.character(NA), tag=as.character(NA)) {
  
  .Object@executionId <- sprintf("urn:uuid:%s", UUIDgenerate())
  .Object@metadataId <- sprintf("urn:uuid:%s", UUIDgenerate())
  .Object@tag         <- tag
  .Object@datapackageId <- sprintf("urn:uuid:%s", UUIDgenerate())
  .Object@user <- Sys.info()[['user']]
  .Object@subject <- as.character(NA)
  .Object@hostId <- Sys.info()[['nodename']]
  .Object@startTime <- as.character(Sys.time())
  .Object@operatingSystem <- R.Version()$platform
  .Object@runtime <- R.Version()$version.string
  .Object@softwareApplication  <- programName
  .Object@endTime <- as.character(NA)
  .Object@errorMessage <- as.character(NA)
  .Object@publishTime <- as.character(NA)
  .Object@publishNodeId <- as.character(NA)
  .Object@publishId <- as.character(NA)
  .Object@console <- FALSE
  .Object@seq <- as.integer(0)
  
  # Get list of packages that recordr has loaded and store as characters, i.e.
  # "recordr 0.1, uuid 0.1-1, dataone 1.0.0, dataonelibs 1.0.0, XML 3.98-1.1, rJava 0.9-6"
  pkgs <- sessionInfo()$otherPkgs
  .Object@moduleDependencies <- paste(lapply(pkgs, function(x) paste(x$Package, x$Version)), collapse = ', ')
  return(.Object)
})

##########################
## Methods
##########################

#' Save a single execution metadata.
#' @param recordr A Recordr object
#' @param execMetadata an ExecMetadata object to save.
#' @param ... Not yet used.
#' @seealso \code{\link[=ExecMetadata-class]{ExecMetadata}} { class description}
setGeneric("writeExecMeta", function(recordr, execMeta, ...) {
  standardGeneric("writeExecMeta")
})

#' @describeIn  writeExecMeta
setMethod("writeExecMeta", signature("Recordr", "ExecMetadata"), function(recordr, execMeta, ...) {
  
  # Check if the connection to the database is still working
  tmpDBconn <- FALSE
  if (!dbIsValid(recordr@dbConn)) {
    dbConn <- getDBconnection(dbFile=recordr@dbFile)
    if(is.null(dbConn)) {
      stop(sprintf("Error reconnecting to database file %s\n", recordr@dbFile))
    }
  } else {
    dbConn <- recordr@dbConn
  }
  #print(sprintf("writeExecMeta: writing file %s/runs/%s/execMetadata.csv", recordr@recordrDir, execMeta@executionId))
  # Get values from all the slots for the execution metadata, in the order they were declared in the class definition.
  execSlotNames <- slotNames("ExecMetadata")
  # Remove the 'seq' slot, as this is an autoincrement column in the db, and cannot be specified as the db
  # will determine it's value apon insert.
  #execSlotNames <- execSlotNames[which(execSlotNames!="seq")]
  #execSlotNamesStr <- paste(dQuote(execSlotNames), collapse=",")
  execSlotNamesStr <- paste(execSlotNames, collapse=",")
  # Get the database connection and chek if the execmeta table exists.
  #  dbSendQuery(conn = recordr@dbConn, statement="SELECT name FROM sqlite_master WHERE type='table' AND name='execmeta';")
  # if (dbGetRowCount(result) == 0) {
  
  if (!is.element("execmeta", dbListTables(dbConn))) {
    createStatement <- "CREATE TABLE execmeta
            (seq                INTEGER PRIMARY KEY,
            executionId         TEXT not null,
            metadataId          TEXT,
            tag                 TEXT,
            datapackageId       TEXT,
            user                TEXT,
            subject             TEXT,
            hostId              TEXT,
            startTime           TEXT,
            operatingSystem     TEXT,
            runtime             TEXT,
            softwareApplication TEXT,
            moduleDependencies  TEXT,
            endTime             TEXT,
            errorMessage        TEXT,
            publishTime         TEXT,
            publishNodeId       TEXT,
            publishId           TEXT,
            console             INTEGER,
            unique(executionId));"
    
    #cat(sprintf("create: %s\n", createStatement))
    result <- dbSendQuery(conn=dbConn, statement=createStatement)
    dbClearResult(result)
  }
  
  execSlotNames <- slotNames("ExecMetadata")
  # Get slot types
  slotDataTypes <- getSlots("ExecMetadata")
  # Get values from all the slots for the execution metadata, in the order they were declared in the class definition.
  slotValues <- unlist(lapply(execSlotNames, function(x) as.character(slot(execMeta, x))))
  slotValuesStr <- NULL
  execSlotNamesStr <- paste(execSlotNames, collapse=",")
  # SQLite doesn't like the 'fancy' quotes that R uses for output, so switch to standard quotes
  quoteOption <- getOption("useFancyQuotes")
  options(useFancyQuotes="FALSE")
  for (i in 1:length(execSlotNames)) {
    slotName <- execSlotNames[[i]]
    slotDataType <- slotDataTypes[[i]]
    # Surround character values in single quotes for 'insert' statement 
    # Set sequence number to NULL so that SQLite will autoincrement it
    if(slotName == "seq") {
      slotValuesStr <- ifelse(is.null(slotValuesStr),  
                              slotValuesStr <- "NULL",
                              slotValuesStr <- paste(slotValuesStr, "NULL", sep=","))
    } else  if(slotDataType =="character") {
      # if single quotes already exist in the string, then double them up, which 
      # is the way to escape them in SQLite!
      if (grepl("'", slotValues[i])) {
        thisValue <- gsub("'", "''", slotValues[i])
      } else {
        thisValue <- slotValues[i]
      }
      # If the character value is unassinged, write it out as NULL
      if(is.na(thisValue)) {
        slotValuesStr <- ifelse(is.null(slotValuesStr),  
                                slotValuesStr <- "NULL", 
                                slotValuesStr <- paste(slotValuesStr, "NULL", sep=","))
      } else {
        slotValuesStr <- ifelse(is.null(slotValuesStr),  
                                slotValuesStr <- sQuote(thisValue), 
                                slotValuesStr <- paste(slotValuesStr, sQuote(thisValue), sep=","))
      }
    } else if (slotDataType=="logical") {
      # Logical values are actually stored as integers in SQLite
      ifelse(slotValues[i], thisValue <- 1, thisValue <- 0)
      slotValuesStr <- ifelse(is.null(slotValuesStr),  
                              slotValuesStr <- thisValue, 
                              slotValuesStr <- paste(slotValuesStr, thisValue, sep=","))
    } else {
      # All other datatypes don't get quotes
      slotValuesStr <- ifelse(is.null(slotValuesStr),  
                              slotValuesStr <- slotValues[i], 
                              slotValuesStr <- paste(slotValuesStr, slotValues[i], sep=","))
    }
  }
  options(useFancyQuotes=quoteOption)
  insertStatement <- paste("INSERT INTO execmeta ", "(", execSlotNamesStr, ")", " VALUES (", slotValuesStr, ")", sep=" ")
  #cat(sprintf("insert: %s\n", insertStatement))
  result <- dbSendQuery(conn=dbConn, statement=insertStatement)
  dbClearResult(result)
  # We can't return this database connection we just opened, as we are not returning the recordr object with a
  # slot that contains the new database connection, so just disconnect. 
  if(tmpDBconn) dbDisconnect(dbConn)
  # TODO: interpret result status and set true or false
  return(TRUE)
})

#' Update a single execution metadata object. 
#' @description UPdate an existing execution metadata entry with the
#' values supplied. 
#' @details Saved execution metadata is typically first stored when
#' an execution begins, then updated at the end of a run (with error messages
#' and ending time, for example). Also, excution can be updated when a run
#' is published, with information about the publishing process.
#' @param recordr A Recordr object
#' @param executionId The execution id of the execution to be updated
#' @seealso \code{\link[=ExecMetadata-class]{ExecMetadata}} { class description}
#' @export
setGeneric("updateExecMeta", function(recordr, executionId, ...) {
  standardGeneric("updateExecMeta")
})

#' @describeIn updateExecMeta
#' @param subject The authorized subject, i.e. from the client certificate.
#' @param endTime The ending time of the exection.
#' @param errorMessage An error message generated by the execution.
#' @param publishTime The data and time that the execution was published
#' @param publishNodeId The node identifier, e.g. "urn:node:testKNB" that the execution was published to.
#' @param publishId The identifier that the execution was published with. In DataONE, this can be
#' the identifier of the metadata object describing the datasets that were uploaded.
setMethod("updateExecMeta", signature("Recordr"), function(recordr, 
                                    executionId=as.character(NA), subject=as.character(NA),
                                    endTime=as.character(NA),  
                                    errorMessage=as.character(NA), publishTime=as.character(NA), 
                                    publishNodeId=as.character(NA), publishId=as.character(NA)) {
  
  # Check if the connection to the database is still working
  tmpDBconn<- FALSE
  if (!dbIsValid(recordr@dbConn)) {
    dbConn <- getDBconnection(dbFile=recordr@dbFile)
    if(is.null(dbConn)) {
      stop(sprintf("Error reconnecting to database file %s\n", recordr@dbFile))
    }
  } else {
    dbConn <- recordr@dbConn
  }
  # If the 'execmeta' table doesn't exist yet, then there is no exec metadata for this
  # executionId, so just return a blank data.frame
  if (!is.element("execmeta", dbListTables(dbConn))) {
    return(data.frame())
  }
  
  # Construct an Update statement to update the execution metadata entry for a specific run
  update <- "UPDATE execmeta "
  setClause <- NULL
  if(!is.na(subject)) { 
    subject <- gsub("'", "''", subject)
    if(!is.null(setClause)) {
      setClause <- sprintf("%s , subject = \'%s\'", setClause, subject)
    } else {
      setClause <- sprintf("SET subject = \'%s\'", subject)
    }
  }
  
  if(!is.na(endTime)) { 
    if(!is.null(setClause)) {
      setClause <- sprintf("%s , endTime = \'%s\'", setClause, endTime)
    } else {
      setClause <- sprintf("SET endTime = \'%s\'", endTime)
    }
  }

  if(!is.na(errorMessage)) { 
    errorMessage <- gsub("'", "''", errorMessage)
    if(!is.null(setClause)) {
      setClause <- sprintf("%s , errorMessage = \'%s\'", setClause, errorMessage)
    } else {
      setClause <- sprintf("SET errorMessage = \'%s\'", errorMessage)
    }
  }

  if(!is.na(publishTime)) { 
    if(!is.null(setClause)) {
      setClause <- sprintf("%s , publishTime = \'%s\'", setClause, publishTime)
    } else {
      setClause <- sprintf("SET publishTime = \'%s\'", publishTime)
    }
  }

  if(!is.na(publishNodeId)) { 
    if(!is.null(setClause)) {
      setClause <- sprintf("%s , publishNodeId = \'%s\'", setClause, publishNodeId)
    } else {
      setClause <- sprintf("SET publishNodeId = \'%s\'", publishNodeId)
    }
  }

  if(!is.na(publishId)) { 
    if(!is.null(setClause)) {
      setClause <- sprintf("%s , publishId = \'%s\'", setClause, publishId)
    } else {
      setClause <- sprintf("SET publishId = \'%s\'", publishId)
    }
  }
  
  updateStatement <- sprintf("%s %s where executionId=\'%s\'", update, setClause, executionId)
  #cat(sprintf("update: %s\n", updateStatement))
  result <- dbSendQuery(conn = dbConn, statement=updateStatement)
  dbClearResult(result)
  
  # We can't return this database connection we just opened, as we are not returning the recordr object with a
  # slot that contains the new database connection, so just disconnect. 
  if(tmpDBconn) dbDisconnect(dbConn)
})

#' Retrieve saved execution metadata. 
#' @description Execution metadata is retrived from recordr database table _execmeta_ 
#' based on search parameters.
#' @details The \code{"startTime"} and \code{"endTime"} parameters are used to specify a time
#' range to find runs that started execution between the start and end times that are specified.
#' @param recordr A Recordr object
#' @seealso \code{\link[=ExecMetadata-class]{ExecMetadata}} { class description}
#' @export
setGeneric("readExecMeta", function(recordr, ...) {
  standardGeneric("readExecMeta")
})

#' @describeIn readExecMeta
#' @param executionId A character value that specifies an execution identifier to search for.
#' @param script A character value that specifies a script name to search for.
#' @param startTime A character value that specifies the start of a time range. This value must be
#' entered in the form 'YYYY-MM-DD HH:MM:SS' but can be shortened to "YYYY-MM-DD"
#' @param endTime A character value that specifies the end of a time to to search. This value must
#' be entered in the form 'YYYY-MM-DD HH:MM:SS' but can be shortened to "YYYY-MM-DD"
#' @param tag A tag value to search for
#' @param errorMessage An execution error message to search for.
#' @param seq An exectioin sequence nuber
#' @param orderBy The column to sort the result set by.
#' @param sortOrder The sort order. Values include "ascending", "descending".
#' @return A dataframe containing execution metadata objects
setMethod("readExecMeta", signature("Recordr"), function(recordr, 
                                    executionId=as.character(NA),  script=as.character(NA), 
                                    startTime=as.character(NA),  endTime=as.character(NA), 
                                    tag=as.character(NA),  errorMessage=as.character(NA), 
                                    seq=as.character(NA), orderBy=as.character(NA), 
                                    sortOrder="ascending", delete=FALSE, ...) {
  
  # Check if the connection to the database is still working
  tmpDBconn<- FALSE
  if (!dbIsValid(recordr@dbConn)) {
    dbConn <- getDBconnection(dbFile=recordr@dbFile)
    if(is.null(dbConn)) {
      stop(sprintf("Error reconnecting to database file %s\n", recordr@dbFile))
    }
  } else {
    dbConn <- recordr@dbConn
  }
  # If the 'execmeta' table doesn't exist yet, then there is no exec metadata for this
  # executionId, so just return a blank data.frame
  if (!is.element("execmeta", dbListTables(dbConn))) {
    return(data.frame())
  }
  
  # Construct a SELECT statement to retrieve the runs that match the specified search criteria.
  select <- "SELECT * from execmeta"
  whereClause <- NULL
  colNames <- c("script", "tag", "startTime", "endTime", "runId", "packageId", "publishTime", "errorMessage", "console", "seq")
  # Is the column that the user specified for ordering correct?
  orderByClause <- ""
  if (!is.na(orderBy)) {
    # Change the requested ordering statement to valid SQLite
    if(tolower(sortOrder) %in% c("descending", "desc")) {
      sortOrder <- "DESC"
    } else {
      sortOrder <- "ASC"
    }
    orderByClause <- sprintf("order by %s %s", orderBy, sortOrder)
  }
  
  if(!is.na(executionId)) {    
    matchClause <- "executionId = "
    if(grepl("*", executionId)) {
      executionId <- gsub("\\*", "%", executionId)
      matchClause <- "executionId LIKE "
    } 
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and %s \'%s\'", whereClause, matchClause, executionId)
    } else {
      whereClause <- sprintf(" where %s \'%s\'", matchClause, executionId)
    }
  }
  
  if(!is.na(script)) { 
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and softwareApplication like \'%%%s%%\'", whereClause, script)
    } else {
      whereClause <- sprintf(" where softwareApplication like \'%%%s%%\'", script)
    }
  }
  
  if(!all(is.na(startTime))) { 
    if (length(startTime) > 1) {
      start <- startTime[1]
      end <- startTime[2]
    } else {
      start <- startTime[1]
      end <- "9999-99-99"
    }
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and startTime BETWEEN \'%s\' AND \'%s\'", whereClause, start, end)
    } else {
      whereClause <- sprintf(" where startTime BETWEEN \'%s\' AND \'%s\'", start, end)
    }
  }
  
  if(!all(is.na(endTime))) { 
    if (length(endTime) > 1) {
      start <- endTime[1]
      end <- endTime[2]
    } else {
      start <- endTime[1]
      end <- "9999-99-99"
    }
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and endTime BETWEEN \'%s\' AND \'%s\'", whereClause, start, end)
    } else {
      whereClause <- sprintf(" where endTime BETWEEN \'%s\' AND \'%s\'", start, end)
    }
  }
  
  if(!is.na(tag)) { 
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and tag like \'%%%s%%\'", whereClause, tag)
    } else {
      whereClause <- sprintf(" where tag like \'%%%s%%\'", tag)
    }
  }
  
  if(!is.na(errorMessage)) { 
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and errorMessage like \'%%%s%%\'", whereClause, errorMessage)
    } else {
      whereClause <- sprintf(" where errorMessage like \'%%%s%%\'", errorMessage)
    }
  }
  
  # The 'seq' column is an integer, so in R interger ranges can be specified as 'n:n'
  # If the user specified a range for 'seq' values to return, translate this into the
  # SQLite equivalent.
  if(!is.na(seq)) { 
    seqStr <- as.character(seq)
    if(grepl(":", seq)) {
      seqVals <- unlist(strsplit(seq, ":"))
      lowVal <- seqVals[[1]]
      highVal <- seqVals[[2]]
      seqClause <- sprintf(" seq BETWEEN %s and %s ", lowVal, highVal)
    } else {
      seqClause <- sprintf("seq=%s", seq)
    }
    if(!is.null(whereClause)) {
      whereClause <- sprintf("%s and %s", whereClause, seqClause)
    } else {
      whereClause <- sprintf(" where %s", as.character(seqClause))
    }
  }
  
  # If the user specified 'delete=TRUE', so first fetch the
  # matching records, then delete them.
  if (delete) {
    # Don't allow the user to delete all records unless they specify at
    # least one search term, possibly with a wildcard that will match
    # all records.
    if (is.null(whereClause)) {
      message("Deleting all records is not allowed unless at least one search term is supplied.") 
      if(tmpDBconn) dbDisconnect(dbConn)
      return(data.frame())
    } 
  }
  
  # Retrieve records that match search criteria
  selectStatement <- paste(select, whereClause, orderByClause, sep=" ")
  #cat(sprintf("select: %s\n", selectStatement))
  result <- dbSendQuery(conn = dbConn, statement=selectStatement)
  resultdf <- dbFetch(result)
  dbClearResult(result)
  
  # Now delete records if requested.
  if(delete) {
    deleteStatement <- paste("DELETE from execmeta ", whereClause, sep=" ")
    result <- dbSendQuery(conn=dbConn, statement=deleteStatement)
    dbClearResult(result)
  }
  
  # We can't return this database connection we just opened, as we are not returning the recordr object with a
  # slot that contains the new database connection, so just disconnect. 
  if(tmpDBconn) dbDisconnect(dbConn)
  return(resultdf)
})
