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

## A class representing a script execution run manager
#' @include Recordr.R
#' @slot name (not currently used)
#' @author slaughter
#' @export
setClass("ExecMetadata", slots = c(executionId      = "character",
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

#' execution metadata
#' @param ... (not yet used)
#' @return the ExecMetadata object
#' @author slaughter
#' @export
setGeneric("ExecMetadata", function(programName, ...) {
  standardGeneric("ExecMetadata")
})

setMethod("ExecMetadata", signature("character"), function(programName=as.character(NA), tag=as.character(NA)) {
  
  ## create new MNode object and insert uri endpoint
  execMeta <- new("ExecMetadata")
  execMeta@executionId <- sprintf("urn:uuid:%s", UUIDgenerate())
  execMeta@tag         <- tag
  execMeta@datapackageId <- sprintf("urn:uuid:%s", UUIDgenerate())
  execMeta@user <- Sys.info()[['user']]
  execMeta@subject <- as.character(NA)
  execMeta@hostId <- Sys.info()[['nodename']]
  execMeta@startTime <- as.character(Sys.time())
  execMeta@operatingSystem <- R.Version()$platform
  execMeta@runtime <- R.Version()$version.string
  execMeta@softwareApplication  <- programName
  execMeta@endTime <- execMeta@startTime
  execMeta@errorMessage <- as.character(NA)
  execMeta@publishTime <- as.character(NA)
  execMeta@publishNodeId <- as.character(NA)
  execMeta@publishId <- as.character(NA)
  execMeta@console <- FALSE
  execMeta@seq <- as.integer(0)
  
  # Get list of packages that recordr has loaded and store as characters, i.e.
  # "recordr 0.1, uuid 0.1-1, dataone 1.0.0, dataonelibs 1.0.0, XML 3.98-1.1, rJava 0.9-6"
  pkgs <- sessionInfo()$otherPkgs
  execMeta@moduleDependencies <- paste(lapply(pkgs, function(x) paste(x$Package, x$Version)), collapse = ', ')
  return(execMeta)
})

##########################
## Methods
##########################

#' Save a single execution metadata to a database
#' @param ExecMetadata object
#' @author slaughter
#' @export
setGeneric("writeExecMeta", function(recordr, execMeta, ...) {
  standardGeneric("writeExecMeta")
})

setMethod("writeExecMeta", signature("Recordr", "ExecMetadata"), function(recordr, execMeta, ...) {
  
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
            publishdId          TEXT,
            console             INTEGER);"
    
    cat(sprintf("create: %s\n", createStatement))
    result <- dbSendQuery(conn=dbConn, statement=createStatement)
    dbClearResult(result)
  }
  
  tmpSlotNames <- execSlotNames[which(execSlotNames!="seq")]
  # Extract slot values from the exec meta object
  slotValues <- unlist(lapply(tmpSlotNames, function(x) as.character(slot(execMeta, x))))
  # The error message slot can potentially contain single quotes, which causes an
  # SQL error from the INSERT statement. Double up the single quotes for this slot value,
  # if they exists.
  tmpInd <- which(execSlotNames=="errorMessage")
  slotValues[tmpInd] <- gsub("'", "''", slotValues[tmpInd])
  # Set the seq value to NULL so that SQLite will autoincrement the value apon insert
  #slotValues <- slotValues[2:]
  #seqInd <- which(execSlotNames=="seq")
  # SQLite doesn't like the 'fancy' start and end quotes, so use the simple quotes
  quoteOption <- getOption("useFancyQuotes")
  options(useFancyQuotes="FALSE")
  slotValuesStr <- paste(sQuote(slotValues), collapse=",")
  options(useFancyQuotes=quoteOption)
  #insertStatement <- paste("INSERT INTO execmeta (", execSlotNamesStr, ")", "VALUES (", slotValuesStr, ")')")
  # Add seq back in with NULL value
  slotValuesStr <- sprintf("NULL,%s", slotValuesStr)
  insertStatement <- paste("INSERT INTO execmeta ", "VALUES (", slotValuesStr, ")", sep=" ")
  cat(sprintf("insert: %s\n", insertStatement))
  result <- dbSendQuery(conn=dbConn, statement=insertStatement)
  dbClearResult(result)
  # We can't return this database connection we just opened, as we are not returning the recordr object with a
  # slot that contains the new database connection, so just disconnect. 
  if(tmpDBconn) dbDisconnect(dbConn)
  # TODO: interpret result status and set true or false
  return(TRUE)
})

#' Get Execution metadata from a database
#' @description Execution metadata is retrived from recordr database table _execmeta_ 
#' based on search parameters.
#' @details The \code{"startTime"} and \code{"endTime"} parameters are used to specify a time
#' range to find runs that started execution between the start and end times that are specified.
#' @param executionId A character value that specifies an execution identifier to search for.
#' @param script A character value that specifies a script name to search for.
#' @param startTime A character value that specifies the start of a time range. This value must be
#' entered in the form 'YYYY-MM-DD HH:MM:SS' but can be shortened to "YYYY-MM-DD"
#' @param endTime A character value that specifies the end of a time to to search. This value must
#' be entered in the form 'YYYY-MM-DD HH:MM:SS' but can be shortened to "YYYY-MM-DD"
#' @param tag
#' @param errorMessage
#' @param seq
#' @param orderBy
#' @param sortOrder
#' @return A dataframe containing execution metadata objects
#' @export
setGeneric("readExecMeta", function(recordr, ...) {
  standardGeneric("readExecMeta")
})

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
    return(df)
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
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and executionId=\'%s\'", whereClause, executionId)
    } else {
      whereClause <- sprintf(" where executionId=\'%s\'", executionId)
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
