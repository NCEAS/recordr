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
#' @slot executionId A character containing the unique indentifier for this execution.
#' @slot metadataId A character containing the unique identifier for the associated metadata object.
#' @slot tag A character vector containing text associated with this execution.
#' @slot datapackageId A character containing the unique identifier for an uploaded package.
#' @slot user A character containing the user name that ran the execution.
#' @slot subject A character containing the user identity that uploaded the package.
#' @slot hostId A character containing the host identifier to which the package was uploaded.
#' @slot startTime A character containing a the start time of the execution.
#' @slot operatingSystem A character continaing the operating system name.
#' @slot runtime A character containing R build and version information.
#' @slot softwareApplication
#' @slot moduleDependencies
#' @slot endTime
#' @slot errorMessage
#' @slot publishTime
#' @slot publishNodeId
#' @slot publishId
#' @slot console
#' @slot seq
#' @import RSQLite
#' @include Recordr.R
#' @section Methods:
#' \itemize{
#'  \item{\code{\link[=initialize-ExecMetadata]{initialize}}}{: Initialize an execution metadata object}
#'  \item{\code{\link{readExecMeta}}}{: Retrieve saved Execution metadata.}
#'  \item{\code{\link{writeExecMeta}}}{: Save a single execution metadat.}
#'  \item{\code{\link{updateExecMeta}}}{: Update saved execution metadata.}
#' }
#' @seealso \code{\link{recordr}}{ package description.}
setClass("ExecMetadata", slots = c(executionId      = "character",
                                   metadataId       = "character",
                                   tag             = "character",
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
#' @param executionId a \code{"character"}, the unique identifier for an execution
#' @param metadataId a \code{"character"}, the unique identifier for the metadata object associated with an execution
#' @param tag A character vector that describes this execution.
#' @param datapackageId a \code{"character"}, the unique identifier for the datapackage associated with an execution
#' @param user a \code{"character"}, the user that started the execution
#' @param subject a \code{"character"}, the user identity that owns the uploaded execution datapackage
#' @param hostId  a \code{"character"},  the host identifier that the execution datapackage was uploaded to
#' @param startTime a \code{"character"},  the starting time of the execution
#' @param operatingSystem a \code{"character"}, the operating system that the execution ran on
#' @param runtime a \code{"character"}, the software application used for the run, e.g. "R version 3.2.3 (2015-12-10)"
#' @param moduleDependencies a \code{"character"} vector, a list of modules loaded during an execution  
#' @param programName  a \code{"character"}, The name of the program that is being run.
#' @param endTime a \code{"character"}, the ending time of an execution
#' @param errorMessage a \code{"character"}, error messages generated during an execution
#' @param publishTime a \code{"character"}, the time of publication (uploading) of an execution package
#' @param publishNodeId a \code{"character"}, the node identifier that an execution package was published to
#' @param publishId a \code{"character"}, the unique identifier associated with a published execution
#' @param console a \code{"logical"}, was this execution recorded as commands typed at the console 
#' @param seq an \code{"integer"}, an integer associated with an execution
#' @rdname initialize-ExecMetadata
#' @aliases initialize-ExecMetadata
#' @seealso \code{\link[=ExecMetadata-class]{ExecMetadata}} { class description}
setMethod("initialize", signature = "ExecMetadata", definition = function(.Object,
    executionId = as.character(NA), metadataId = as.character(NA), tag=as.character(NA), datapackageId = as.character(NA),
    user = as.character(NA), subject = as.character(NA), hostId = as.character(NA), startTime = as.character(NA),
    operatingSystem = as.character(NA), runtime = as.character(NA), moduleDependencies = as.character(NA), programName=as.character(NA), 
    endTime = as.character(NA), errorMessage = as.character(NA), publishTime = as.character(NA), publishNodeId = as.character(NA),
    publishId = as.character(NA), console = FALSE, seq = as.integer(0)) { 
  
  if(is.na(executionId)) {
    .Object@executionId <- sprintf("urn:uuid:%s", UUIDgenerate())
  } else {
    .Object@executionId <- executionId
  }
  
  if(is.na(metadataId)) {
    .Object@metadataId <- sprintf("urn:uuid:%s", UUIDgenerate())
  } else {
    .Object@metadataId <- metadataId
  }
  .Object@tag <- tag
  
  if(is.na(datapackageId)) {
    .Object@datapackageId <- sprintf("urn:uuid:%s", UUIDgenerate())
  } else {
    .Object@datapackageId <- datapackageId
  }
  if(is.na(user)) {
    .Object@user    <- Sys.info()[['user']]
  } else {
    .Object@user    <- user
  }
  
  .Object@subject <- subject
  if(is.na(hostId)) {
    .Object@hostId <- Sys.info()[['nodename']]
  } else {
    .Object@hostId <- hostId
  }
  
  if(is.na(startTime)) {
    .Object@startTime <- as.character(format(Sys.time(), format="%Y-%m-%d %H:%M:%S %Z"))
  } else {
    .Object@startTime <- startTime
  }
  
  if(is.na(operatingSystem)) {
    .Object@operatingSystem <- R.Version()$platform
  } else {
    .Object@operatingSystem <-operatingSystem
  }
  
  if(is.na(runtime)) {
    .Object@runtime <- R.Version()$version.string
  } else {
    .Object@runtime <- runtime
  }
  
  if(is.na(moduleDependencies)) {
    # Get list of packages that recordr has loaded and store as characters, i.e.
    # "recordr 0.1, uuid 0.1-1, dataone 1.0.0, dataonelibs 1.0.0, XML 3.98-1.1, rJava 0.9-6"
    basePkgs   <- paste(sessionInfo()$basePkgs, collapse=", ")
    loadedPkgs <- paste(lapply(sessionInfo()$loadedOnly, function(x) paste(x$Package, x$Version, sep="_")), collapse = ', ')
    otherPkgs  <- paste(lapply(sessionInfo()$otherPkgs, function(x) paste(x$Package, x$Version, sep="_")), collapse = ', ')
    .Object@moduleDependencies <- sprintf("%s, %s, %s", basePkgs, loadedPkgs, otherPkgs)
  } else {
    .Object@moduleDependencies <- moduleDependencies  
  }
    
  .Object@softwareApplication  <- programName
  .Object@endTime <- endTime
  .Object@errorMessage <- errorMessage
  .Object@publishTime <- publishTime
  .Object@publishNodeId <- publishNodeId
  .Object@publishId <- publishId
  .Object@console <- console
  .Object@seq <- seq
  
  return(.Object)
})

##########################
## Methods
##########################

#' Save a single execution metadata.
#' @param recordr A Recordr object
#' @param ... Not yet used.
#' @seealso \code{\link[=ExecMetadata-class]{ExecMetadata}} { class description}
setGeneric("writeExecMeta", function(recordr, ...) {
  standardGeneric("writeExecMeta")
})

#' @rdname  writeExecMeta
#' @param execMeta an ExecMetadata object to save.
setMethod("writeExecMeta", signature("Recordr"), function(recordr, execMeta, ...) {
  
  # Check if the connection to the database is still working
  tmpDBconn <- FALSE
  if (!dbIsValid(recordr@dbConn)) {
    dbConn <- getDBconnection(dbFile=recordr@dbFile)
    if(is.null(dbConn)) {
      stop(sprintf("Error reconnecting to database file %s\n", recordr@dbFile))
    }
    tmpDBconn <- TRUE
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
    
    result <- dbSendQuery(conn=dbConn, statement=createStatement)
    dbClearResult(result)
  }
  
  if (!is.element("tags", dbListTables(dbConn))) {
    createTagsTable(recordr)
  }
  
  execSlotNames <- slotNames("ExecMetadata")
  # "tag" is a slot but not a column in 'execmeta' table, so don't process it here.
  # Tags are contained in the tags table, so they are added to the tags table separately.
  tagInd <- which(execSlotNames=="tag")
  #execSlotNames <- [-which(execSlotNames=="tag")]
  execSlotNames <- execSlotNames[-tagInd]
  # Get slot types
  slotDataTypes <- getSlots("ExecMetadata")
  slotDataTypes <- slotDataTypes[-tagInd]
  # Get values from all the slots for the execution metadata, in the order they were declared in the class definition.
  slotValues <- unlist(lapply(execSlotNames, function(x) as.character(slot(execMeta, x))))
  slotValuesStr <- NULL
  # "tag" is a slot but not a column in 'execmeta' table, so remove 'tag' from the list.
  execSlotNamesStr <- paste(execSlotNames[execSlotNames != "tag"], collapse=",")
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
  insertStatement <- paste("INSERT INTO execmeta ", "(", execSlotNamesStr, ")", " VALUES (", slotValuesStr, ")", sep=" ")
  #cat(sprintf("insert: %s\n", insertStatement))
  result <- dbSendQuery(conn=dbConn, statement=insertStatement)
  dbClearResult(result)
  
  # Insert tag into 'tags' table, one record per tag, if tag was specified for this execution.
  for(i in 1:length(execMeta@tag)) {
    insertStatement <- sprintf("INSERT INTO tags ('executionId', 'tag') VALUES (%s, %s)", sQuote(execMeta@executionId), 
                               sQuote(execMeta@tag[[i]]))
    result <- dbSendQuery(conn=dbConn, statement=insertStatement)
    dbClearResult(result)
  }
  options(useFancyQuotes=quoteOption)
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
#' @param ... additional arguments
#' @seealso \code{\link[=ExecMetadata-class]{ExecMetadata}} { class description}
setGeneric("updateExecMeta", function(recordr, ...) {
  standardGeneric("updateExecMeta")
})

#' @rdname updateExecMeta
#' @param executionId The execution id of the execution to be updated
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
    tmpDBconn <- TRUE
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
#' @param ... additional parameters
#' @seealso \code{\link[=ExecMetadata-class]{ExecMetadata}} { class description}
setGeneric("readExecMeta", function(recordr, ...) {
  standardGeneric("readExecMeta")
})

#' @rdname readExecMeta
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
#' @param delete a \code{"logical"}, if TRUE, the selected runs are deleted (default: FALSE).
#' @return A list of ExecMetadata objects 
setMethod("readExecMeta", signature("Recordr"), function(recordr, 
                                    executionId=as.character(NA),  script=as.character(NA), 
                                    startTime=as.character(NA),  endTime=as.character(NA), 
                                    tag=as.character(NA), errorMessage=as.character(NA), 
                                    seq=as.integer(NA), orderBy=as.character(NA), 
                                    sortOrder="ascending", delete=FALSE, ...) {
  
  # Check if the connection to the database is still working
  tmpDBconn <- FALSE
  if (!dbIsValid(recordr@dbConn)) {
    dbConn <- getDBconnection(dbFile=recordr@dbFile)
    if(is.null(dbConn)) {
      stop(sprintf("Error reconnecting to database file %s\n", recordr@dbFile))
    }
    tmpDBconn <- TRUE
  } else {
    dbConn <- recordr@dbConn
  }
  # If the 'execmeta' table doesn't exist yet, then there is no exec metadata for this
  # executionId, so just return a blank data.frame
  if (!is.element("execmeta", dbListTables(dbConn))) {
    return(data.frame())
  }
   
  if (!is.element("tags", dbListTables(dbConn))) {
    createTagsTable(recordr)
  }
  
  # Construct a SELECT statement to retrieve the runs that match the specified search criteria.
  select <- "SELECT e.*, t.tag from execmeta e, tags t "
  whereClause <- NULL
  #colNames <- c("script", "startTime", "endTime", "runId", "packageId", "publishTime", "errorMessage", "console", "seq")
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
    matchClause <- "e.executionId = "
    if(grepl("*", executionId)) {
      executionId <- gsub("\\*", "%", executionId)
      matchClause <- "e.executionId LIKE "
    } 
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and %s \'%s\'", whereClause, matchClause, executionId)
    } else {
      whereClause <- sprintf(" where %s \'%s\'", matchClause, executionId)
    }
  }
  
  if(!is.na(script)) { 
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and e.softwareApplication like \'%%%s%%\'", whereClause, script)
    } else {
      whereClause <- sprintf(" where e.softwareApplication like \'%%%s%%\'", script)
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
      whereClause <- sprintf(" %s and e.startTime BETWEEN \'%s\' AND \'%s\'", whereClause, start, end)
    } else {
      whereClause <- sprintf(" where e.startTime BETWEEN \'%s\' AND \'%s\'", start, end)
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
      whereClause <- sprintf(" %s and e.endTime BETWEEN \'%s\' AND \'%s\'", whereClause, start, end)
    } else {
      whereClause <- sprintf(" where e.endTime BETWEEN \'%s\' AND \'%s\'", start, end)
    }
  }
  
  # Tags are specified as a list of character strings, so add each tag in an 'or' relationship
  # Have to structure the query so that this is a subselect returning the matching values from the
  # child table. If we didn't use a subselect, the 'or' operator would match all rows. 
  subSelect <- NULL
  if(!all(is.na(tag))) { 
    # ... and t.seq in (select t.seq where t.tag like '%them%' or t.tag like '%those%') and e.executionId == t.executionId  ... 
    subSelect <- 't.executionId in (select distinct executionId from tags '
    subWhereClause <- NULL
    for(i in 1:length(tag)) {
      thisTag <- tag[[i]]
      if(!is.null(subWhereClause)) {
        subWhereClause <- sprintf(" %s or tag like \'%%%s%%\'", subWhereClause, thisTag)
      } else {
        subWhereClause <- sprintf(" where tag like \'%%%s%%\'", thisTag)
      }
    }
    subSelect <- sprintf("%s %s ) ", subSelect, subWhereClause)
  }
  
  if(!is.na(errorMessage)) { 
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and e.errorMessage like \'%%%s%%\'", whereClause, errorMessage)
    } else {
      whereClause <- sprintf(" where e.errorMessage like \'%%%s%%\'", errorMessage)
    }
  }
  
  # The 'seq' column is an integer, so in R interger ranges can be specified as 'n:n'
  # If the user specified a range for 'seq' values to return, translate this into the
  # SQLite equivalent.
  if(!all(is.na(seq))) { 
    if(length(seq) > 1) {
      lowVal <- seq[[1]]
      highVal <- seq[[length(seq)]]
      seqClause <- sprintf(" e.seq BETWEEN %s and %s ", lowVal, highVal)
    } else {
      seqClause <- sprintf("e.seq=%s", seq)
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
    if (is.null(whereClause) && is.null(subSelect)) {
      message("Deleting all records is not allowed unless at least one search term is supplied.") 
      if(tmpDBconn) dbDisconnect(dbConn)
      return(data.frame())
    } 
  }
  
  if(!is.null(whereClause)) {
    deleteWhereClause <- whereClause
    if(!is.null(subSelect)) {
      deleteWhereClause <- paste(deleteWhereClause, " and", subSelect, sep=" ")
    } 
  } else {
    deleteWhereClause <- " where"
    if(!is.null(subSelect)) {
      deleteWhereClause <- paste(deleteWhereClause, subSelect, sep=" ")
    } 
  }
   
  # Remove table name abbreviations, i.e. 'e.executionId, t.tags' because SQLite doesn't allow them in DELETE statements
  deleteWhereClause <- gsub("e\\.", "", deleteWhereClause, perl=TRUE)
  deleteWhereClause <- gsub("t\\.", "", deleteWhereClause, perl=TRUE)
  # Always need this constraint as the join between parent (execmeta) and child (tags)
  join <- sprintf(" e.executionId == t.executionId ")
  
  if(!is.null(whereClause)) {
    whereClause <- sprintf(" %s and %s", whereClause, join)
  } else {
    whereClause <- sprintf(" where %s", join)
  }
  
  # Retrieve records that match search criteria
  if(!is.null(subSelect)) {
    selectStatement <- paste(select, whereClause, "and", subSelect, orderByClause, sep=" ")
  } else {
    selectStatement <- paste(select, whereClause, orderByClause, sep=" ")
  }
  #cat(sprintf("select: %s\n", selectStatement))
  result <- dbSendQuery(conn = dbConn, statement=selectStatement)
  resultdf <- dbFetch(result, n=-1)
  dbClearResult(result)
  
  # If no result were returned from the query, return an empty list
  # and don't do a delete (even if 'delete=TRUE' was specified)
  if(nrow(resultdf) == 0) return(list())
  # Convert the SQLite result set to a list of ExecMetadata objects, so that the
  # caller of this method doesn't have to know about result set structure and can
  # just deal with a list of ExecMetadata objects.
  execIds = ""
  execMetas <- list()
  for (i in 1:nrow(resultdf)) {
    executionId <- resultdf[i, 'executionId']        
    metadataId <- resultdf[i, 'metadataId']
    if(is.null(metadataId)) metadataId <- as.character(NA)
    tag <- resultdf[i, 'tag']
    if(is.null(tag)) tag <- as.character(NA)
    datapackageId <- resultdf[i, 'datapackageId']
    if(is.null(datapackageId)) datapackageId <- as.character(NA)
    user  <- resultdf[i, 'user']
    if(is.null(user)) user <- as.character(NA)
    subject <- resultdf[i, 'subject']
    if(is.null(subject)) subject <- as.character(NA)
    hostId <- resultdf[i, 'hostId']
    if(is.null(hostId)) hostId<- as.character(NA)
    startTime <- resultdf[i, 'startTime']
    if(is.null(startTime)) startTime <- as.character(NA)
    operatingSystem <-resultdf[i, 'operatingSystem']
    if(is.null(operatingSystem)) operatingSystem <- as.character(NA)
    runtime  <- resultdf[i, 'runtime']
    if(is.null(runtime)) runtime <- as.character(NA)
    moduleDependencies <- resultdf[i, 'moduleDependencies']
    if(is.null(moduleDependencies)) moduleDependencies <- as.character(NA)
    programName <- resultdf[i, 'softwareApplication']
    if(is.null(programName)) programName <- as.character(NA)
    endTime <- resultdf[i, 'endTime']
    if(is.null(endTime)) endTime <- as.character(NA)
    errorMessage  <- resultdf[i, 'errorMessage']
    if(is.null(errorMessage)) errorMessage <- as.character(NA)
    publishTime <- resultdf[i, 'publishTime']
    if(is.null(publishTime)) publishTime <- as.character(NA)
    publishNodeId  <- resultdf[i, 'publishNodeId']
    if(is.null(publishNodeId)) publishNodeId <- as.character(NA)
    publishId <- resultdf[i, 'publishId']
    if(is.null(publishId)) publishId <- as.character(NA)
    console <- as.logical(resultdf[i, 'console'])
    seq  <- resultdf[i,'seq']
    
    # We have seen this executionId before, so just add the new tag
    # to the ExecMetadata entry for this executionId.
    if (is.element(executionId, execIds)) {
      thisExecMeta <- execMetas[[executionId]]
      # Get current tag
      newTag <- resultdf[i,'tag']
      savedTags <- thisExecMeta@tag
      # Add additional tag to tag list for this executionId
      savedTags[[length(savedTags)+1]] <- newTag
      thisExecMeta@tag <- savedTags
      # Put the modified execMeta back into the list
      execMetas[[executionId]] <- thisExecMeta
    } else {
      # We haven't seen this executionId before, so create a new execMetadata object and
      # add it to our result list
      thisExecMeta <- new("ExecMetadata",  executionId=executionId, metadataId=metadataId, 
                       tag=tag, datapackageId=datapackageId, user = user, subject=subject,
                       hostId=hostId, startTime=startTime, operatingSystem=operatingSystem,
                       runtime=runtime, moduleDependencies=moduleDependencies,
                       programName=programName, endTime=endTime, errorMessage=errorMessage,
                       publishTime=publishTime, publishNodeId=publishNodeId, publishId=publishId,
                       console=console, seq=seq)
      # Haven't seen this executionId yet, so just add it to the flattened result
      emNames <- names(execMetas)
      execMetas[length(execMetas)+1] <- thisExecMeta
      if(length(execMetas) == 1) {
         names(execMetas) <- executionId
         execIds <- executionId
      } else {
        names(execMetas) <- c(emNames, executionId)
        execIds <- c(execIds, executionId)
      }
    }
  }
  
  # Now delete records if requested.
  if(delete) {
    # Delete from the execmeta table (which will be propagated to the delete to the child 'tags' table).
    deleteStatement <- sprintf("DELETE from execmeta %s",  gsub("e\\.", "", deleteWhereClause, perl=TRUE))
    #cat(sprintf("Delete: %s", deleteStatement))
    result <- dbSendQuery(conn=dbConn, statement=deleteStatement)
    dbClearResult(result)
  }
  
  if(tmpDBconn) dbDisconnect(dbConn)
  return(execMetas)
})

# Create the table that contains informational tags. Multiple tags can
# be associatd with a single
createTagsTable <- function(recordr) {
  
  # Check if the connection to the database is still working and if
  # not, create a new, temporary connection.
  tmpDBconn <- FALSE
  if (!dbIsValid(recordr@dbConn)) {
    dbConn <- getDBconnection(dbFile=recordr@dbFile)
    if(is.null(dbConn)) {
      stop(sprintf("Error reconnecting to database file %s\n", recordr@dbFile))
    }
    tmpDBconn <- TRUE
  } else {
    dbConn <- recordr@dbConn
  }
  
  if (!is.element("tags", dbListTables(dbConn))) {
    createStatement <- "CREATE TABLE tags
            (seq                INTEGER PRIMARY KEY,
            executionId         TEXT not NULL,
            tag TEXT not NULL,
            unique(executionId, tag) ON CONFLICT IGNORE,
            foreign key (executionId) references execmeta(executionId)
            on delete cascade);"
    
    result <- dbSendQuery(conn=dbConn, statement=createStatement)
    dbClearResult(result)
  }
  # We can't return this database connection we just opened, as we are not returning the recordr object with a
  # slot that contains the new database connection, so just disconnect. 
  if(tmpDBconn) {
    dbDisconnect(dbConn)
  }
  return(TRUE)
}
