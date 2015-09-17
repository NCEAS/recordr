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

#' Manage R script executions, capture provenance, upload data producs to DataONE`
#' @description Recordr provides methods to run R scripts and record the files that were
#' read and written by the script, along with information about the execution.
#' This provenance information along with any files created by the script can then be
#' combined into a data package to aid in uploading data products and their description
#' to a data repository.
#' @slot recordrDir value of type \code{"character"} containing a path to the Recordr working directory
#' @slot dbConn A value of type SQLiteConnection that contains the connection to the recordr database
#' @slot dbFile A valof of type \code{"character"} that contains the location to the database file
#' @rdname Recordr-class
#' @author Peter Slaughter
#' @import dataone
#' @import datapackage
#' @import uuid
#' @import tools
#' @import digest
#' @import XML
#' @import EML
#' @import RSQLite
#' @import DBI
#' @include Constants.R
#' @export
setClass("Recordr", slots = c(recordrDir = "character",
                              dbConn = "SQLiteConnection",
                              dbFile = "character")
)

#' Initialize a Recorder object
setMethod("initialize", signature = "Recordr", definition = function(.Object,
                                                                     recordrDir = RecordrHome) {
  
  .Object@recordrDir <- recordrDir
  # Open a connection to the database that contains execution metadata,
  .Object@dbFile <- sprintf("%s/recordr.sqlite", recordrDir)
  dbConn <- getDBconnection(dbFile=.Object@dbFile)
  if (is.null(dbConn)) {
    stop("Unable to create a Recordr object\n")
  } else {
    .Object@dbConn <- dbConn
  }
  return(.Object)
})

##########################
## Methods
##########################

#' Begin recording provenance for an R session. 
#' @description This method starts the recording process and the method endRecord() completes it.
#' @details The startRecord() method can be called from the R console to begin a recording session
#' during which provenance is captured for any functions that are inspected by Recordr. This recordr
#' session can be closed by calling the endRecord() method.
#' @param recordr a Recordr instance
#' @param tag a string that is associated with this run
#' @param .file the filename for the script to run (only used internally when startRecord() is called from record())
#' @param .console a logical argument that is used internally by the recordr package
#' @param config an instance of SessionConfig, which contains configuration parameters
#' @return execution identifier
#' @import dataone
#' @export
setGeneric("startRecord", function(recordr, ...) {
  standardGeneric("startRecord")
})

#' @describeIn Recordr
setMethod("startRecord", signature("Recordr"), function(recordr, tag="", .file=as.character(NA), .console=TRUE, config=as.character(NA)) {
  
  # Check if a recording session has already been started.
  if (is.element(".recordr", base::search())) {
    stop("A Recordr session is already active. Please run endRecord() if you wish to close this session.")
  }
  
  # Create an environment on the search path that will store the overridden 
  # funnction names. These overridden functions are the ones that Recordr will
  # record provenance information for. This mechanism is similiar to a callback,
  # so that when the user script calls these functions, the Recordr version will be
  # called first, provenance relationships will be determined and recorded by Recordr,
  # then the Recordr version will call the native R function.
  # Using this mechanism, the DataONE and R methods and functions
  # are only overridden while the record function is running, allowing the user to use DataONE
  # and R normally from their interactive R session.
  attach(NULL, name=".recordr")
  recordrEnv <- as.environment(".recordr")
#   attach(NULL, name=".recordrConfig")
#   recordrConfigEnv <- as.environment(".recordrConfig")
  
  # If the user specified a configuration session instance, use it, otherwise use
  # the default configuration session.
  if(is.na(config)) {
    # Config session has not been specified on the command line,
    config <- new("SessionConfig")
    loadConfig(config)
  } else {
    if(class(config) == "SessionConfig") {
      loadConfig(config)
    } else {
      msg = sprintf("The \"config\" parameter to startRecord() is not the correct type. 
                    It is a %s, but should be a \"dataone:SessionConfig\"\n", class(config))
      stop(msg)
    }
  }
  # Save the session configuration object to the recordr environment so it will be available to all methods
  recordrEnv$config <- config

  # Put a copy of the database connection into the recordr environment so that it is available
  # to the overriding functions. The db connection has to be open in the Recordr class
  # initialization because it also needs to be available to functions like listRuns() that
  # operate outside the startRecord -> endRecord execution.
  recordrEnv$dbConn <- recordr@dbConn

  # If no scriptName is passed to startRecord(), then we are running in the R console, and R
  # itself is the top level program we are running.
  currentTime <- format(Sys.time(), "%Y%m%d%H%M%s")
  recordrEnv$programId <- sprintf("urn:uuid:%s", UUIDgenerate())
  recordrEnv$execInputIds <- list()
  recordrEnv$execOutputIds <- list()
  if(is.na(.file)) {
    recordrEnv$scriptPath <- ""
  } else {
    if(.console) {
      stop("Can't specify file argument if running startRecord(), only console commands allowed")
    } else {
      # If user invoked recordr(), not startRecord()/endRecord(), then save the name of the file ru
      recordrEnv$scriptPath <- .file
    }
  }

  recordrEnv$execMeta <- ExecMetadata(recordrEnv$scriptPath, tag=tag)
  recordrEnv$execMeta@console <- .console
  
  # TODO: handle concurrent executions of recordr, if ever necessary. Currently recordr
  # doesn't allow concurrent record() sessions to run, as only one copy of the 
  # .recordr environment can exist, which startRecord() checks for.
  # Get the run sequence number by ascending search of all runs by ending time. This method
  # doesn't handle concurrent executions of recordr. This method of obtaining a sequence
  # number needs to be more robust. An alternative solution could use Sqlite for
  # indexing the runs, and have an autoincrement column for the sequence number.
#   runs <- selectRuns(recordr, orderBy="-endTime")
#   if (nrow(runs) == 0) {
#     seq <- 1
#   } else {
#     seq <-  as.integer(runs[1, 'seq']) + 1
#   }
# 
#   recordrEnv$execMeta@seq <- as.integer(seq)
  # Create an empty D1 datapackage object and make it globally avilable, i.e. available
  # to the masking functions, e.g. "recordr_write.csv".
  # TODO: Read memmber node from session API
  #recordrEnv$cnNodeId <- "STAGING2"
  recordrEnv$mnNodeId <- getConfig(recordrEnv$config, "target_member_node_id")
  # TODO: use new() initialization when datapackage is fixed - currently using new() causing
  # an old object to be reused!!!
  #recordrEnv$dataPkg <- new("DataPackage", packageId=recordrEnv$execMeta@datapackageId)
  recordrEnv$dataPkg <- new("DataPackage", packageId=recordrEnv$execMeta@datapackageId)
    
  # Add the ProvONE Execution type
  # Store the provONE relationship: execution -> prov:qualifiedAssociation -> association
  associationId <- sprintf("urn:uuid:%s", UUIDgenerate())
  insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=associationId, predicate=provQualifiedAssociation)
  insertRelationship(recordrEnv$dataPkg, subjectID=associationId, objectIDs=recordrEnv$programId, predicate=provHadPlan)
  # Record relationship identifying this id as a provone:Execution
  insertRelationship(recordrEnv$dataPkg, subjectID=associationId, objectIDs=provAssociation, predicate=rdfType, objectType="uri")
  
  # Record a relationship identifying the program (script or console log)
  insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$programId, objectIDs=provONEprogram, predicate=rdfType, objectType="uri")
  
  # Store the Prov relationship: association -> prov:agent -> user
  # TODO: when available, check session API for orchid and use that instead of user, i.e.
  #   insertRelationship(recordrEnv$dataPkg, subjectID=associationId , objectIDs=recordrEnv$execMeta@orcid, predicate=provAgent, objectType="uri")
  userId <- recordrEnv$execMeta@user
  insertRelationship(recordrEnv$dataPkg, subjectID=associationId , objectIDs=userId, predicate=provAgent, objectType="literal", dataTypeURI=xsdString)
  # Record a relationship identifying the user
  insertRelationship(recordrEnv$dataPkg, subjectID=userId, objectIDs=provONEuser, predicate=rdfType, objectType="uri")
  
  # Override R functions
  recordrEnv$source <- recordr::recordr_source
  
  # override DataONE V1.1.0 methods
  recordrEnv$getD1Object <- recordr::recordr_getD1Object
  #recordrEnv$createD1Object <- recordr::recordr_createD1Object
  # override DataONE v2.0 methods
  recordrEnv$get <- recordr::recordr_D1MNodeGet
  # override R functions
  recordrEnv$read.csv <- recordr::recordr_read.csv
  recordrEnv$write.csv <- recordr::recordr_write.csv
  
  # Create the run metadata directory for this record()
  dir.create(sprintf("%s/runs/%s", recordr@recordrDir, recordrEnv$execMeta@executionId), recursive = TRUE)
  # Put recordr working directory in so masked functions can access it. No information can be saved locally until this
  # variable is defined.
  recordrEnv$recordrDir <- recordr@recordrDir
  #cat(sprintf("filePath: %s\n", recordrEnv$scriptPath))
  if (recordrEnv$scriptPath == "") {
    saveFileInfo(recordrEnv$programId, fileArg="R console", headerOnly=TRUE)
  }
  else {
    saveFileInfo(recordrEnv$programId, recordrEnv$scriptPath, headerOnly=FALSE, access="execute")
  }
  # Record relationship identifying this id as a provone:Execution
  insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=provONEexecution, predicate=rdfType, objectType="uri")
  # Record relationship between the Exectution and the User
  insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=userId, predicate=provWasAssociatedWith, objectType="uri")
  
  # If startRecord()/endRecord() was invoked by the user (vs invoked by record(), then capture
  # all the commands typed by setting marks in the history, then reading the history when
  # endRecord() is called.
  if (.console) {
    startMarker <- sprintf("recordr execution %s started", recordrEnv$execMeta@executionId)
    timestamp (stamp = c(date(), startMarker), quiet = TRUE)
  }
  
  # Load configuration values.
  #loadConfig(recordr)
  setProvCapture(TRUE)
  # The Recordr provenance capture capability is now setup and when startRecord() returns, the
  # user can continue to work in the calling context, i.e. the console and provenance will be
  # capture until endRecord() is called.
  invisible(recordrEnv$execMeta@executionId)
})

#' End the recording session that was started by \code{startRecord()}
#' @description Prepare and return a DataPackage that contains all derived products created during the 
#' session and all recorded provenance relationships
#' @param recordr a Recordr instance
#' @return pkg a DataONE data package
#' @export
setGeneric("endRecord", function(recordr) {
  standardGeneric("endRecord")
})

#' @describeIn Recordr
setMethod("endRecord", signature("Recordr"), function(recordr) {
  
  # Check if a recording session is active
  if (! is.element(".recordr", base::search())) {
    message("A Recordr session is not currently active.")
    return(NULL)
  }
  
  on.exit(recordrShutdown())
  recordrEnv <- as.environment(".recordr")
  #recordrConfigEnv <- as.environment(".recordrConfig")
  runDir <- sprintf("%s/runs/%s", recordr@recordrDir, recordrEnv$execMeta@executionId)
  if (!file.exists(runDir)) {
      dir.create(runDir, recursive = TRUE)
  }
  
  # Disable provenance capture now that endRecord() has been called
  setProvCapture(FALSE)
  recordrEnv$execMeta@endTime <- as.character(Sys.time())
  # For each output dataset created by this execution, record a prov relationship of 'wasDerivedFrom' for each of the 
  # input datasets
  for (outputId in recordrEnv$execOutputIds) {
    for (inputId in recordrEnv$execInputIds) {
      insertRelationship(recordrEnv$dataPkg, subjectID=outputId, objectIDs=inputId, predicate=provWasDerivedFrom)
    }
  }
  # Use the datapackage id as the resourceMap id
  serializationId = recordrEnv$execMeta@datapackageId
  filePath <- sprintf("%s/%s.rdf", runDir, serializationId)
  status <- serializePackage(recordrEnv$dataPkg, file=filePath, id=serializationId)
  
  # User invoked startRecord()/endRecord()
  if (recordrEnv$execMeta@console) {
    # If the user has run with startRecord()/eneRecord(), then retrieve all commands typed
    # at the console since startRecord() was invoked.
    startMarker <- sprintf("recordr execution %s started", recordrEnv$execMeta@executionId)
    endMarker   <- sprintf("recordr execution %s ended", recordrEnv$execMeta@executionId)
    timestamp (stamp = c(date(), endMarker), quiet = TRUE)
    tmpFile <- tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".log")
    savehistory(file=tmpFile)
    recordrHistory <- ""
    allHistory <- ""
    foundStart <- FALSE
    foundEnd <- FALSE
    consoleLogFile <-  sprintf("%s/console.log", runDir)
    
    # Loop through the history, extracting everything between the start and end markers
    # Handle special case of start marker not being in the history, i.e. exceeded max
    # history lines and start marker no longer in history
    for (line in readLines(tmpFile)) {
      allHistory <- c(allHistory, line)
      if(grepl(startMarker, line)) foundStart <- TRUE
      if(grepl(endMarker, line)) foundEnd <- TRUE
      
      if (foundEnd) break
      if (foundStart) {
        recordrHistory <- c(recordrHistory, line)
      }
    }
    
    # Write the console log to the run directory
    headerLine <- "# R console log capture by recordr"
    if (foundStart && foundEnd) {
      recordrHistory <- c(headerLine, recordrHistory)
      writeLines(recordrHistory, consoleLogFile)
    } else if (length(recordrHistory) > 0) {
        recordrHistory <- c(headerLine, recordrHistory)
        writeLines(recordrHistory, consoleLogFile)
    } else {
        allHistory <- c(headerLine, recordrHistory)
        writeLines(recordrHistory, consoleLogFile)
    }
    recordrEnv$scriptPath = consoleLogFile
  }
  
  # Save the executed file or console log to the data package
  script <- charToRaw(paste(readLines(recordrEnv$scriptPath), collapse = '\n'))
  # Create a data package object for the program that we are running and store it.
  scriptFmt <- "text/plain"
  programD1Obj <- new("DataObject", id=recordrEnv$programId, dataobj=script, format=scriptFmt, user=recordrEnv$execMeta@user, mnNodeId=recordrEnv$mnNodeId)    
  # TODO: Set access control on the action object to be public
  addData(recordrEnv$dataPkg, programD1Obj)
  # Serialize/Save the entire package object to the run directory
  filePath <- sprintf("%s/%s.pkg", runDir, recordrEnv$execMeta@datapackageId)
  saveRDS(recordrEnv$dataPkg, file=filePath)
  dataPkg <- recordrEnv$dataPkg
  
  # Save execution metadata to a file in the run directory
  writeExecMeta(recordr, recordrEnv$execMeta)
  
  # Don't print this object if startRecord()/endRecord() was called
  if(recordrEnv$execMeta@console) {
    base::invisible(dataPkg)
  } else {
    # Return the regular object if we are returning to record()
    return(dataPkg)
  }
})

#' Record provenance for an R script 
#' @description The R script is executed and information about file reads and writes
#' is recordred. A data package that contains this provenance information and 
#' all derived products create by the script is returned.
#' @details The data package and other script execution information is also archived 
#' locally in the location specified by the configuration parameter "provenance_storage_directory"
#' @param recordr a Recordr instance
#' @param file the name of the R script to run and collect provenance information for
#' @param tag a string that will be associated with this run
#' @param config A \code{\link[dataone]{SessionConfig}} object
#' @param ... additional parameters that will be passed to the R \code{"base::source()"} function
#' @return the DataONE datapackge created by this run
#' @export
setGeneric("record", function(recordr, file, ...) {
  standardGeneric("record")
})

#' @describeIn Recordr
setMethod("record", signature("Recordr"), function(recordr, file, tag="", config=as.character(NA), ...) {
  # Check if a recording session didn't clean up properly by removing the
  # temporary environments. If yes, then remove them now. The recordr pacakge
  # does not allow concurrent execution of two record() sessions.
  if ( is.element(".recordr", base::search())) {
    detach(".recordr")
  }
#   if ( is.element(".recordrConfig", base::search())) {
#     detach(".recordrConfig")
#   }

  if(!file.exists(file)) {
    stop(sprintf("Error, file \"%s\" does not exist\n", file))
  }
  
  startRecord(recordr, tag, .file=file, .console=FALSE, config=config)
  recordrEnv <- as.environment(".recordr")
  # recordrConfigEnv <- as.environment(".recordrConfig")
  setProvCapture(TRUE)
  # Source the user's script, passing in arguments that they intended for the 'source' call.  
  result = tryCatch ({
    #cat(sprintf("Sourcing file %s\n", filePath))
    # Because we are calling the 'source' function with the packageId, the overridden function
    # for 'source' will not be called, and a provenance entry for this 'source' will not be
    # recorded.
    # Note: ellipse argument is passed from method call so user can pass args to source, if desired.
    #base::source(file, local=FALSE, ...)
    base::source(file, ...)
  }, warning = function(warningCond) {
    slot(recordrEnv$execMeta, "errorMessage") <- warningCond$message
    cat(sprintf("Warning:: %s\n", recordrEnv$execMeta@errorMessage))
  }, error = function(errorCond) {
    slot(recordrEnv$execMeta, "errorMessage") <- errorCond$message
    cat(sprintf("Error:: %s\n", recordrEnv$execMeta@errorMessage))
  }, finally = {
    # Disable provenance capture while some housekeeping is done
    setProvCapture(FALSE)

    # Stop recording provenance and finalize the data package    
    pkg <- endRecord(recordr)
    if (is.element(".recordr", base::search())) {
      detach(".recordr", unload=TRUE)
    }
#     if (is.element(".recordrConfig", base::search())) {
#       detach(".recordrConfig", unload=TRUE)
#     }
    # return a datapackage object
    return(pkg)
  })
})

#' Select runs that match search parameters
#' @description This method is used internally by the recordr package
#' @param recordr a Recordr instance
#' @param runId an execution identifiers
#' @param script name of script to match (can be a regex)
#' @param startTime match executions that started after this time (inclusive)
#' @param endTime match executions that ended before this time (inclusive)
#' @param tag text of tag to match (can be a regex)
#' @param errorMessage text of error message to match (can be a regex)
#' @param seq a run sequence number
#' @param orderBy the column that will be used to sort the output. This can include a minus sign before the name, e.g. -startTime
#' @export
setGeneric("selectRuns", function(recordr, ...) {
  standardGeneric("selectRuns")
})

#' @describeIn Recordr
setMethod("selectRuns", signature("Recordr"), function(recordr, runId=as.character(NA), script=as.character(NA), startTime=as.character(NA), endTime=as.character(NA), 
                                                       tag=as.character(NA), errorMessage=as.character(NA), seq=as.integer(NA), orderBy="-startTime", delete=FALSE) {
  
  colNames = c("script", "tag", "startTime", "endTime", "runId", "packageId", "publishTime", "errorMessage", "console", "seq")
  
  # Find all run directories
  df <- data.frame(script = character(), 
                   tag = character(),
                   startTime = character(),
                   endTime = character(),
                   runId = character(),
                   packageId = character(), 
                   publishTime = character(),
                   errorMessage = character(),
                   console = logical(),
                   seq = integer(),
                   row.names = NULL)
  
  sortOrder = "ascending"
  # Is the column that the user specified for ordering correct?
  if (!is.na(orderBy)) {
    # Check if column name to sort by is prefaced with a "-", which indicates descending column oroder
    if (grepl("^\\s*-", orderBy)) {
      sortOrder <- "descending"
      orderBy <- sub("^\\s*-", "", orderBy)
    } else if (grepl("^\\s*\\+", orderBy)) {
      sortOrder <- "ascending"
      orderBy <- sub("^\\s*\\+", "", orderBy)
    }
    if (! is.element(orderBy, colNames)) {
      cat(sprintf("Invalid column name: \"%s\"\n", orderBy))
      stop(sprintf("Please use one of the following column names: \"%s\"\n", paste(colNames, collapse = '", "')))
    }
  }
  
  # Retrieve the execution metadata that match the search criteria
  execMeta <- readExecMeta(recordr, executionId=runId, script=script, 
    startTime=startTime, endTime=endTime,
    tag=tag, errorMessage=errorMessage,
    seq=seq, orderBy=orderBy, sortOrder=sortOrder, delete)
    
    return(execMeta)
})

#' Delete runs that match search parameters
#' @param recordr a Recordr instance
#' @param id an execution identifier (runId)
#' @param file name of script to match (can be a regex)
#' @param start match executions that started after this time (inclusive)
#' @param end match executions that ended before this time (inclusive)
#' @param tag text of tag to match (can be a regex)
#' @param error text of error message to match (can be a regex)
#' @param seq a run sequence number (can be a range, e.g \code{seq=1:10})
#' @param noop don't delete execution directories
#' @param quiet don't print any informational messages to the display
#' @export
setGeneric("deleteRuns", function(recordr, ...) {
  standardGeneric("deleteRuns")
})

#' @describeIn Recordr
setMethod("deleteRuns", signature("Recordr"), function(recordr, id = as.character(NA), file = as.character(NA), 
                                                       start = as.character(NA), end = as.character(NA), 
                                                       tag = as.character(NA), error = as.character(NA), 
                                                       seq = as.integer(NA), noop = FALSE, quiet = FALSE) {

  runs <- selectRuns(recordr, runId=id, script=file, startTime=start, endTime=end, tag=tag, errorMessage=error, seq=seq, delete=TRUE)
  if (nrow(runs) == 0) {
    if (!quiet) {
      message(sprintf("No runs matched search criteria."))
    }
    return(invisible(runs))
  } else {
    if (!quiet) {
      if (noop) {
        message(sprintf("The following %d runs would have been deleted:\n", nrow(runs)))
      } else {
        message(sprintf("The following %d runs have been deleted:\n", nrow(runs)))
      }
    }
  }
  
  if (! quiet) {
    printRun(headerOnly=TRUE)
  }
  
  # Loop through selected runs
  for(i in 1:nrow(runs)) {
    row <- runs[i,]
    thisrunId <- row["executionId"]
    thisRunDir <- sprintf("%s/runs/%s", recordr@recordrDir, thisrunId)
    if (!noop) {
      if(thisRunDir == sprintf("%s/runs", recordr@recordrDir) || thisRunDir == "") {
        stop(sprintf("Error determining directory to remove, directory: %s", thisRunDir))
      }
      unlink(thisRunDir, recursive = TRUE)
    }
    if (! quiet) {
      printRun(row)
    }
  }
  invisible(runs)
})
  
#' List all runs recorded by record() or startRecord()
#' @description If no search terms are specified, then all runs are listed. The
#' method arguments are search terms that limit the runs listed, with anly runs listed that
#' match all arguments. 
#' @details Each of the \code{"start"} and \code{"end"} can be used are used to specify a time
#' range to find runs that started execution or ended in the specified time range. For examples, specifying
#' \code{"start=c("2015-01-01, "2015-02-01)} will cause the search to return any execution with a starting
#' time in the first month of 2015. A time range can also be entered for when executions ended using the 
#' \code{"end"} parameter.
#' @param recordr a Recordr instance
#' @param file name of script to match (can be a regex)
#' @param start match runs that started in this time range (inclusive)
#' Times must be entered in the form 'YYYY-MM-DD HH:MM:SS' but can be shortened to not less that "YYYY"
#' @param end A character value runs that ended in this time range (inclusive)
#' Times must be entered in the form 'YYYY-MM-DD HH:MM:SS' but can be shortened to not less that "YYYY"
#' @param tag text of tag to match (can be a regex)
#' @param error text of error message to match (can be a regex)
#' @param seq a run sequence number (can be a range, e.g \code{seq=1:10})
#' @param quiet don't print any informational messages to the display
#' @param orderBy the column that will be used to sort the output. This can include a minus sign before the name, e.g. -startTime
#' @return data frame containing information for each run
#' @export
#' @examples \dontrun {
#'   rc <- new("Recordr")
#'   listRuns(rc, start=c("2015-01-01", "2015-01-02) end="2015-01-07")
#' }
setGeneric("listRuns", function(recordr, ...) {
  standardGeneric("listRuns")
})

#' @describeIn Recordr
setMethod("listRuns", signature("Recordr"), function(recordr, id=as.character(NA), script=as.character(NA), start = as.character(NA), end=as.character(NA), tag=as.character(NA), 
                                                     error=as.character(NA), seq=as.character(NA), quiet=FALSE, orderBy = "-startTime") {
  
  runs <- selectRuns(recordr, runId=id, script=script, startTime=start, endTime=end, tag=tag, errorMessage=error, seq=as.character(seq), orderBy=orderBy)
  if (nrow(runs) == 0) {
    if (!quiet) {
      message(sprintf("No runs matched search criteria."))
    }
    return(invisible(runs))
  }

  if (!quiet) {
    # Print header line
    if (!quiet) printRun(headerOnly = TRUE)
    # Loop through selected runs
    for(i in 1:nrow(runs)) {
      row <- runs[i,]
      printRun(row)
    }
  }
  
  invisible(runs)
})

# Internal function used to print execution metadata for a single run
# For the fields that can have variable width content, specify a maximum
# length that will be displayed.
# @param row the row (character vector) to print
# @param headerOnly if TRUE then only the header line is printed, if FALSE then only the row is printed
# authoer: slaughter

printRun <- function(row=list(), headerOnly = FALSE) {
  
  tagLength = 20
  scriptNameLength = 30
  errorMsgLength = 30
  
  #fmt <- "%-20s %-20s %-19s %-19s %-36s %-36s %-19s %-30s\n"
  fmt <- paste("%-6s", "%-", sprintf("%2d", scriptNameLength), "s", 
               " %-", sprintf("%2d", tagLength), "s",
               # " %-19s %-19s %-45s %-19s",
               " %-19s %-19s %-7s %-19s",
               " %-", sprintf("%2d", errorMsgLength), "s", "\n", sep="")
  
  if (headerOnly) {
    cat(sprintf(fmt, "Seq", "Script", "Tag", "Start Time", "End Time", "Run Id", "Published Time", "Error Message"), sep = " ")
  } else {
    console          <- row[["console"]]
    if(console) {
      thisScript <- "Console log"
    }
    else {
      thisScript       <- row[["softwareApplication"]]
      # Print shortened form of script name, e.g. "/home/slaugh...ocalReadWrite.R"
      if(nchar(thisScript) > scriptNameLength-nchar("...")) {
        thisScript       <- sprintf("%s...%s", substring(thisScript, 1, 12), substring(thisScript, nchar(thisScript)-14, nchar(thisScript)))
      } 
    }
    thisStartTime    <- row[["startTime"]]
    thisEndTime      <- row[["endTime"]]
    thisRunId       <- row[["executionId"]]
    thisRunId       <- sprintf("...%s", substring(thisRunId, nchar(thisRunId)-3, nchar(thisRunId)))
    #thisPackageId    <- row[["datapackageId"]]
    thisPublishTime  <- row[["publishTime"]]
    thisErrorMessage <- row[["errorMessage"]]
    thisTag         <- row[["tag"]]
    thisSeq         <- row[["seq"]]
    cat(sprintf(fmt, thisSeq, strtrim(thisScript, scriptNameLength), strtrim(thisTag, tagLength), thisStartTime, 
                thisEndTime, thisRunId, thisPublishTime, strtrim(thisErrorMessage, errorMsgLength)), sep = " ")
  }
}

#' View detailed information for an execution
#' @description Detailed information for an execution and any packags created by that exectution are printed to the display.
#' @param recordr a Recordr instance
#' @param file name of script to match (can be a regex)
#' @param start match runs that started after this time (inclusive)
#' @param end match runs that ended before this time (inclusive)
#' @param tag text of tag to match (can be a regex)
#' @param error text of error message to match (can be a regex)
#' @param seq a run sequence number (can be a range, e.g \code{seq=1:10})
#' @param page a logical value - if true then through the results if multiple runs will be displayed
#' @export
setGeneric("viewRuns", function(recordr, ...) {
  standardGeneric("viewRuns")
})

#' @describeIn Recordr
setMethod("viewRuns", signature("Recordr"), function(recordr, id=as.character(NA), file=as.character(NA), start=as.character(NA), end=as.character(NA), tag=as.character(NA), error=as.character(NA),
                                                 seq=as.character(NA), orderBy="-startTime", sections=c("details","used","generated"), showProv=FALSE, page=TRUE, quiet=TRUE) {
  
  runs <- selectRuns(recordr, runId=id, script=file, startTime=start, endTime=end, tag=tag, errorMessage=error, seq=seq, orderBy=orderBy)
  
  if (nrow(runs) == 0) {
    if (!quiet) {
      message(sprintf("No runs matched search criteria."))
    }
    return(invisible(runs))
  }
        
  # Loop through selected runs
  for(i in 1:nrow(runs)) {     
    thisRow <- runs[i,]       
    executionId         <- thisRow[["executionId"]]
    tag                 <- thisRow[["tag"]]
    datapackageId       <- thisRow[["datapackageId"]]
    user                <- thisRow[["user"]]
    subject             <- thisRow[["subject"]]
    hostId              <- thisRow[["hostId"]]
    startTime           <- thisRow[["startTime"]]
    operatingSystem     <- thisRow[["endTime"]]
    runtime             <- thisRow[["runtTime"]]
    softwareApplication <- thisRow[["softwareApplication"]]
    moduleDependencies  <- thisRow[["moduleDependencies"]]
    endTime             <- thisRow[["endTime"]]
    errorMessage        <- thisRow[["errorMessage"]]
    publishTime         <- thisRow[["publishTime"]]
    console             <- as.logical(thisRow[["console"]])
    publishNodeId       <- thisRow[["publishNodeId"]]
    publishId           <- thisRow[["publishId"]]
    seq                 <- thisRow[["seq"]]
    thisRunDir <- sprintf("%s/runs/%s", recordr@recordrDir, executionId)
    
    # Read the archived data package
    packageFile <- sprintf("%s/%s.pkg", thisRunDir, thisRow[["datapackageId"]])
    # Deserialize saved data package
    pkg <- readRDS(file=packageFile)
    relations <- getRelationships(pkg)
    scriptURL <- relations[relations$predicate == "http://www.w3.org/ns/prov#hadPlan","object"]
    # Clear screen before showing results if we are paging the results
    if (i == 1 && page) cat("\014")
    # Print out [DETAILS] section
    scriptNameLength=50
    if (is.element("details", sections)) {
      cat(sprintf("[details]: Run details\n"))
      cat(sprintf("----------------------\n"))
      thisScript <- softwareApplication
      if(console) {
        cat(sprintf("Started recording console input at %s\n", startTime))
      } else {      
        if(nchar(thisScript) > scriptNameLength-nchar("...")) {
          thisScript <- sprintf("%s...%s", substring(thisScript, 1, 15), substring(thisScript, nchar(thisScript)-3, nchar(thisScript)))
        } 
        cat(sprintf("%s was executed on %s\n", dQuote(thisScript), startTime))
      }
      cat(sprintf("Tag: %s\n", dQuote(tag)))
      cat(sprintf("Run sequence #: %d\n", seq))
      if(is.na(publishTime) || is.null(publishTime)) {
        published <- FALSE
        publishTime <- "Not published"
        publishViewURL <- as.character(NA)
      } else {
        published <- TRUE
        publishViewURL <- sprintf("%s/%s", D1_View_URL, publishedId)
      }
      
      cat(sprintf("Publish date: %s\n", publishTime))
      cat(sprintf("Published to: %s", publishNodeId))
      cat(sprintf("Published Id: ", publishId))
      cat(sprintf("View at: ", publishViewURL))
      cat(sprintf("Run by user: %s\n", user))
      cat(sprintf("Account subject: %s\n", subject))
      cat(sprintf("Run Id: %s\n", executionId))
      cat(sprintf("Data package Id: %s\n", datapackageId))
      cat(sprintf("HostId: %s\n", hostId))
      cat(sprintf("Operating system: %s\n", operatingSystem))
      cat(sprintf("Runtime: %s\n", runtime))
      cat(sprintf("Dependencies: %s\n", moduleDependencies))
      if(console) {
        cat(sprintf("Record console input start time: %s\n", startTime))
        cat(sprintf("Record console input end time: %s\n", endTime))
      } else {
        cat(sprintf("Run start time: %s\n", startTime))
        cat(sprintf("Run end time: %s\n", endTime))
      }
      cat(sprintf("Error message from this run: %s\n", errorMessage))
      # Find the data package in the recordr run directories
      if (! file.exists(thisRunDir)) {
        msg <- sprintf("Directory not found for execution identifier: %s", executionId)
        message(msg)
        next
      }

      #       if(published) {
      #         fmt <- paste("%-", sprintf("%2d", fileNameLength), "s", 
      #                      " %-12s %-19s %-50s\n", sep="")
      #         cat(sprintf(fmt, "\nFilename", "Size (kb)", "Modified time", "DataONE URL"), sep = " ")
      #       }
      #       else {
      #         fmt <- paste("%-", sprintf("%2d", fileNameLength), "s", 
      #                      " %-12s %-19s\n", sep="")
      #         cat(sprintf(fmt, "\nFilename", "Size (kb)", "Modified time"), sep = " ")
      #       }
      #     }

    }
    
    # Read the info file once, and prepare this dfs of read files and generated files
    if(is.element("used", sections) || is.element("generated", sections)) {
      fstats <- getFileInfo(recordr, executionId)
      fstatsRead <- fstats[fstats$access=="read",]
      fstatsRead <- fstats[order(basename(fstatsRead$filePath)),]
      fstatsWrite <- fstats[fstats$access=="write",]
      fstatsWrite <- fstats[order(basename(fstatsWrite$filePath)),]
    }
    
    # "%-30s %-10d %-19s\n"
    fileNameLength = 30    
    if (is.element("used", sections)) {
      cat(sprintf("\n[used]: %d items used by this run\n", nrow(fstatsRead)))
      if(nrow(fstatsRead) > 0) {
        cat(sprintf("-----------------------------------\n"))
        fmt <- paste("%-", sprintf("%2d", fileNameLength), "s",  " %-12s %-19s\n", sep="")
        cat(sprintf(fmt, "Local name", "Size (kb)", "Modified time"), sep = " ")
        for (i in 1:nrow(fstatsRead)) {
          cat(sprintf(fmt, strtrim(basename(fstatsRead[i, "filePath"]), fileNameLength), fstatsRead[i, "size"], fstatsRead[i, "mtime"]), sep = "")
        }
      }
    }
    
    if (is.element("generated", sections)) {
      cat(sprintf("\n[generated]: %d items generated by this run\n", nrow(fstatsWrite)))
      if(nrow(fstatsWrite) > 0) {
        cat(sprintf("-----------------------------------------\n"))
        fmt <- paste("%-", sprintf("%2d", fileNameLength), "s",  " %-12s %-19s\n", sep="")
        cat(sprintf(fmt, "Local name", "Size (kb)", "Modified time"), sep = " ")
        for (i in 1:nrow(fstatsWrite)) {
          cat(sprintf(fmt, strtrim(basename(fstatsWrite[i, "filePath"]), fileNameLength), fstatsWrite[i, "size"], fstatsWrite[i, "mtime"]), sep = "")
        }
      }
    }
    
    #     dsDown <- relations[relations$predicate=="http://www.w3.org/ns/prov#used",]
    #     if(nrow(dsDown) > 0) cat(sprintf("\nDatasets downloaded/read by this run:\n\n"))
    #     for (i in 1:nrow(dsDown)) {
    #       cat(sprintf("%s\n", dsDown[i, "object"]))
    #     }
    #     
    # Print provenance relationships
    if(showProv) {
      cat(sprintf("\nProvenance relationships:\n"))
      relations <- getRelationships(pkg)
      print(relations)
    }
    # Page console output if multiple runs are being viewed
    if (nrow(runs) > 1 && page) {
      inputLine <- readline("enter <return> to continue, q<return> to quit: ")
      if (inputLine == "q") break
      # Clear the console screen
      cat("\014")
      next
    }
  }
  
  # Return the run information if multiple runs, return data package if just one run
  if (nrow(runs) == 1) {
    invisible(pkg)
  } else {
    invisible(runs)
  }
})

#' Publish a recordr'd execution to DataONE
#' @param recordr a Recordr instance
#' @param id the run identifier for the package to upload to DataONE
#' @param assignDOI a boolean value: if TRUE, assign DOI values for system metadata, otherwise assign uuid values
#' @param update a boolean value: if TRUE, republish a previously published execution
#' @export
setGeneric("publishRun", function(recordr, id, ...) {
  standardGeneric("publishRun")
})

#' @describeIn Recordr
setMethod("publishRun", signature("Recordr"), function(recordr, id, assignDOI=FALSE, update=FALSE) {
  
  runDir <- sprintf("%s/runs/%s", recordr@recordrDir, id)
  if (! file.exists(runDir)) {
    msg <- sprintf("A directory was not found for execution identifier: %s", id)
    stop(msg)
  }
  
  node <- getConfig(recordrEnv$config, "target_member_node_id")
  cm <- CertificateManager()
  isExpired <- isCertExpired(cm)
  user <- showClientSubject(cm)
  cn <- CNode(getConfig(recordrEnv$config, "dataone_env"))                     # Use Testing repository
  message(sprintf("Obtaining member node information for %s", node))
  mn <- getMNode(cn, node)
  if (is.null(mn)) {
    stop(sprintf("Member node %s encounted an error on the get() request", node))
  }

  execMeta <- readExecMeta(recordr, id)
  if (! is.null(execMeta)) {
    packageId <-  execMeta@datapackageId
    cat(sprintf("Package identifier: %s\n", packageId))
    packageFile <- sprintf("%s/%s.pkg", runDir, packageId)
    # Deserialize saved data package
    pkg <- readRDS(file=packageFile)
  }
  
  # See if this execution has been published before
  if (!is.na(execMeta@publishTime)) {
    msg <- sprintf("The datapackage for this execution was published on %s\nThe datapackage id is %s", execMeta@publishTime, packageId)
    stop(msg)
  }
  
  # Stop if the DataONE certificate is expired
  if(isExpired) {
    stop("Please create a valid DataONE certificate before calling publish()")
  }
  
  # Upload each data object that was added to the datapackage
  for (dataObjId in getIdentifiers(pkg)) {
    dataObj <- getMember(pkg, dataObjId)
    message(sprintf("Uploading id: %s\n", dataObjId))
    # upload data zip to the data repository
    # TODO: Hangle datapackage objects where data object resides on disk (currently recordr::record() doesn't save objects this way)
    dataFile <- tempfile()
    # write out data object to file so we can get checksum and pass data to DataONE create
    dataBytes <- getData(pkg, dataObjId)
    writeBin(dataBytes, dataFile)
    # get access policy, replicate nodes from configuration id
    sysmeta <- createSysmeta(mn, dataFile, dataObjId, format=getFormatId(dataObj), public=TRUE, replicate=TRUE, user=user)
    sysmetaStr <- serializeSystemMetadata(sysmeta)
    writeLines(sysmetaStr, sprintf("%s/sysmeta_%s.xml", runDir, dataObjId))
    
    # Upload the data to the MN using create(), checking for success and a returned identifier
    returnId <- dataone::create(mn, dataObjId, dataFile, sysmeta)
    if (is.null(returnId) || !grepl(dataObjId, xmlValue(xmlRoot(returnId)))) {
     # TODO: Process the error
     message(paste0("Error on returned identifier: ", returnId))
    }
  }
  
  # create metadata for the directory
  # TODO: Create project specific metadata templates, use configuration API to fill in
  mdfile <- sprintf("%s/metadata.R", path.expand("~"), recordr@recordrDir)
  success <- source(mdfile, local=TRUE)
  
  # Generate a unique identifier for the metadatobject
  if (assignDOI) {
    metadata_id <- generateIdentifier(mn, "DOI")
    # TODO: check if we actually got one, if not then error
    system <- "doi"
  } else {
    metadata_id <- paste0("urn:uuid:", UUIDgenerate())
    system <- "uuid"
  }
  execMeta@publishId <- metadata_id
  
  eml <- makeEML(metadata_id, system, title, creators, abstract, methodDescription, geo_coverage, temp_coverage, pkg, mn@endpoint)
  eml_xml <- as(eml, "XMLInternalElementNode")
  #print(eml_xml)
  # Write the eml file to the execution directory
  eml_file <- sprintf("%s/%s.eml", runDir, metadata_id)
  saveXML(eml_xml, file = eml_file)
  #message(sprintf("Saved EML to file: %s\n", eml_file))
  
  # upload metadata to the repository
  format="eml://ecoinformatics.org/eml-2.1.1"
  
  sysmeta <- createSysmeta(mn, eml_file, metadata_id, format, public=TRUE, replicate=TRUE, user=user)
  #sysmetaStr <- serializeSystemMetadata(sysmeta)
  #writeLines(sysmetaStr, sprintf("%s/sysmeta_%s.xml", runDir, metadata_id))
  
  returnId <- dataone::create(mn, metadata_id, eml_file, sysmeta)
  if (is.null(returnId) || !grepl(metadata_id, xmlValue(xmlRoot(returnId)))) {
    # TODO: Process the error
    message(paste0("Error on returned identifier: ", returnId))
  } else {
    message(sprintf("Uploaded metadata with id: %s\n", metadata_id))
  }
  
  dataIds <- getIdentifiers(pkg)
  mdo <- new("DataObject", id=metadata_id, filename=eml_file, format=format, user=user, mnNodeId=node)  
  addData(pkg, mdo)
  #unlink(eml_file)
  insertRelationship(pkg, subjectID=metadata_id, objectIDs=dataIds)
  serializationId <- execMeta@datapackageId
  
  # create and upload the resource map for this datapackage
  resMapFn <- sprintf("%s/%s.rdf", runDir, serializationId)
  status <- serializePackage(pkg, resMapFn, id=serializationId)
  #message(paste0("serializing package to file: ", resMapFn))
  message(sprintf("Uploading resource map with id: %s\n", serializationId))
  
  format <- OREterms_URI
  sysmeta <- createSysmeta(mn, resMapFn, serializationId, format, public=TRUE, replicate=TRUE, user=user)
  sysmetaStr <- serializeSystemMetadata(sysmeta)
  writeLines(sysmetaStr, sprintf("%s/sysmeta_%s.xml", runDir, serializationId))
  returnId <- dataone::create(mn, serializationId, resMapFn, sysmeta)
  if (is.null(returnId) || !grepl(serializationId, xmlValue(xmlRoot(returnId)))) {
    # TODO: Process the error
    message(paste0("Error on returned identifier: ", returnId))
  }
  
  # Record the time that this execution was published
  execMeta@publishTime <- as.character(Sys.time())
  cm <- CertificateManager()
  execMeta@subject <- showClientSubject(cm)
  mdFilePath <- writeExecMeta(recordr, execMeta)
  
  #unlink(tf)
  invisible(metadata_id)
})

recordrShutdown <- function() {
  if (is.element(".recordr", base::search())) detach(".recordr")
  #if (is.element(".recordrConfig", base::search())) detach(".recordrConfig")
}
#' Create a minimal EML document.
#' Creating EML should be more complete, but this minimal example will suffice to create a valid document.
makeEML <- function(id, system, title, creators, abstract=NA, methodDescription=NA, geo_coverage=NA, temp_coverage=NA, datapackage=NULL, endpoint=NA) {
  #dt <- eml_dataTable(dat, description=description)
  oe_list <- as(list(), "ListOfotherEntity")
  if (!is.null(datapackage)) {
    for (id in getIdentifiers(datapackage)) {
      print(paste("Creating entity for ", id, sep=" "))
      current_do <- getMember(datapackage, id)
      oe <- new("otherEntity", entityName=basename(current_do@filename), entityType="text/csv")
      oe@physical@objectName <- basename(current_do@filename)
      oe@physical@size <- current_do@sysmeta@size
      if (!is.na(endpoint)) {
        oe@physical@distribution@online@url <- paste(endpoint, id, sep="/")
      }
      f <- new("externallyDefinedFormat", formatName="Comma Separated Values")
      df <- new("dataFormat", externallyDefinedFormat=f)
      oe@physical@dataFormat <- df
      oe_list <- c(oe_list, oe)
    }
  }
  creator <- new("ListOfcreator", lapply(as.list(with(creators, paste(given, " ", surname, " ", "<", email, ">", sep=""))), as, "creator"))
  ds <- new("dataset",
            title = title,
            abstract = abstract,
            creator = creator,
            contact = as(creator[[1]], "contact"),
            #coverage = new("coverage"),
            pubDate = as.character(Sys.Date()),
            #dataTable = c(dt),
            otherEntity = as(oe_list, "ListOfotherEntity")
            #methods = new("methods"))
  )
  
  if (!is.na(methodDescription)) {
    ms <- new("methodStep", description=methodDescription)
    listms <- new("ListOfmethodStep", list(ms))
    ds@methods <- new("methods", methodStep=listms)
  }
  ds@coverage <- coverageElement(geo_coverage, temp_coverage)
  eml <- new("eml",
             packageId = id,
             system = system,
             dataset = ds)
  return(eml)
}

createSysmeta <- function(mn, filename, identifier, format, public=TRUE, replicate=TRUE, user=NA) {
    
  if(is.na(user)) {
    stop("A username must be specified for SystemMetadata object.")
  }
  # Create SystemMetadata for the object
  size <- file.info(filename)$size
  sha1 <- digest(filename, algo="sha1", serialize=FALSE, file=TRUE)
  sysmeta <- new("SystemMetadata", identifier=identifier, formatId=format, size=size, submitter=user, rightsHolder=user, checksum=sha1, 
                 originMemberNode=mn@identifier, authoritativeMemberNode=mn@identifier)
  sysmeta@replicationAllowed <- replicate
  sysmeta@numberReplicas <- 2
  sysmeta@preferredNodes <- list("urn:node:mnUCSB1", "urn:node:mnUNM1", "urn:node:mnORC1")
  if (public) {
    sysmeta <- addAccessRule(sysmeta, "public", "read")
  }
  
  return(sysmeta)

}

#' Create a geographic coverage element from a description and bounding coordinates
geoCoverage <- function(geoDescription, west, east, north, south) {
  bc <- new("boundingCoordinates", westBoundingCoordinate=west, eastBoundingCoordinate=east, northBoundingCoordinate=north, southBoundingCoordinate=south)
  geoDescription="Southeast Alaska"
  gc <- new("geographicCoverage", geographicDescription=geoDescription, boundingCoordinates=bc)
  return(gc)
}

## Create a temporal coverage object
temporalCoverage <- function(begin, end) {
  bsd <- new("singleDateTime", calendarDate=begin)
  b <- new("beginDate", bsd)
  esd <- new("singleDateTime", calendarDate=end)
  e <- new("endDate", esd)
  rod <- new("rangeOfDates", beginDate=b, endDate=e)
  temp_coverage <- new("temporalCoverage", rangeOfDates=rod)
  return(temp_coverage)
}

#' Create a coverage element
coverageElement <- function(gc, tempc) {
  coverage <- new("coverage", geographicCoverage=gc, temporalCoverage=tempc)
  return(coverage)
}

#' Get a database connection
getDBconnection <- function(dbFile) {
  dbConn <- dbConnect(drv=RSQLite::SQLite(), dbname=dbFile)
  if (dbIsValid(dbConn)) {
    return(dbConn)
  } else {
    message(sprintf("Error opening database connection to %s\n", dbFile))
    return(NULL)
  }
}
  