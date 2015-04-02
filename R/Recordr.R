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

#' Manage R script executions, capture provenance, upload data producs to DataONE
#' @description Recordr provides methods to run R scripts and record the files that were
#' read and written by the script, along with information about the execution.
#' This provenance information along with any files created by the script can then be
#' combined into a data package to aid in uploading data products and their description
#' to a data repository.
#' @slot recordrDir value of type \code{"character"} containing a path to the Recordr working directory
#' @rdname Recordr-class
#' @author Peter Slaughter
#' @import dataone
#' @import datapackage
#' @import uuid
#' @import tools
#' @import digest
#' @import XML
#' @import EML
#' @include Constants.R
#' @export
setClass("Recordr", slots = c(recordrDir = "character")
)

#' Initialize a Recorder object
setMethod("initialize", signature = "Recordr", definition = function(.Object,
                                                                     recordrDir = normalizePath("~/.recordr")) {
  
  .Object@recordrDir <- recordrDir
  return(.Object)
})

##########################
## Methods
##########################

#' Begin recording provenance for an R session. 
#' @description This method starts the recording process and the method endRecord() completes it.
#' @details The startRecord() method can be called from the R console to begin a recording session
#' during which provenance is captured for any functions that are inspected by Recordr. This recordr
#' session can be closed by calling the \code{endRecord()} method.
#' @param recordr a Recordr instance
#' @param tag a string that is associated with this run
#' @param .file the filename for the script to run (only used internally when startRecord() is called from record())
#' @param .console a logical argument that is used internally by the recordr package
#' @return execution identifier
#' @export
setGeneric("startRecord", function(recordr, ...) {
  standardGeneric("startRecord")
})

#' @describeIn Recordr
setMethod("startRecord", signature("Recordr"), function(recordr, tag="", .file=as.character(NA), .console=TRUE) {
  
  # Check if a recording session has already been started.
  if (is.element(".recordr", base::search())) {
    message("A Recordr session is already active. Please run endRecord() if you wish to close this session.")
    return(NULL)
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
  runs <- selectRuns(recordr, matchType="non-specific", orderBy="-endTime")
  if (nrow(runs) == 0) {
    seq <- 1
  } else {
    seq <-  as.integer(runs[1, 'seq']) + 1
  }

  recordrEnv$execMeta@seq <- as.integer(seq)
  # Create an empty D1 datapackage object and make it globally avilable, i.e. available
  # to the masking functions, e.g. "recordr_write.csv".
  # TODO: Read memmber node from session API
  recordrEnv$cnNodeId <- "STAGING2"
  recordrEnv$mnNodeId <- "urn:node:mnTestKNB"
  # TODO: use new() initialization when datapackage is fixed - currently using new() causing
  # an old object to be reused!!!
  #recordrEnv$dataPkg <- new("DataPackage", packageId=recordrEnv$execMeta@datapackageId)
  recordrEnv$dataPkg <- DataPackage(packageId=recordrEnv$execMeta@datapackageId)
    
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
  userId <- recordrEnv$execMeta@accountName
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
  
  on.exit(detach(".recordr"))
  recordrEnv <- as.environment(".recordr")
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
  programD1Obj <- new("DataObject", id=recordrEnv$programId, dataobj=script, format=scriptFmt, user=recordrEnv$execMeta@accountName, mnNodeId=recordrEnv$mnNodeId)    
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
#' @param ... additional parameters that will be passed to the R \code{"base::source()"} function
#' @return the DataONE datapackge created by this run
#' @export
setGeneric("record", function(recordr, file, ...) {
  standardGeneric("record")
})

#' @describeIn Recordr
setMethod("record", signature("Recordr"), function(recordr, file, tag="", ...) {
  # Check if a recording session is active
  if ( is.element(".recordr", base::search())) {
    detach(".recordr")
  }
  
  startRecord(recordr, tag, .file=file, .console=FALSE)
  recordrEnv <- as.environment(".recordr")
  setProvCapture(TRUE)
  # Source the user's script, passing in arguments that they intended for the 'source' call.  
  result = tryCatch ({
    #cat(sprintf("Sourcing file %s\n", filePath))
    # Because we are calling the 'source' function with the packageId, the overridden function
    # for 'source' will not be called, and a provenance entry for this 'source' will not be
    # recorded.
    # Note: ellipse argument is passed from method call so user can pass args to source, if desired.
    base::source(file, local=FALSE, ...)
  }, warning = function(warningCond) {
    slot(recordrEnv$execMeta, "errorMessage") <- warningCond$message
    cat(sprintf("Warning:: %s", recordrEnv$execMeta@errorMessage))
  }, error = function(errorCond) {
    slot(recordrEnv$execMeta, "errorMessage") <- errorCond$message
    cat(sprintf("Error:: %s", recordrEnv$execMeta@errorMessage))
  }, finally = {
    # Disable provenance capture while some housekeeping is done
    setProvCapture(FALSE)

    # Stop recording provenance and finalize the data package    
    pkg <- endRecord(recordr)
    if (is.element(".recordr", base::search())) {
      detach(".recordr", unload=TRUE)
    }
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
#' @param matchType if "specific" then at least one search criteria must be specified and matched
#' @param orderBy the column that will be used to sort the output. This can include a minus sign before the name, e.g. -startTime
#' @export
setGeneric("selectRuns", function(recordr, ...) {
  standardGeneric("selectRuns")
})

#' @describeIn Recordr
setMethod("selectRuns", signature("Recordr"), function(recordr, runId=as.character(NA), script=as.character(NA), startTime=as.character(NA), endTime=as.character(NA), 
                                                       tag=as.character(NA), errorMessage=as.character(NA), seq=as.integer(NA), matchType="specific", orderBy = as.character(NA)) {
  
  colNames = c("script", "tag", "startTime", "endTime", "runId", "packageId", "publishTime", "errorMessage", "console", "seq")
  
  # Find all run directories
  dirs <- list.files(sprintf("%s/runs", recordr@recordrDir))
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
      return(df)
    }
  }
  # Return an empty data frame if no run directories exist
  if (length(dirs) == 0) {
    return(df)
  }
  
  # MatchType controls which run directories are included if no search parameters are specified. If we are
  # searching for runs to delete, (i.e. via deleteRuns), then we don't want to match all runs and delete
  # everything if no search criteria were specified.
  matchTypes <- c("specific", "non-specific")
  if (! is.element(matchType, matchTypes)) {
    msg <- paste("Invalid argument 'matchTypes', must be one of: ", matchTypes)
    stop(msg)
  }
  
  # Loop through run directories
  for (d in dirs) {
    # Create an execution metadata object for this run
    execMeta <- readExecMeta(recordr, d)
    if (! is.null(execMeta)) {
      thisScript       <- execMeta@softwareApplication
      thisTag          <- execMeta@tag
      thisStartTime    <- execMeta@startTime
      thisEndTime      <- execMeta@endTime
      thisrunId       <- execMeta@executionId
      thisPackageId    <- execMeta@datapackageId
      thisPublishTime  <- execMeta@publishTime
      thisErrorMessage <- execMeta@errorMessage
      thisConsole      <- execMeta@console
      thisSeq <- as.integer(execMeta@seq)
      
      # If endTime unset, set to start so that our tests won't fail
      if (is.na(thisEndTime)) thisEndTime <- thisStartTime
      
      match <- FALSE
      # Test each selection argument separately. 
      # The selection criteria are applied in an "and" relationship to each other
      # so exclude this run as soon as any test doesn't match.
      #
      # Check if run sequence number was specified. As this is an integer, it
      # can be specified as a single number or a range, i.e. seq=1, seq=1:10
      if (!all(is.na(seq))) {
        if(is.element(thisSeq, seq)) {
          match <- TRUE 
        } else {
          next
        }
      }
      # Check execution identifier
      if (!is.na(runId)){
        if(!grepl(runId, thisrunId)) {
          next
        }
        match <- TRUE
      }
      # Check for match with argument 'script'
      if (!is.na(script)) {
        # seearch for parameter 'tag' (a regex) in current tag string
        if (!grep(script, thisScript)) {
          next
        }
        match <- TRUE
      }
      # Check for match with argument 'startTime'
      if (!is.na(startTime)) {
        # Current run started before specified time, so skip it
        if (as.POSIXlt(thisStartTime) < as.POSIXlt(startTime)) {
          next
        }
        match <- TRUE
      }
      # Check for match with argument 'endTime'
      if (!is.na(endTime)) {
        # This run ended after specified time, so skip it
        if (as.POSIXlt(thisEndTime) > as.POSIXlt(endTime)) {          
          next
        }
        match <- TRUE
      }
      # Check for match with argument 'tag'
      if (!is.na(tag)) {
        # seearch for parameter 'tag' (a regex) in current tag string
        if (length(grep(tag, thisTag)) == 0) {
          next
        }
        match <- TRUE
      }
      # Check for match with argument 'errorMessage'
      if (!is.na(errorMessage)) {
        # seearch for parameter 'tag' (a regex) in current tag string
        if (length(grep(errorMessage, thisErrorMessage)) == 0) {
          next
        }
        match <- TRUE
      }
      # If "specific" matching was specified, then a definite match has to have been made.
      if (matchType == "specific") {
        if(match == FALSE) {
          next
        }
      }

      if(exists("runMeta")) {
        runMeta <- rbind(runMeta, data.frame(script=thisScript, tag=thisTag, startTime=thisStartTime, endTime=thisEndTime, executionId=thisrunId, 
                                             datapackageId=thisPackageId, publishTime=thisPublishTime, errorMessage=thisErrorMessage, 
                                             console=thisConsole, seq=thisSeq, row.names = NULL, stringsAsFactors = FALSE))
      }
      else {
        runMeta <- data.frame(script=thisScript, tag=thisTag, startTime=thisStartTime, endTime=thisEndTime, executionId=thisrunId, 
                              datapackageId=thisPackageId, publishTime=thisPublishTime, errorMessage=thisErrorMessage, 
                              console=thisConsole, seq=thisSeq, row.names = NULL, stringsAsFactors = FALSE)
      }
    }
  }
  # If we didn't match any runs, return an emply data frame
  if (!exists("runMeta")) {
    return(df)
  } else {
    # Order the run metadata by the specified column
    if (!is.na(orderBy)) {
      if (sortOrder == "descending") {
        colStr <- paste("runMeta[order(runMeta$", orderBy, ", decreasing=TRUE),]", sep="")
      }
      else {
        colStr <- paste("runMeta[order(runMeta$", orderBy, "),]", sep="")
      }
      runMeta <- eval(parse(text=colStr))
    }
    return(runMeta)
  }
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

  runs <- selectRuns(recordr, runId=id, script=file, startTime=start, endTime=end, tag=tag, errorMessage=error, seq=seq, matchType="specific")
  if (nrow(runs) == 0) {
    if (!quiet) {
      message(sprintf("No runs matched search criteria."))
    }
    return(runs)
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
#' @param recordr a Recordr instance
#' @param file name of script to match (can be a regex)
#' @param start match runs that started after this time (inclusive)
#' @param end match runs that ended before this time (inclusive)
#' @param tag text of tag to match (can be a regex)
#' @param error text of error message to match (can be a regex)
#' @param seq a run sequence number (can be a range, e.g \code{seq=1:10})
#' @param quiet don't print any informational messages to the display
#' @param orderBy the column that will be used to sort the output. This can include a minus sign before the name, e.g. -startTime
#' @return data frame containing information for each run
#' @export
setGeneric("listRuns", function(recordr, ...) {
  standardGeneric("listRuns")
})

#' @describeIn Recordr
setMethod("listRuns", signature("Recordr"), function(recordr, file=as.character(NA), start = as.character(NA), end=as.character(NA), tag=as.character(NA), 
                                                     error=as.character(NA), seq=as.integer(NA), quiet=FALSE, orderBy = "-startTime") {
  
  runs <- selectRuns(recordr, script=file, startTime=start, endTime=end, tag=tag, errorMessage=error, seq=seq, matchType="non-specific", orderBy=orderBy)

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
  scriptNameLength = 20
  errorMsgLength = 30
  
  #fmt <- "%-20s %-20s %-19s %-19s %-36s %-36s %-19s %-30s\n"
  fmt <- paste("%-6s", "%-", sprintf("%2d", scriptNameLength), "s", 
               " %-", sprintf("%2d", tagLength), "s",
               " %-19s %-19s %-45s %-19s",
               " %-", sprintf("%2d", errorMsgLength), "s", "\n", sep="")
  
  if (headerOnly) {
    cat(sprintf(fmt, "Seq", "Script", "Tag", "Start Time", "End Time", "Run Identifier", "Published Time", "Error Message"), sep = " ")
  } else {
    thisScript       <- row["script"]
    thisStartTime    <- row["startTime"]
    thisEndTime      <- row["endTime"]
    thisrunId       <- row["executionId"]
    #thisPackageId    <- row["datapackageId"]
    thisPublishTime  <- row["publishTime"]
    thisErrorMessage <- row["errorMessage"]
    thisTag         <- row["tag"]
    thisSeq         <- row["seq"]
    cat(sprintf(fmt, thisSeq, strtrim(thisScript, scriptNameLength), strtrim(thisTag, tagLength), thisStartTime, 
                thisEndTime, thisrunId, thisPublishTime, strtrim(thisErrorMessage, errorMsgLength)), sep = " ")
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
setGeneric("view", function(recordr, ...) {
  standardGeneric("view")
})

#' @describeIn Recordr
setMethod("view", signature("Recordr"), function(recordr, id=as.character(NA), file=as.character(NA), start=as.character(NA), end=as.character(NA), tag=as.character(NA), error=as.character(NA),
                                                 seq=as.character(NA), orderBy="-startTime", showProv=FALSE, page=TRUE) {
  
  runs <- selectRuns(recordr, runId=id, script=file, startTime=start, endTime=end, tag=tag, errorMessage=error, seq=seq, matchType="non-specific", orderBy=orderBy)
  
  if (nrow(runs) == 0) {
    if (!quiet) {
      message(sprintf("No runs matched search criteria."))
    }
    return(invisible(runs))
  }
  
  # Loop through selected runs
  for(i in 1:nrow(runs)) {
    row <- runs[i,]
    thisrunId <- row[["executionId"]]
    thisRunDir <- sprintf("%s/runs/%s", recordr@recordrDir, thisrunId)
    # Clear screen before showing results if we are paging the results
    if (i == 1 && page) cat("\014")
    cat(sprintf("Information for execution: %s\n", thisrunId))
    
    # Find the data package in the recordr run directories
    #dirs <- list.files(recordr@runDir)
    thisRunDir <- sprintf("%s/runs/%s", recordr@recordrDir, thisrunId)
    if (! file.exists(thisRunDir)) {
      msg <- sprintf("Directory not found for execution identifier: %s", thisrunId)
      message(msg)
      next
    }
    
    execMeta <- readExecMeta(recordr, thisrunId)
    if (! is.null(execMeta)) {
      packageId <-  execMeta@datapackageId
      packageFile <- sprintf("%s/%s.pkg", thisRunDir, packageId)
      # Deserialize saved data package
      pkg <- readRDS(file=packageFile)
      relations <- getRelationships(pkg)
      if(is.na(execMeta@publishTime) || execMeta@publishTime == "") {
        published <- FALSE
        cat(sprintf("This run has not been published\n"))
      } else {
        published <- TRUE
        cat(sprintf("This run was published to DatONE at: \"%s\"\n", execMeta@publishTime))
        cat(sprintf("Data package pid: %s\n", execMeta@datapackageId))
        scriptURL <- relations[relations$predicate == "http://www.w3.org/ns/prov#hadPlan","object"]
        # Add in the CN resolve URL if not already added
        if(grepl(D1_CN_Resolve_URL, scriptURL)) {
          scriptURL <- sprintf("%s/%s", D1_CN_Resolve_URL, scriptURL)
        }
      }
      if (execMeta@softwareApplication != "") cat(sprintf("Script executed: %s\n", execMeta@softwareApplication))
      #cat(sprintf("Package identifier: %s\n", packageId))

      cat(sprintf("tag: %s\t", execMeta@tag))
      cat(sprintf("sequence #: %d\n", execMeta@seq))
      
      fileNameLength = 30
      # "%-30s %-10d %-19s\n"
      cat(sprintf("\nFiles generated by this run:\n"))
      
      if(published) {
        fmt <- paste("%-", sprintf("%2d", fileNameLength), "s", 
                     " %-12s %-19s %-50s\n", sep="")
        cat(sprintf(fmt, "\nFilename", "Size (kb)", "Modified time", "DataONE URL"), sep = " ")
      }
      else {
        fmt <- paste("%-", sprintf("%2d", fileNameLength), "s", 
                     " %-12s %-19s\n", sep="")
        cat(sprintf(fmt, "\nFilename", "Size (kb)", "Modified time"), sep = " ")
      }
      infoFile <- sprintf("%s/fileInfo.csv", thisRunDir)
      fstats <- getFileInfo(recordr, thisrunId)
      # Order the list of files by file most recently modified
      # Note: we could also sort by basename of the file: fstats[order(basename(rownames(fstats))),]
      fstats <- fstats[order(fstats$mtime),]
      # Print out file information
      for (i in 1:nrow(fstats)) {
        if(fstats[i, "access"] != "write") next
        if (published) {
          pid <- fstats[i,"dataObjId"]
          pidURL <- sprintf("%s/%s", D1_CN_Resolve_URL, pid)
          #cat(sprintf(fmt, strtrim(basename(rownames(fstats)[i]), fileNameLength), fstats[i, "size"], fstats[i, "mtime"], pidURL), sep = "")
          cat(sprintf(fmt, strtrim(basename(fstats[i, "filePath"]), fileNameLength), fstats[i, "size"], fstats[i, "mtime"], pidURL), sep = "")
          
        } else {
          #cat(sprintf(fmt, strtrim(basename(rownames(fstats)[i]), fileNameLength), fstats[i, "size"], fstats[i, "mtime"]), sep = "")
          cat(sprintf(fmt, strtrim(basename(fstats[i, "filePath"]), fileNameLength), fstats[i, "size"], fstats[i, "mtime"]), sep = "")
        }
      }
      
      dsDown <- relations[relations$predicate=="http://www.w3.org/ns/prov#used",]
      if(nrow(dsDown) > 0) cat(sprintf("\nDatasets downloaded/read by this run:\n\n"))
      for (i in 1:nrow(dsDown)) {
        cat(sprintf("%s\n", dsDown[i, "object"]))
      }
      
      # Print provenance relationships
      if(showProv) {
        cat(sprintf("\nProvenance relationships:\n"))
        relations <- getRelationships(pkg)
        print(relations)
      }
    } else {
      msg <- sprintf("Unable to read execution metadata from working directory: %s", thisRunDir)
      stop(msg)
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
setGeneric("publish", function(recordr, id, ...) {
  standardGeneric("publish")
})

#' @describeIn Recordr
setMethod("publish", signature("Recordr"), function(recordr, id, assignDOI=FALSE, update=FALSE) {
  
  runDir <- sprintf("%s/runs/%s", recordr@recordrDir, id)
  if (! file.exists(runDir)) {
    msg <- sprintf("A directory was not found for execution identifier: %s", id)
    stop(msg)
  }
  
  node <- "urn:node:mnTestKNB"
  cm <- CertificateManager()
  isExpired <- isCertExpired(cm)
  user <- showClientSubject(cm)
  cn <- CNode("STAGING2")                     # Use Testing repository
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
  
  format <- "http://www.openarchives.org/ore/terms"
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
  mdFilePath <- writeExecMeta(recordr, execMeta)
  
  #unlink(tf)
  invisible(metadata_id)
})

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