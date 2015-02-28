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

#' Recordr provides methods for capturing, managing and publishing R processing provenance.
#' @slot recordrDir value of type \code{"character"} containing a path to the Recordr working directory
#' @rdname Recordr-class
#' @author slaughter
#' @import dataone
#' @import datapackage
#' @import uuid
#' @import tools
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
#' @description
#' This method starts the recording process and the method endRecord() completes it.
#' @param Recordr object
#' @param tag a string that is associated with this run
#' @param scriptPath the script path (only used internally when startRecord() is called from record())
#' @author slaughter
#' @export
setGeneric("startRecord", function(recordr, tag="", scriptPath="", ...) {
  standardGeneric("startRecord")
})

#' @export
setMethod("startRecord", signature("Recordr"), function(recordr, tag="", scriptPath=as.character(NA), ...) {
  
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
  if (is.na(scriptPath)) {
    recordrEnv$scriptPath = ""
    recordrEnv$programId <- R.Version()$version.string
    script = "R console commands"
  } else {
    recordrEnv$scriptPath <- scriptPath
    #recordrEnv$programId <- sprintf("%s_%s", basename(scriptPath), UUIDgenerate() )
    recordrEnv$programId <- sprintf("urn:uuid:%s", UUIDgenerate())
    script <- charToRaw(paste(readLines(recordrEnv$scriptPath), collapse = ''))
  }
  
  recordrEnv$execMeta <- ExecMetadata(recordrEnv$scriptPath, tag=tag)
  # Create an empty D1 datapackage object and make it globally available, i.e. available
  # to the masking functions, e.g. "recordr_write.csv".
  # TODO: Read memmber node from configuration API
  recordrEnv$mnNodeId <- "urn:node:mnDemo5"
  recordrEnv$dataPkg <- new(Class="DataPackage", packageId=recordrEnv$execMeta@datapackageId)
  
  # Store the provONE relationship: execution -> prov:qualifiedAssociation -> association
  #associationId <- sprintf("%s_%s", "association", UUIDgenerate())
  associationId <- sprintf("urn:uuid:%s", UUIDgenerate())
  insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=associationId, predicate=provQualifiedAssociation)
  
  # Create a data package object for the program that we are running and store it.
  scriptFmt <- "text/plain"
  programD1Obj <- new("DataObject", recordrEnv$programId, script, scriptFmt, recordrEnv$execMeta@accountName, recordrEnv$mnNodeId)
                      function(.Object, id, dataobj=NA, format=NA, user=NA, mnNodeId=NA, filename=as.character(NA))
  # Set access control on the action object to be public
  #setPublicAccess(programD1Obj)
  
  # Store the Prov relationship: association -> prov:hadPlan -> program
  addData(recordrEnv$dataPkg, programD1Obj)
  insertRelationship(recordrEnv$dataPkg, subjectID=associationId, objectIDs=recordrEnv$programId, predicate=provHadPlan)
  
  # Store the Prov relationship: association -> prov:agent -> user
  insertRelationship(recordrEnv$dataPkg, subjectID=associationId , objectIDs=recordrEnv$execMeta@accountName , predicate=provAgent, objectType=xsdString)
  
  # Override R functions
  #recordrEnv$source <- recordr::recordr_source
  
  # override DataONE V1.1.0 methods
  #recordrEnv$getD1Object <- recordr::recordr_getD1Object
  #recordrEnv$createD1Object <- recordr::recordr_createD1Object
  # override DataONE v2.0 methods
  recordrEnv$get <- recordr::recordr_D1MNodeGet
  # override R functions
  recordrEnv$read.csv <- recordr::recordr_read.csv
  recordrEnv$write.csv <- recordr::recordr_write.csv
  
  # Create the run metadata directory for this record()
  dir.create(sprintf("%s/runs/%s", recordr@recordrDir, recordrEnv$execMeta@executionId), recursive = TRUE)
  # Put recordr working directory in so masked functions can access it.
  recordrEnv$recordrDir <- recordr@recordrDir
  setProvCapture(TRUE)
  # The Recordr provenance capture capability is now setup and when startRecord() returns, the
  # user can continue to work in the calling context, i.e. the console and provenance will be
  # capture until endRecord() is called.
})

#' End the recording session that was started by startRecord()
#' Prepare and return a DataPackage that contains all derived products created during the 
#' session and all recorded provenance relationships
#' @param Recordr object
#' @return pkg a DataONE data package
#' @author slaughter
#' @export
setGeneric("endRecord", function(recordr) {
  standardGeneric("endRecord")
})

#' @export
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
      # Create the run metadata directory for this record()
      dir.create(runDir, recursive = TRUE)
  }
  
  # Disable provenance capture while some housekeeping is done
  setProvCapture(FALSE)
  recordrEnv$execMeta@endTime <- as.character(Sys.time())
  writeExecMeta(recordr, recordrEnv$execMeta)
  # Generate a uuid for this serialization object
  serializationId = sprintf("%s_%s", "resourceMap", UUIDgenerate())
  filePath <- sprintf("%s/%s.rdf", runDir, serializationId)
  status <- serializePackage(recordrEnv$dataPkg, file=filePath, id=serializationId)
  
  # Save the package object to the run directory
  dataPkg <- recordrEnv$dataPkg
  filePath <- sprintf("%s/%s.pkg", runDir, recordrEnv$execMeta@datapackageId)
  saveRDS(dataPkg, file=filePath)
  return(dataPkg)
})

#' Record provenance for an R script execution and create a DataONE DataPackage that
#' contains this provenance information and all derived products create by the script
#' @param Recordr object
#' @param The filename of the R script to run and collect provenance information for
#' @return the identifier for the DataONE datapackge created by this run
#' @author slaughter
#' @export
setGeneric("record", function(recordr, filePath, tag="", ...) {
  standardGeneric("record")
})

#' @export
setMethod("record", signature("Recordr", "character"), function(recordr, filePath, tag="", ...) {

  startRecord(recordr, tag, filePath)
  recordrEnv <- as.environment(".recordr")
  setProvCapture(TRUE)
  # Source the user's script, passing in arguments that they intended for the 'source' call.
  result = tryCatch ({
    #cat(sprintf("Sourcing file %s\n", filePath))
    base::source(filePath, local=FALSE, ...)
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
    # return a datapackage object
    if (is.element(".recordr", base::search())) {
      detach(".recordr", unload=TRUE)
    }
    return(pkg)
  })
})

#' Select runs that match search parameters
#' @param runIds a list of execution identifiers
#' @param script name of script to match (can be a regex)
#' @param startTime match executions that started after this time (inclusive)
#' @param endTime match executions that ended before this time (inclusive)
#' @param tag text of tag to match (can be a regex)
#' @param errorMessage text of error message to match (can be a regex)
#' @param matchType if "specific" then at least one search criteria must be specified and matched
#' @author slaughter
## @export
setGeneric("selectRuns", function(recordr, ...) {
  standardGeneric("selectRuns")
})

setMethod("selectRuns", signature("Recordr"), function(recordr, runIds = "", script = "", startTime = "", endTime = "", tag = "", errorMessage = "", matchType="specific", orderBy = "") {
  
  colNames = c("script", "tag", "startTime", "endTime", "execId", "packageId", "publishTime", "errorMessage")
  
  # Find all run directories
  dirs <- list.files(sprintf("%s/runs", recordr@recordrDir))
  df <- data.frame(script = character(), 
                   tag = character(),
                   startTime = character(),
                   endTime = character(),
                   execId = character(),
                   packageId = character(), 
                   publishTime = character(),
                   errorMessage = character(),
                   row.names = NULL)
  
  sortOrder = "ascending"
  # Is the column that the user specified for ordering correct?
  if (orderBy != "") {
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
      cat(sprintf("Please use one of the following column names: \"%s\"\n", paste(colNames, collapse = '", "')))
      return(df)
    }
  }
  # Return an empty data frame if no run directories exist
  if (length(dirs) == 0) {
    return(df)
  }
  
  # MatchType controls which run directories are included if no search parameters are specified.
  matchTypes <- c("specific", "non-specific")
  if (! is.element(matchType, matchTypes)) {
    msg <- paste("Invalid argument 'matchTypes', must be one of: ", matchTypes)
    message(msg)
    return(df)
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
      thisExecId       <- execMeta@executionId
      thisPackageId    <- execMeta@datapackageId
      thisPublishTime  <- execMeta@publishTime
      thisErrorMessage <- execMeta@errorMessage
      
      match = FALSE
      # Test each selection argument separately. 
      # The selection criteria are applied in an "and" relationship to each other
      # so exclude this run as soon as any test doesn't match.
      #
      # Check for matching run identifiers in argument 'runIds'. If found, then
      # continue to other tests
      if (runIds[1] != "" ){
        if(!is.element(thisExecId, runIds)) {
          next
        }
        match = TRUE
      }
      # Check for match with argument 'script'
      if (script != "") {
        # seearch for parameter 'tag' (a regex) in current tag string
        if (length(grep(script, thisScript)) == 0) {
          next
        }
        match = TRUE
      }
      # Check for match with argument 'startTime'
      if (startTime != "") {
        # Current run started before specified time, so skip it
        if (as.POSIXlt(thisStartTime) < as.POSIXlt(startTime)) {
          next
        }
        match = TRUE
      }
      # Check for match with argument 'endTime'
      if (endTime != "") {
        # This run ended after specified time, so skip it
        if (as.POSIXlt(thisEndTime) > as.POSIXlt(endTime)) {          
          next
        }
        match = TRUE
      }
      # Check for match with argument 'tag'
      if (tag != "") {
        # seearch for parameter 'tag' (a regex) in current tag string
        if (length(grep(tag, thisTag)) == 0) {
          next
        }
        match = TRUE
      }
      # Check for match with argument 'errorMessage'
      if (errorMessage != "") {
        # seearch for parameter 'tag' (a regex) in current tag string
        if (length(grep(errorMessage, thisErrorMessage)) == 0) {
          next
        }
        match = TRUE
      }
      
      # If "specific" matching was specified, then a definite match has to have been made.
      if (matchType == "specific") {
        if(match == FALSE) {
          next
        }
      }

      if(exists("runMeta")) {
        runMeta <- rbind(runMeta, data.frame(script=thisScript, tag=thisTag, startTime=thisStartTime, endTime=thisEndTime, executionId=thisExecId, datapackageId=thisPackageId, publishTime=thisPublishTime, errorMessage=thisErrorMessage, row.names = NULL, stringsAsFactors = FALSE))
      }
      else {
        runMeta <- data.frame(script=thisScript, tag=thisTag, startTime=thisStartTime, endTime=thisEndTime, executionId=thisExecId, datapackageId=thisPackageId, publishTime=thisPublishTime, errorMessage=thisErrorMessage, row.names = NULL, stringsAsFactors = FALSE)
      }
    }
  }
  # If we didn't match any runs, return an emply data frame
  if (!exists("runMeta")) {
    return(df)
  } else {
    # Order the run metadata by the specified column
    if (orderBy != "") {
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
#' #' @param runIds a list of execution identifiers
#' @param script name of script to match (can be a regex)
#' @param startTime match executions that started after this time (inclusive)
#' @param endTime match executions that ended before this time (inclusive)
#' @param tag text of tag to match (can be a regex)
#' @param errorMessage text of error message to match (can be a regex)
#' @param noop don't delete execution directories
#' @param quiet don't print any informational messages to the display
#' @author slaughter
#' @export
setGeneric("deleteRuns", function(recordr, ...) {
  standardGeneric("deleteRuns")
})

setMethod("deleteRuns", signature("Recordr"), function(recordr, runIds = "", script = "", startTime = "", endTime = "", tag = "", errorMessage = "", noop = FALSE, quiet = FALSE) {

  runs <- selectRuns(recordr, runIds=runIds, script=script, startTime=startTime, endTime=endTime, tag=tag, errorMessage=errorMessage, matchType="specific")
  if (nrow(runs) == 0) {
    if (!quiet) {
      cat(sprintf("No runs matched search criteria."))
    }
    return(runs)
  } else {
    if (!quiet) {
      if (noop) {
        cat(sprintf("The following %d runs would have been deleted:\n", nrow(runs)))
      } else {
        cat(sprintf("The following %d runs have been deleted:\n", nrow(runs)))
      }
    }
  }
  
  if (! quiet) {
    printRun(headerOnly=TRUE)
  }
  
  # Loop through selected runs
  for(i in 1:nrow(runs)) {
    row <- runs[i,]
    thisExecId <- row["executionId"]
    thisRunDir <- sprintf("%s/runs/%s", recordr@recordrDir, thisExecId)
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
  return(runs)
})
  
#' List all recorded runs
#' @param script name of script to match (can be a regex)
#' @param startTime match executions that started after this time (inclusive)
#' @param endTime match executions that ended before this time (inclusive)
#' @param tag text of tag to match (can be a regex)
#' @param errorMessage text of error message to match (can be a regex)
#' @param quiet don't print any informational messages to the display
## @returnType data frame containing the run information  
## 
## @author slaughter
#' @export
setGeneric("listRuns", function(recordr, ...) {
  standardGeneric("listRuns")
})

setMethod("listRuns", signature("Recordr"), function(recordr, script="", startTime = "", endTime = "", tag = "", errorMessage = "", quiet=FALSE, orderBy = "startTime") {

  runs <- selectRuns(recordr, script=script, startTime=startTime, endTime=endTime, tag=tag, errorMessage=errorMessage, matchType="non-specific", orderBy=orderBy)

  if (nrow(runs) == 0) {
    if (!quiet) {
      cat(sprintf("No runs matched search criteria."))
    }
    return(runs)
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
  
  return(runs)
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
  fmt <- paste("%-", sprintf("%2d", scriptNameLength), "s", 
               " %-", sprintf("%2d", tagLength), "s",
               " %-19s %-19s %-36s %-19s",
               " %-", sprintf("%2d", errorMsgLength), "s", "\n", sep="")
  
  if (headerOnly) {
    cat(sprintf(fmt, "Script", "Tag", "Start Time", "End Time", "Run Identifier", "Published Time", "Error Message"), sep = " ")
  } else {
    thisScript       <- row["script"]
    thisStartTime    <- row["startTime"]
    thisEndTime      <- row["endTime"]
    thisExecId       <- row["executionId"]
    #thisPackageId    <- row["datapackageId"]
    thisPublishTime  <- row["publishTime"]
    thisErrorMessage <- row["errorMessage"]
    thisTag         <- row["tag"]
    cat(sprintf(fmt, strtrim(thisScript, scriptNameLength), strtrim(thisTag, tagLength), thisStartTime, 
                thisEndTime, thisExecId, thisPublishTime, strtrim(thisErrorMessage, errorMsgLength)), sep = " ")
  }
}

#' View detailed information for an execution
#' @description Detailed information for an execution and any packags created by that exectution are printed to the display.
#' @param identifier of the data package
## @returnType DataPackage  
## 
#' @author slaughter
#' @export
setGeneric("view", function(recordr, id) {
  standardGeneric("view")
})
# 
setMethod("view", signature("Recordr"), function(recordr, id) {
  cat(sprintf("Information for execution: %s\n", id))
  
  # Find the data package in the recordr run directories
  #dirs <- list.files(recordr@runDir)
  thisRunDir <- sprintf("%s/runs/%s", recordr@recordrDir, id)
  if (! file.exists(thisRunDir)) {
    msg <- sprintf("Directory not found for execution identifier: %s", id)
    stop(msg)
  }
  
  execMeta <- readExecMeta(recordr, id)
  if (! is.null(execMeta)) {
    packageId <-  execMeta@datapackageId
    cat(sprintf("Package identifier: %s\n", packageId))
    packageFile <- sprintf("%s/%s.pkg", thisRunDir, packageId)
    # Deserialize saved data package
    pkg <- readRDS(file=packageFile)
    relations <- getRelationships(pkg)
    print(relations)
    
    fileNameLength = 30
    # "%-30s %-10d %-19s\n"
    fmt <- paste("%-", sprintf("%2d", fileNameLength), "s", 
                 " %-12s %-19s\n", sep="")
    cat(sprintf(fmt, "\nFilename", "Size (kb)", "Modified time"), sep = " ")
    infoFile <- sprintf("%s/fileInfo.csv", thisRunDir)
    fstats <- read.csv(infoFile, stringsAsFactors=FALSE, row.names = 1)
    # Order the list of files by file most recently modified
    # Note: we could also sort by basename of the file: fstats[order(basename(rownames(fstats))),]
    fstats <- fstats[order(fstats$mtime),]
    # Print out file information
    for (i in 1:nrow(fstats)) {
      cat(sprintf(fmt, strtrim(basename(rownames(fstats)[i]), fileNameLength), fstats[i, "size"], fstats[i, "mtime"]), sep = "")
    }
  } else {
    msg <- sprintf("Unable to read execution metadata from working directory: %s", thisRunDir)
    stop(msg)
  }
  # Return the data package (not implemented yet)
})

## @param identifier The node identifier with which this node is registered in DataONE
## @returnType DataPackage  
## @return the DataPackage object containing all provenance relationships and derived data for this run
## 
## @author slaughter
## @export
setGeneric("publish", function(recordr, packageId, MNode) {
  standardGeneric("publish")
})

