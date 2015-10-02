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
#' Capture, review and publish data provenance
#' @description The \emph{Recordr} class provides methods to record, search, review and publish data provenance about
#' R script executions. Information about files read and written by a script and the execution environment 
#' can be captured for each script execution. Script executions can then be reviewed and selected to be published to
#' the DataONE data repository, by retrieving archived copies of the R script, the files read and written by 
#' a script and a description of the provenance relationships between objects in the run, which are then combined into a
#' package and uploaded to the requested member node.
#' @rdname Recordr-class
#' @aliases Recordr-class
#' @slot recordrDir value of type \code{"character"} containing a path to the Recordr working directory
#' @slot dbConn A value of type \code{"SQLiteConnection"} that contains the connection of the recordr database
#' @slot dbFile A valof of type \code{"character"} that contains the location of the recordr database file
#' @rdname Recordr-class
#' @import dataone
#' @import datapackage
#' @import uuid
#' @import tools
#' @import digest
#' @import XML
#' @import EML
#' @import RSQLite
#' @include Constants.R
#' @section Methods:
#' \itemize{
#'  \item{\code{\link[=initialize-Recordr]{initialize}}}{: Initialize a Recordr object}
#'  \item{\code{\link{startRecord}}}{: Begin recording provenance for an R session}
#'  \item{\code{\link{endRecord}}}{: Get the Identifiers of Package Members}
#'  \item{\code{\link{record}}}{: Get the data content of a specified data object}
#'  \item{\code{\link{listRuns}}}{: Add a DataObject to the DataPackage}
#'  \item{\code{\link{viewRuns}}}{: Record relationships of objects in a DataPackage}
#'  \item{\code{\link{deleteRuns}}}{: Record derivation relationships between objects in a DataPackage}
#'  \item{\code{\link{publishRun}}}{: Retrieve relationships of package objects}
#' }
#' @seealso \code{\link{recordr}}{ package description.}
#' @export
setClass("Recordr", slots = c(recordrDir = "character",
                              dbConn = "SQLiteConnection",
                              dbFile = "character"))

#' Initialize a Recorder object
#' @rdname initialize-Recordr
#' @aliases initialize-Recordr
#' @param .Object The Recordr object
#' @param recordrDir The directory to store provenance data in.
#' @seealso \code{\link[=Recordr-class]{Recordr}} { class description}
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
#' session can be closed by calling the endRecord() method. When the record() function is called to record
#' a script, the startRecord() function is called automatically.
#' @param recordr a Recordr instance
#' @param tag a string that is associated with this run
#' @param .file the filename for the script to run (only used internally when startRecord() is called from record())
#' @param .console a logical argument that is used internally by the recordr package
#' @param config A \code{\link[dataone]{SessionConfig-class}} objec
#' @import dataone
#' @seealso \code{\link[=Recordr-class]{Recordr}} { class description}
#' @export
setGeneric("startRecord", function(recordr, ...) {
  standardGeneric("startRecord")
})

#' @describeIn startRecord
#' @return execution identifier that uniquely identifies this recorded session
#' @examples 
#' \dontrun{
#' rc <- new("Recordr")
#' startRecord(rc, tag="my first console run")
#' x <- read.csv(file="./test.csv")
#' runIdentifier <- endRecord(rc)
#' }
setMethod("startRecord", signature("Recordr"), function(recordr, tag="", .file=as.character(NA), .console=TRUE, configFile=as.character(NA)) {
  
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
  # If the user specified a configuration session instance, use it, otherwise use
  # the default configuration session.
  if(!is.na(configFile)) {
    config <- new("SessionConfig")
    loadConfig(configFile)
  } else {
    # If the user didn't specify a config file, then try to load the default file,
    # otherwise copy an initial file (from dataone package) to the default file location
    # and load that.
    config <- new("SessionConfig")
    loadConfig(config)
  }
  # Save the session configuration object to the recordr environment so it will be available to all methods
  recordrEnv$config <- config
  
  # Put a copy of the recordr object itself info the recordr environment, in case
  # to the overriding functions need any information that it contains, such as the db connection
  # Note: The recordr object contains the db connection that has to be open in the Recordr class
  # initialization because it also needs to be available to functions like listRuns() that
  # operate outside the startRecord -> endRecord execution.
  recordrEnv$recordr <- recordr

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

  recordrEnv$execMeta <- new("ExecMetadata", recordrEnv$scriptPath, tag=tag)
  recordrEnv$execMeta@console <- .console
  # Write out execution metadata now, and update the metadata during endRecord()
  # and possibly publish().
  writeExecMeta(recordr, recordrEnv$execMeta)
  
  # TODO: handle concurrent executions of recordr, if ever necessary. Currently recordr
  # doesn't allow concurrent record() sessions to run, as only one copy of the 
  # .recordr environment can exist, which startRecord() checks for.
  #
  # Create an empty D1 datapackage object and make it globally avilable, i.e. available
  # to the masking functions, e.g. "recordr_write.csv".
  recordrEnv$mnNodeId <- getConfig(recordrEnv$config, "target_member_node_id")
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
  recordrEnv$get <- recordr::recordr_getD1Object
  #recordrEnv$createD1Object <- recordr::recordr_createD1Object
  # override DataONE v2.0 methods
  recordrEnv$get <- recordr::recordr_D1MNodeGet
  # override R functions
  recordrEnv$read.csv <- recordr::recordr_read.csv
  recordrEnv$write.csv <- recordr::recordr_write.csv
  recordrEnv$ggsave <- recordr::recordr_ggsave
  
  # Create the run metadata directory for this record()
  dir.create(sprintf("%s/runs/%s", recordr@recordrDir, recordrEnv$execMeta@executionId), recursive = TRUE)
  # Put recordr working directory in so masked functions can access it. No information can be saved locally until this
  # variable is defined.
  recordrEnv$recordrDir <- recordr@recordrDir
  #cat(sprintf("filePath: %s\n", recordrEnv$scriptPath))
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
#' @description The recordring session started by the \code{startRecord()} method is
#' terminated and all provenance collecting is discontinued. A log of all the
#' console commands is saved. 
#' @param recordr A Recordr instance
#' @seealso \code{\link[=Recordr-class]{Recordr}} { class description}
#' @export
setGeneric("endRecord", function(recordr) {
  standardGeneric("endRecord")
})

#' @describeIn endRecord
#' @return id The execution identifier that uniquely identifiers this execution.
#' @examples 
#' \dontrun{
#' rc <- new("Recordr")
#' startRecord(rc, tag="my first console run")
#' x <- read.csv(file="./test.csv")
#' runIdentifier <- endRecord(rc)
#' }
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
  
  # Archive the script that was executed, or the console log if we
  # were recording console commands. The script can be retrieved
  # by searching for access="execute"
  archivedFilePath <- archiveFile(file=recordrEnv$scriptPath)
  filemeta <- new("FileMetadata", file=recordrEnv$scriptPath, 
               fileId=recordrEnv$programId, 
               executionId=recordrEnv$execMeta@executionId,
               access="execute", format="text/plain",
               archivedFilePath=archivedFilePath)
  writeFileMeta(recordr, filemeta)
  
  # Save the executed file or console log to the data package
  script <- charToRaw(paste(readLines(recordrEnv$scriptPath), collapse = '\n'))
  # Create a data package object for the program that we are running and store it.
  scriptFmt <- "text/plain"
  programD1Obj <- new("DataObject", id=recordrEnv$programId, dataobj=script, format=scriptFmt, user=recordrEnv$execMeta@user, mnNodeId=recordrEnv$mnNodeId)    
  # TODO: Set access control on the action object to be public
  addData(recordrEnv$dataPkg, programD1Obj)
  # Serialize/Save the entire package object to the run directory
  
  # Check if the metadata template file exists, if no, then copy the initial version
  # to the user's .recordr directory.
  metadataTemplateFile <- getConfig(recordrEnv$config, "package_metadata_template_path")
  if(!file.exists(metadataTemplateFile)) {
      file.copy(system.file("extdata/package_metadata_template.R", package="recordr"), metadataTemplateFile)
      message(sprintf("An initial package metadata template file has been copied to \"%s\"", metadataTemplateFile))
      message("Please review the \"dataone\" package documentation section 'Configuring dataone'")
      message("and then edit the configuration file with values appropriate for your installation.")
      setConfig(recordrEnv$config, "package_metadata_template_path", metadataTemplateFile)
      saveConfig(recordrEnv$config)
  }
    
  #mdfile <- sprintf("%s/metadata.R", path.expand("~"), recordr@recordrDir)
  success <- source(metadataTemplateFile, local=TRUE)
  # Set the identifier scheme to "uuid" for now, the user might specify "doi" during the
  # publish step.
  system <- "uuid"
  eml <- makeEML(recordr, id=recordrEnv$execMeta@executionId, system, title, creators, abstract, 
                 methodDescription, geo_coverage, temp_coverage)
  eml_xml <- as(eml, "XMLInternalElementNode")
  #print(eml_xml)
  # Write the eml file to the execution directory
  eml_file <- sprintf("%s/%s.xml", runDir, recordrEnv$execMeta@metadataId)
  saveXML(eml_xml, file = eml_file)
  #message(sprintf("Saved EML to file: %s\n", eml_file))
  metaObj <- new("DataObject", id=recordrEnv$execMeta@metadataId, format="eml://ecoinformatics.org/eml-2.1.1", 
                 filename=eml_file)
  addData(recordrEnv$dataPkg, metaObj)
  metaObjId <- getIdentifier(metaObj)
  # Now add the relationships between the science objects in the package and the metadata object
  # "metaObj documents sciObj"
  sciObjIds <- as.character(list())
  for (id in getIdentifiers(recordrEnv$dataPkg)) {
    if(id == metaObjId) next()
    sciObjIds <- c(sciObjIds, id)
  }
  
  if (length(sciObjIds) > 0) insertRelationship(recordrEnv$dataPkg, metaObjId, sciObjIds)
  # Save the package relationships to disk so that we can recreate this package
  # at a later date.
  provRels <- getRelationships(recordrEnv$dataPkg)
  filePath <- sprintf("%s/%s.csv", runDir, recordrEnv$execMeta@datapackageId)
  write.csv(provRels, file=filePath, row.names=FALSE)
  # Use the datapackage id as the resourceMap id
  #serializationId = recordrEnv$execMeta@datapackageId
  #filePath <- sprintf("%s/%s.rdf", runDir, serializationId)
  #status <- serializePackage(recordrEnv$dataPkg, file=filePath, id=serializationId)
  #filePath <- sprintf("%s/%s.pkg", runDir, recordrEnv$execMeta@datapackageId)
  #saveRDS(recordrEnv$dataPkg, file=filePath)
  #dataPkg <- recordrEnv$dataPkg
  
  # Save execution metadata to a file in the run directory
  updateExecMeta(recordr, executionId=recordrEnv$execMeta@executionId, endTime=recordrEnv$execMeta@endTime, errorMessage=recordrEnv$execMeta@errorMessage)
  # Don't print this object if startRecord()/endRecord() was called
  if(recordrEnv$execMeta@console) {
    base::invisible(recordrEnv$execMeta@executionId)
  } else {
    # Return the regular object if we are returning to record()
    return(recordrEnv$execMeta@executionId)
  }
})

#' Record data provenance for an R script exection
#' @description The R script is executed and information about file reads and writes
#' is recorded.
#' @details Input files, the script itself and igenerated files are archived.
#' Information about the execution environment is also saved.
#' @param recordr a Recordr instance
#' @param file The name of the R script to run and collect provenance information for
#' @param tag A string that will be associated with this run
#' @param config A \code{\link[dataone]{SessionConfig-class}} object
#' @param ... additional parameters that will be passed to the R \code{"base::source()"} function
#' @seealso \code{\link[=Recordr-class]{Recordr}} { class description}
#' @export
setGeneric("record", function(recordr, file, ...) {
  standardGeneric("record")
})

#' @describeIn record 
#' @return The execution identifier for this run
#' @examples
#' \dontrun{
#' rc <- new("Recordr")
#' executionId <- record(rc, file="myscript.R", tag="first run of myscript.R")
#' }
setMethod("record", signature("Recordr"), function(recordr, file, tag="", configFile=as.character(NA), ...) {
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
  
  execId <- startRecord(recordr, tag, .file=file, .console=FALSE, configFile=configFile)
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
    endRecord(recordr)
    if (is.element(".recordr", base::search())) {
      detach(".recordr", unload=TRUE)
    }
#     if (is.element(".recordrConfig", base::search())) {
#       detach(".recordrConfig", unload=TRUE)
#     }
    # return the execution identifier
    return(execId)
  })
})

#' Select runs that match search parameters
#' @description This method is used to retrieve execution metadata for 
#' runs that match the search parameters.
#' @details This method is used internally by the \emph{recordr} package.
#' @param recordr A Recordr instance
#' @param runId An execution identifiers
#' @param script The flle name of script to match.
#' @param startTime Match executions that started after this time (inclusive)
#' @param endTime Match executions that ended before this time (inclusive)
#' @param tag The text of tag to match.
#' @param errorMessage The text of error message to match.
#' @param seq The run sequence number
#' @param orderBy The column that will be used to sort the output. This can include a minus sign before the name, e.g. -startTime
#' @seealso \code{\link[=Recordr-class]{Recordr}} { class description}
setGeneric("selectRuns", function(recordr, ...) {
  standardGeneric("selectRuns")
})

#' @describeIn selectRuns
#' @return A data.frame that contains execution metadata for executions that matched the search criteria
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
#' @description  The execution metadata and all archived files associated with
#' each matching run are permanently delete from the file system. No backup is maintained
#' by the recordr package, so this deletion is irreversible, unless the user
#' maintains their own backup.
#' @param recordr A Recordr instance
#' @param id An execution identifier
#' @param file The name of script to match.
#' @param start A one or two element character list specifying a date range to match for run start time
#' @param end A one or tow element character list specifying a date range to match for run end time
#' @param tag The text of the tags to match.
#' @param error The text of the error message to match.
#' @param seq The run sequence number (can be a single value or a range, e.g \code{seq="1:10"})
#' @param noop Don't delete any date, just show what would be deleted.
#' @param quiet Don't print any informational messages to the display
#' @seealso \code{\link[=Recordr-class]{Recordr}} { class description}
#' @export
setGeneric("deleteRuns", function(recordr, ...) {
  standardGeneric("deleteRuns")
})

#' @describeIn deleteRuns
#' @return A data.frame containing execution metadata for the runs that were deleted.
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
      # TODO: delete all files associated with this run from the file archive
      # TODO: delete run metadata using readExecMeta
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
#' @details The \code{"start"} and \code{"end"} parameters can be used to specify a time
#' range to find runs that started execution and ended in the specified time range. For examples, specifying
#' \code{"start=c("2015-01-01, "2015-01-31)} will cause the search to return any execution with a starting
#' time in the first month of 2015. 
#' @param recordr A Recordr instance
#' @param file The name of the script to match 
#' @param start Match runs that started in this time range (inclusive)
#' Times must be entered in the form 'YYYY-MM-DD HH:MM:SS' but can be shortened to not less that "YYYY"
#' @param end A character value runs that ended in this time range (inclusive)
#' Times must be entered in the form 'YYYY-MM-DD HH:MM:SS' but can be shortened to not less that "YYYY"
#' @param tag Text of tag to match
#' @param error Text of error message to match 
#' @param seq A run sequence number (can be a range, e.g \code{seq=1:10})
#' @param quiet Don't print any informational messages to the display
#' @param orderBy The column that will be used to sort the output. This can include a minus sign before the name, e.g. -startTime
#' @seealso \code{\link[=Recordr-class]{Recordr}} { class description}
#' @export
setGeneric("listRuns", function(recordr, ...) {
  standardGeneric("listRuns")
})

#' @describeIn listRuns
#' @return data frame containing information for each run
#' @examples 
#' \dontrun {
#'   rc <- new("Recordr")
#'   # List runs that started in January 2015
#'   listRuns(rc, start=c("2015-01-01", "2015-01-31))
#'   # List runs that started on or after March 1, 2014
#'   listruns(rc, start="2014-03-01"
#'   # List runs that contain a tag with the string "analysis v1.3")
#'   listRuns(rc, tag="analysis v1.3")
#' }
#
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
               " %-19s %-19s %-13s %-19s",
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
    thisRunId       <- sprintf("...%s", substring(thisRunId, nchar(thisRunId)-9, nchar(thisRunId)))
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
#' @description Detailed information for an execution is printed to the display.
#' @param recordr A Recordr instance
#' @export
#' @seealso \code{\link[=Recordr-class]{Recordr}} { class description}
setGeneric("viewRuns", function(recordr, ...) {
  standardGeneric("viewRuns")
})

#' @describeIn viewRuns 
#' @param id The execution identifier of a run to view
#' @param file The name of script to match 
#' @param start Match runs that started in this time range (inclusive)
#' Times must be entered in the form 'YYYY-MM-DD HH:MM:SS' but can be shortened to not less that "YYYY"
#' @param end Match runs that ended in this time range (inclusive)
#' Times must be entered in the form 'YYYY-MM-DD HH:MM:SS' but can be shortened to not less that "YYYY"
#' @param tag The text of tag to match 
#' @param error The text of error message to match. 
#' @param seq A run sequence number (can be a range, e.g \code{seq=1:10})
#' @param orderBy Sort the results according to the specified column. A hypen ('-') prepended to the column name 
#' denoes a descending sort. The default value is "-startTime"
#' @param sections Print the specified sections of the output. Default=c("details", "used", "generated")
#' @param verbose
#' @param page A logical value - if TRUE then pause after each run is displayed.
#' @examples
#' \dontrun{
#' rc <- new("Recordr")
#' # View the tenth run that was recorded
#' viewRuns(rc, seq=10)
#' # View the first ten runs, with only the files "generated" section displayed
#' viewRuns(rc, seq="1:10", sections="generated")
#' }
setMethod("viewRuns", signature("Recordr"), function(recordr, id=as.character(NA), file=as.character(NA), start=as.character(NA), end=as.character(NA), tag=as.character(NA), error=as.character(NA),
                                                 seq=as.character(NA), orderBy="-startTime", sections=c("details","used","generated"), verbose=FALSE, page=TRUE, quiet=TRUE) {
  
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
    metadataId          <- thisRow[["metadataId"]]
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
    #packageFile <- sprintf("%s/%s.pkg", thisRunDir, thisRow[["datapackageId"]])
    # Deserialize saved data package
    #pkg <- readRDS(file=packageFile)
    #relations <- getRelationships(pkg)
    relations <- read.csv(sprintf("%s/%s.csv", thisRunDir, datapackageId), stringsAsFactors=FALSE)
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
        publishViewURL <- sprintf("%s/%s", D1_View_URL, publishId)
      }
      
      cat(sprintf("Publish date: %s\n", publishTime))
      cat(sprintf("Published to: %s\n", publishNodeId))
      cat(sprintf("Published Id: %s\n", publishId))
      cat(sprintf("View at: %s\n", publishViewURL))
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
      fstatsRead <- readFileMeta(recordr, executionId=executionId, access="read")
      #fstatsRead <- fstats[order(basename(fstatsRead$filePath)),]
      fstatsWrite <- readFileMeta(recordr, executionId=executionId, access="write")
      #fstatsWrite <- fstats[order(basename(fstatsWrite$filePath)),]
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
          cat(sprintf(fmt, strtrim(basename(fstatsRead[i, "filePath"]), fileNameLength), fstatsRead[i, "size"], fstatsRead[i, "modifyTime"]), sep = "")
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
          cat(sprintf(fmt, strtrim(basename(fstatsWrite[i, "filePath"]), fileNameLength), fstatsWrite[i, "size"], fstatsWrite[i, "modifyTime"]), sep = "")
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
    if(verbose) {
      cat(sprintf("\nProvenance relationships:\n"))
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
})

#' Publish a recordr'd execution to DataONE
#' @param recordr a Recordr instance
#' seealso \code{\link[=Recordr-class]{Recordr}} { class description}
#' @export
setGeneric("publishRun", function(recordr, ...) {
  standardGeneric("publishRun")
})

#' @describeIn publishRun
#' @import EML
#' @param id the run identifier for the execution to upload to DataONE
#' @param seq The sequence number for the execution to upload to DataONE
#' @param assignDOI a boolean value: if TRUE, assign DOI values for system metadata, otherwise assign uuid values
#' @param update a boolean value: if TRUE, republish a previously published execution
#' @param quiet A boolean value: if TRUE, informational messages are not printed (default=TRUE)
#' @return The published identifier of the uploaded package
setMethod("publishRun", signature("Recordr"), function(recordr, id=as.character(NA), 
                                                       seq=as.character(NA), 
                                                       assignDOI=FALSE, update=FALSE, quiet=TRUE) {
  
  if(is.na(id) && is.na(seq) ||
     !is.na(id) && !is.na(seq)) {
    stop("Please specify either \"seq\" or \"id\" parameter")
  }
  
  if(!is.na(seq)) {
    thisExecMeta <- readExecMeta(recordr, seq=seq)
  } else if (!is.na(id)) {
    thisExecMeta <- readExecMeta(recordr, id=id)
  }
  
  if(nrow(thisExecMeta) == 0) {
      stop(sprintf("No exeuction found\n"))
  }
  if(is.na(id)) id <- thisExecMeta[['executionId']]
 
  runDir <- sprintf("%s/runs/%s", recordr@recordrDir, id)
  if (! file.exists(runDir)) {
    msg <- sprintf("A directory was not found for execution identifier: %s\n", id)
    stop(msg)
  }
  # See if this execution has been published before
  if (!is.na(thisExecMeta[['publishTime']])) {
    msg <- sprintf("The datapackage for this execution was published on %s\n", thisExecMeta[['publishTime']])
    stop(msg)
  }
  
  # Get configuration parameters from the dataone SessionConfig
  sc <- new("SessionConfig")
  loadConfig(sc)
  public <- getConfig(sc, "public_read_allowed")
  subjectDN <- getConfig(sc, "subject_dn")
  replicationAllowed <- getConfig(sc, "replication_allowed") 
  numberOfReplicas <- getConfig(sc, "number_of_replicas")
  preferredNodes <- getConfig(sc, "preferred_replica_node_list")
  mnId <- getConfig(sc, "target_member_node_id")
  d1Env <- getConfig(sc, "dataone_env")
  if (!quiet) cat(sprintf("Publishing execution %s to %s\n", id, mnId))
  cm <- CertificateManager()
  isExpired <- isCertExpired(cm)
  publishTime <- format(Sys.time(), format="%Y-%m-%d")
  
  # Stop if the DataONE certificate is expired
  if(isExpired) {
    stop("Please create a valid DataONE certificate before calling publish()")
  }
   
  if(is.null(subjectDN) || is.na(subjectDN)) {
    subject <- showClientSubject(cm)
  } else {
    subject <- subjectDN
  }
  # Read the session configuration value for the DataONE environment to use,
  # e.g. "PROD" for production, "STAGING", "SANDBOX", "DEV"
  if(!quiet) cat(sprintf("Contacting coordinating node for environment %s...\n", d1Env))
  cn <- CNode(d1Env)
  # message(sprintf("Obtaining member node information for %s", mnId))
  if(!quiet) cat(sprintf("Getting member node url for memober node id: %s...\n", mnId))
  mn <- getMNode(cn, mnId)
  if (is.null(mn)) {
    stop(sprintf("Member node %s encounted an error on the get() request", mnId))
  }

  packageId <- thisExecMeta[['datapackageId']]
  pkg <- new("DataPackage", packageId=packageId)
  
  # TODO: if the user requests DOI, fix the metadata entry in the EML
  # to denote this.
 # Generate a unique identifier for the metadatobject
#   if (assignDOI) {
#     metadataId <- generateIdentifier(mn, "DOI")
#     # TODO: check if we actually got one, if not then error
#     system <- "doi"
#   } else {
#     metadataId <- paste0("urn:uuid:", UUIDgenerate())
#     system <- "uuid"
#   }
#   

  # Retrieve metadata that was created for this run and create a DataObject with it.
  # TODO: use the in-memory object when datapackage can upload it
  #metadata <- getMetadata(recordr, id=id)
  metadataId <- thisExecMeta[["metadataId"]]
  # TODO: use getMetadata() output when eml_read accepts
  # XMLInternalDocument, as the documentation says it should
  #metadata <- getMetadata(recordr, id=id, as="parsed")
  metadataFile <- sprintf("%s/%s.xml", runDir, metadataId)
  emlObj <- eml_read(metadataFile)
  
  # Upload each data object that was added to the datapackage
  if(!quiet) cat(sprintf("Getting file info for execution %s\n", id))
  files <- readFileMeta(recordr, executionId=id)
  for (iRow in 1:nrow(files)) {
    thisFile <- files[iRow,]
    format <- thisFile[['format']]
    fileId <- thisFile[['fileId']]
    filePath <- sprintf("%s/%s", recordr@recordrDir, thisFile[['archivedFilePath']])
    # Create DataObject for the science dataone
    sciObj <- new("DataObject", id=fileId, format=format, user=subject, mnNodeId=mnId, filename=filePath)
    if (public) sciObj <- setPublicAccess(sciObj)
    # During endRecord(), each science object was associated with the metadata object
    # via insertRelationship() with the 'documetns' relationship. These relationships
    # were stored with the rest of the package relationships, so we don't have to add them
    # in again.
    if(!quiet) cat(sprintf("Adding science object with id: %s\n", getIdentifier(sciObj)))
    addData(pkg, sciObj)
    # Now update the metadata object corresponding to this dataset in order to set the
    # Online distribution value so that MetacatUI can properly identify and display this item.
    # The 'additionalInfo' value was stored during endRecord() so that we could match up the
    # file and the eml element after the run was finished.
    for(iEntity in 1:length(emlObj@dataset@otherEntity)) {
      thisDatasetId <- emlObj@dataset@otherEntity[[iEntity]]@additionalInfo
      #thisDatasetId <- emlObj@dataset@otherEntity[[iEntity]]@alternateIdentifier
      if(fileId == thisDatasetId) {
        url <- sprintf("%s/object/%s", mn@endpoint, fileId)
        emlObj@dataset@otherEntity[[iEntity]]
        emlObj@dataset@otherEntity[[iEntity]]@physical@distribution@online@url <- url
      }
    }
  }
  
  emlObj@dataset@pubDate <- publishTime
  # Update the metadata stored for this run. The putMetadata() function
  # can't read eml objects yet, so have to write it to a file.
  tempMetadataFile <- tempfile()
  eml_write(emlObj, tempMetadataFile)
  putMetadata(recordr, id=id, metadata=tempMetadataFile, asText=FALSE)
  metaObj <- new("DataObject", id=metadataId, format=EML_211_FORMAT, user=subject, mnNodeId=mnId, filename=metadataFile)
  addData(pkg, metaObj)
  
  #
  # Add the saved relationships back in the data package
  relationshipFile <- sprintf("%s/%s.csv", runDir, packageId)
  relationships <- read.csv(relationshipFile, stringsAsFactors=FALSE)
  for(i in 1:nrow(relationships)) {
    thisRelationship <- relationships[i,]
    thisSubject <- thisRelationship[["subject"]]
    thisPredicate <- thisRelationship[["predicate"]]
    thisObject <- thisRelationship[["object"]]
    thisSubjectType <- thisRelationship[["subjectType"]]
    thisObjectType <- thisRelationship[["objectType"]]
    thisDataTypeURI <- thisRelationship[["dataTypeURI"]]
    insertRelationship(pkg, thisSubject, thisObject, thisPredicate, thisSubjectType,
                       thisObjectType, thisDataTypeURI)
  }
  
  if(!quiet) cat(sprintf("Uploading data package..."))
  resourceMapId <- uploadDataPackage(mn, pkg, replicate=replicationAllowed, numberReplicas=numberOfReplicas, 
                                     preferredNodes=preferredNodes, public=public)
  # Use the metadata id as the 'published identifier' for this datapackage. This will be displayed in the 
  # viewRuns() output for this under "Published ID".
  if(!quiet) cat(sprintf("Uploaded data package with package id: %s", resourceMapId))
  # Record the time that this execution was published, the published id, subject that submitted the data.
  updateExecMeta(recordr, executionId=id, subject=subject, publishTime=publishTime, publishNodeId=mnId, 
                 publishId=metadataId) 
  invisible(metadataId)
})


#' Retrieve the metadata object for a run
#' @description When a script or console session is recorded (see record() and startrecord()), 
#' a metadata object is created that describes the objects associated with the run, using the
#' Ecological Metadata Language \link{https://knb.ecoinformatics.org/#external//emlparser/docs/index.html}.
#' This metadata can be retrieved from the recordr cache for review or editing if desired. If the metadata
#' is updated, it can be re-inserted into the recordr cache using the \code{putMetadata} method.
#' @param recordr a Recordr instance
#' seealso \code{\link[=Recordr-class]{Recordr}} { class description}
#' @export
setGeneric("getMetadata", function(recordr, ...) {
  standardGeneric("getMetadata")
})

#' @describeIn getMetadata
#' @param id The identifier for a run
#' @param seq The sequence number for a run
#' @param as Form to return the metadata as. Possible values are: "text", "parsed" (for parsed XML)
#' @return A character vector containing the metadata
setMethod("getMetadata", signature("Recordr"), function(recordr, id=as.character(NA), 
                                                       seq=as.character(NA), 
                                                       as=as.character("text")) {
  # User must specify "id" or "seq"
  if(is.na(id) && is.na(seq)) {
    stop("Please specify either \"id\" or \"seq\" parameter\n")
  }
  
  if(!is.na(seq)) {
    execMeta <- readExecMeta(recordr, seq=seq)
  } else if (!is.na(id)) {
    execMeta <- readExecMeta(recordr, executionId=id)
  }
  
  # Is this a vaiid id or seq?
  if(nrow(execMeta) == 0) {
      stop(sprintf("No exeuction found\n"))
  }
  
  # Locate the metadata file
  if(is.na(id)) id <- execMeta[['executionId']]
  runDir <- sprintf("%s/runs/%s", recordr@recordrDir, id)
  if (! file.exists(runDir)) {
    msg <- sprintf("A directory was not found for execution identifier: %s\n", id)
    stop(msg)
  }
  
  metadataId <- execMeta[['metadataId']]
  metadataFile <- sprintf("%s/%s.xml", runDir, metadataId)
  metadata <- readLines(metadataFile, warn=FALSE)
  
  if(as == "text") {
    return(metadata)
  } else if (as == "parsed") {
    return(xmlInternalTreeParse(metadata, asText=TRUE))
  }
})

#' Update the metadata object for a run
#' @description Put a metadata document into the recordr cache for an run, replacing the
#' existing metadata object for the specified run, if one exists.
#' @param recordr a Recordr instance
#' @seealso \code{\link[=Recordr-class]{Recordr}} { class description}
#' @export
setGeneric("putMetadata", function(recordr, ...) {
  standardGeneric("putMetadata")
})

#' @describeIn putMetadata
#' @details The \code{metadata} parameter can specify either a character vector that contains the metadata
#' this parameter can be a filename that contains the metadata. The \code{asText} parameter is used to
#' specify which type of value is specified. If \code{asText} is TRUE, then the \code{metadata} parameter
#' is a character vector, if it is FALSE, then the \code{metadata} parameter is a filename.
#' @param id The identifier for a run
#' @param seq The sequence number for a run
#' @param metadata The replacement metadata, as the actual text, or as a filename containing the metadata
#' @param asText A logical. See 'Details'.
#'  If TRUE, then the \code{metadata} parameter is a character vector containing metadata, ir FALSE it is a filename. The
#' default is TRUE.
#' @return A character vector containing the metadata
setMethod("putMetadata", signature("Recordr"), function(recordr, id=as.character(NA), 
                                                       seq=as.character(NA), metadata=as.character(NA),
                                                       asText=TRUE) {
  # User must specify "id" or "seq"
  if(is.na(id) && is.na(seq)) {
    stop("Please specify either \"id\" or \"seq\" parameter\n")
  }
  
  if(!is.na(seq)) {
    execMeta <- readExecMeta(recordr, seq=seq)
  } else if (!is.na(id)) {
    execMeta <- readExecMeta(recordr, executionId=id)
  }
  
  # Is this a vaiid id or seq?
  if(nrow(execMeta) == 0) {
      stop(sprintf("No exeuction found for the specified execution or sequence number\n"))
  }
  
  # Locate the metadata file
  if(is.na(id)) id <- execMeta[['executionId']]
  runDir <- sprintf("%s/runs/%s", recordr@recordrDir, id)
  if (! file.exists(runDir)) {
    msg <- sprintf("A directory was not found for execution identifier: %s\n", id)
    stop(msg)
  }
  
  metadataId <- execMeta[['metadataId']]
  metadataFile <- sprintf("%s/%s.xml", runDir, metadataId)
  metadataFileBackup <- sprintf("%s.bak", metadataFile)
  
  # Either writeout the metadata to the run directory, or copy the user file to that location
  if(asText) {
    # Save a backup copy first
    file.rename(metadataFile, metadataFileBackup)
    # Overwrite the existing metadata file
    writeLines(metadata, metadataFile)
  } else {
    if(!file.exists(metadata)) {
      stop(sprintf("Metadata file %s does not exists, unable to update run metadata\n", metadata))
    } else {
      file.rename(metadataFile, metadataFileBackup)
      file.copy(metadata, metadataFile)
    }
  }
})

recordrShutdown <- function() {
  
  if (is.element(".recordr", base::search())) {
    recordrEnv <- as.environment(".recordr")
    unloadConfig(recordrEnv$config)
    detach(".recordr")
  }
  #if (is.element(".recordrConfig", base::search())) detach(".recordrConfig")
}

#' Create a minimal EML document.
#' Creating EML should be more complete, but this minimal example will suffice to create a valid document.
makeEML <- function(recordr, id, system, title, creators, abstract=NA, methodDescription=NA, geo_coverage=NA, temp_coverage=NA, endpoint=NA) {
  #dt <- eml_dataTable(dat, description=description)
  oe_list <- as(list(), "ListOfotherEntity")
  execMeta <- readExecMeta(recordr, executionId=id)
  metadataId <- execMeta[['metadataId']]
  fileMeta <- readFileMeta(recordr, executionId=id)
  
  # Loop through each file for this run and add an EML "otherEntity"
  # entry for each output file and script that was run.
  for (fileNum in 1:nrow(fileMeta)) {
    thisFile <- fileMeta[fileNum,]
    fileId <- thisFile[["fileId"]]
    access <- thisFile[["access"]]
    # Skip this file if it was not run or generated by this execution. Recordr
    # also tracks input files, but those should not be included in the metadata
    # object as this execution did not create them.
    if(!is.element(access, c("write", "execute"))) next
    filePath <- thisFile[["filePath"]]
    format <- thisFile[["format"]]
    fileSize <- thisFile[["size"]]
    oe <- new("otherEntity", entityName=basename(filePath), entityType=format)
    oe@physical@objectName <- basename(filePath)
    oe@physical@size <- fileSize
    oe@entityName <- basename(filePath)
    # Store the unique identifier for this entity, so that we can find it and
    # update it later if necessary. Turns out that DataONE doesn't recognize
    # <otherEntity><alternateIdentifier as a valid element, so use 'additionalInfo
    # instead.
    oe@alternateIdentifier <- fileId
    #oe@additionalInfo <- fileId
    if (!is.na(endpoint)) {
      oe@physical@distribution@online@url <- paste(endpoint, id, sep="/")
    }
    if(!is.na(format)) {
      f <- new("externallyDefinedFormat", formatName=format)
      df <- new("dataFormat", externallyDefinedFormat=f)
      oe@physical@dataFormat <- df
    }
    oe_list <- c(oe_list, oe)
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
             packageId = metadataId,
             system = system,
             dataset = ds)
  return(eml)
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
  