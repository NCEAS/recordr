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
#   Unless required by  applicable law or agreed to in writing, software
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
#' @import datapackage
#' @import methods
#' @import uuid
#' @import tools
#' @import digest
#' @import EML
#' @import RSQLite
#' @import XML
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
setMethod("initialize", signature = "Recordr", 
          definition = function(.Object,
                                recordrDir = as.character(NA), quiet=FALSE) {
  # User didn't specify recordr dir, use ~/.recordr
  if (is.na(recordrDir)) {
    recordrDir <- sprintf("%s/.recordr", path.expand("~"))
  }
  # If recordrDir doesn't exist, create it
  if(!dir.exists(recordrDir)) {
    dir.create(recordrDir, recursive=TRUE)
  }
  .Object@recordrDir <- recordrDir
  # Open a connection to the database that contains execution metadata,
  .Object@dbFile <- sprintf("%s/recordr.sqlite", recordrDir)
  dbConn <- getDBconnection(dbFile=.Object@dbFile)
  if (is.null(dbConn)) {
    stop("Unable to create a Recordr object\n")
  } else {
    .Object@dbConn <- dbConn
  } 
  
  if (!dbIsValid(dbConn)) {
    if(is.null(dbConn)) {
      stop(sprintf("Error reconnecting to database file %s\n", recordr@dbFile))
    }
  }
  
  #dbVersion <- getRecordrDbVersion(.Object)
  # Get recordr version from admin table. 
  recordrVersion <- packageDescription(pkg="recordr")$Version
  verInfo <- unlist(strsplit(recordrVersion, "\\.", perl=TRUE))
  rcVersionMajor <- verInfo[[1]]
  rcVersionMinor <- verInfo[[2]]
  rcVersionPatch <- verInfo[[3]]
  # Is this a development version?
  if (length(verInfo) > 4) {
    rcVersionDev <- verInfo[[4]]
  } else {
    rcVersionDev <- as.character(NA)
  }
  
  # The 'admin' table doesn't exist, so create a new one with
  # the current database version.
  if (!is.element("admin", dbListTables(.Object@dbConn))) {
    createAdminTable(.Object, RECORDR_DB_VERSION)
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
#' @param ... additional parameters
#' @seealso \code{\link[=Recordr-class]{Recordr}} { class description}
#' @export
setGeneric("startRecord", function(recordr, ...) {
  standardGeneric("startRecord")
})

#' @rdname startRecord
#' @param tag a string that is associated with this run
#' @param .file the filename for the script to run (only used internally when startRecord() is called from record())
#' @param .console a logical argument that is used internally by the recordr package

#' @return execution identifier that uniquely identifies this recorded session
#' @examples 
#' \dontrun{
#' rc <- new("Recordr")
#' startRecord(rc, tag="my first console run")
#' x <- read.csv(file="./test.csv")
#' runIdentifier <- endRecord(rc)
#' }
setMethod("startRecord", signature("Recordr"), function(recordr, tag=list(), .file=as.character(NA), .console=TRUE) {
  
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

  recordrEnv$execMeta <- new("ExecMetadata", programName=recordrEnv$scriptPath, tag=tag)
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
  recordrEnv$mnNodeId <- getOption("target_member_node_id")
  recordrEnv$dataPkg <- new("DataPackage", packageId=recordrEnv$execMeta@datapackageId)
    
  # Add the ProvONE Execution type
  # Store the provONE relationship: execution -> prov:qualifiedAssociation -> association
  #associationId <- sprintf("urn:uuid:%s", UUIDgenerate())
  associationId <- "_:A0"
  insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=associationId, predicate=provQualifiedAssociation, objectTypes="blank")
  insertRelationship(recordrEnv$dataPkg, subjectID=associationId, objectIDs=recordrEnv$programId, predicate=provHadPlan, subjectType="blank")
  # Record relationship identifying this id as a provone:Execution
  insertRelationship(recordrEnv$dataPkg, subjectID=associationId, objectIDs=provAssociation, predicate=rdfType, subjectType="blank", objectType="uri")
  
  # Record a relationship identifying the program (script or console log)
  insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$programId, objectIDs=provONEprogram, predicate=rdfType, objectType="uri")
  
  # Associate a user identity as the agent executing this script.
  # If an ORCID has been specified, then that takes priority and we will use it.
  # If not ORCID has been specified, then use a FOAF:name if that is specified.
  # If neither of these is specified, then create a blank node specifying the username
  
  orcidIdentifier <- getOption("orcid_identifier")
  if(is.null(orcidIdentifier) || is.na(orcidIdentifier)) {
    orcidURL <- as.character(NA)
    orcidIdentifier <- as.character(NA)
  } else {
    orcidURI <- sprintf("https://%s", orcidIdentifier) 
  }
  
  foafName <- getOption("foaf_name")
  if(is.null(foafName) || is.na(foafName)) {
    foafName <- as.character(NA)
  } 
  
  foafAccount <- recordrEnv$execMeta@user
  # Store the Prov relationship: association -> prov:agent -> user
  # Create the 'user' object as an RDF blank node, so that we can describe a user without a URI. All types of user id info will
  # be stored this way: orcid, foaf:name, local compuassociationId ter account
  userBlankNodeId <- "_:U1"
  insertRelationship(recordrEnv$dataPkg, subjectID = userBlankNodeId, objectIDs = provONEuser, predicate = rdfType, subjectType = "blank", objectType="uri")
  insertRelationship(recordrEnv$dataPkg, subjectID = associationId, objectIDs=userBlankNodeId, predicate=provAgent, subjectType = "blank", objectType="blank")
  insertRelationship(recordrEnv$dataPkg, subjectID = recordrEnv$execMeta@executionId, objectIDs=userBlankNodeId, predicate=provWasAssociatedWith, objectType="blank")
  if(!is.na(orcidIdentifier)) {
    # TODO: properly type the orcid, i.e. the predicate in the following statement is not a type. It appears that there is
    # no ORCID ontology that defines the type for an ORCID
    #insertRelationship(recordrEnv$dataPkg, subjectID = userBlankNodeId, objectIDs = orcidURI, predicate = ORCID_NS, subjectType = "blank", objectType = "uri")
    insertRelationship(recordrEnv$dataPkg, subjectID = userBlankNodeId, objectIDs = orcidURI, predicate = ORCID_TYPE, subjectType = "blank", objectType = "uri")
    insertRelationship(recordrEnv$dataPkg, subjectID = orcidURI, objectID = ORCID_TYPE, predicate = rdfType)
  } else if (!is.na(foafName)) {
    insertRelationship(recordrEnv$dataPkg, subjectID = userBlankNodeId, objectIDs = foafName, predicate = FOAF_NAME, subjectType = "blank", objectType = "uri")
    insertRelationship(recordrEnv$dataPkg, subjectID = userBlankNodeId, objectIDs = FOAF_PERSON, predicate=rdfType, subjectType = "blank", objectType="uri")
    insertRelationship(recordrEnv$dataPkg, subjectID = userBlankNodeId, objectIDs = recordrEnv$execMeta@user, predicate=foafAccount, subjectType="blank", objectType="literal", dataTypeURI=xsdString)
  } else {
    # No other identification information available, so just use the username
    # Store the Prov relationship: association
    insertRelationship(recordrEnv$dataPkg, subjectID = userBlankNodeId, objectIDs=recordrEnv$execMeta@user, predicate=FOAF_ACCOUNT, subjectType="blank", objectType="literal", dataTypeURI=xsdString)
  } 
  
  # Override R functions
  #recordrEnv$source <- recordr::recordr_source
  
  #recordrEnv$createD1Object <- recordr::recordr_createD1Object
  # override DataONE v2.0 methods
  recordrEnv$getObject <- recordr::recordr_getObject
  recordrEnv$create <- recordr::recordr_create
  recordrEnv$updateObject <- recordr::recordr_updateObject
  # override R functions
  recordrEnv$read.csv <- recordr::recordr_read.csv
  recordrEnv$write.csv <- recordr::recordr_write.csv
  recordrEnv$ggsave <- recordr::recordr_ggsave
  recordrEnv$readLines <- recordr::recordr_readLines
  recordrEnv$writeLines <- recordr::recordr_writeLines
  recordrEnv$readPNG <- recordr::recordr_readPNG
  recordrEnv$writePNG <- recordr::recordr_writePNG
  recordrEnv$scan <- recordr::recordr_scan
  
  # Create the run metadata directory for this record()
  dir.create(sprintf("%s/runs/%s", recordr@recordrDir, recordrEnv$execMeta@executionId), recursive = TRUE)
  # Put recordr working directory in so masked functions can access it. No information can be saved locally until this
  # variable is defined.
  recordrEnv$recordrDir <- recordr@recordrDir
  #cat(sprintf("filePath: %s\n", recordrEnv$scriptPath))
  # Record relationship identifying this id as a provone:Execution
  insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=provONEexecution, predicate=rdfType, objectType="uri")
 
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
#' @description The recordring session started by the \code{startRecord()} method is
#' terminated and all provenance collecting is discontinued. A log of all the
#' console commands is saved. 
#' @param recordr A Recordr instance
#' @seealso \code{\link[=Recordr-class]{Recordr}} { class description}
#' @export
setGeneric("endRecord", function(recordr) {
  standardGeneric("endRecord")
})

#' @rdname endRecord
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
  runDir <- sprintf("%s/runs/%s", recordr@recordrDir, recordrEnv$execMeta@executionId)
  if (!file.exists(runDir)) {
      dir.create(runDir, recursive = TRUE)
  }
  
  # Disable provenance capture now that endRecord() has been called
  setProvCapture(FALSE)
  recordrEnv$execMeta@endTime <- as.character(format(Sys.time(), format="%Y-%m-%d %H:%M:%S %Z")) 
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
  fpInfo <- file.info(recordrEnv$scriptPath)
  filemeta <- new("FileMetadata", file=recordrEnv$scriptPath, 
               fileId=recordrEnv$programId, 
               sha256=digest(object=recordrEnv$scriptPath, algo="sha256", file=TRUE)[[1]],
               size=as.numeric(fpInfo[["size"]]),
               user=fpInfo[["uname"]],
               createTime=as.character(fpInfo[["ctime"]]),
               modifyTime=as.character(fpInfo[["mtime"]]),
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
  metadataTemplateFile <- getOption("package_metadata_template_path")
  if(is.null(metadataTemplateFile) || !file.exists(metadataTemplateFile)) {
      metadataTemplateFile <- "~/.recordr/package_metadata_template.R"
      # It is possible that the option wasn't set, but the default file already exists
      if(!file.exists(metadataTemplateFile)) {
        file.copy(system.file("extdata/package_metadata_template.R", package="recordr"), metadataTemplateFile)
        message(sprintf("An initial package metadata template file has been copied to \"%s\"", metadataTemplateFile))
        message("Please review the \"recordr\" package documentation section 'Configuring recordr'")
        message("and then set the options parameters with values appropriate for your installation.")
      }
  }
    
  # These variables should be defined by the metadata template, however, define initial values 
  # here in case they are not found in the template.
  creators <- list()
  abstract <- as.character(NA)
  methodDescription <- as.character(NA)
  geo_coverage <- geoCoverage("global", west="-180", east="180", north="90", south="-90")
  temp_coverage <- temporalCoverage(Sys.Date(), Sys.Date())
  #mdfile <- sprintf("%s/metadata.R", path.expand("~"), recordr@recordrDir)
  success <- source(metadataTemplateFile, local=TRUE)
  # Set the identifier scheme to "uuid" for now, the user might specify "doi" during the
  # publish step.
  system <- "uuid"
  eml <- makeEML(recordr, id=recordrEnv$execMeta@executionId, system, title, creators, abstract, 
                 methodDescription, geo_coverage, temp_coverage)
  # Write the eml file to the execution directory
  eml_file <- sprintf("%s/%s.xml", runDir, recordrEnv$execMeta@metadataId)
  write_eml(eml, file = eml_file)
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
#' @param ... additional parameters that will be passed to the R \code{"base::source()"} function
#' @seealso \code{\link[=Recordr-class]{Recordr}} { class description}
#' @export
setGeneric("record", function(recordr, file, ...) {
  standardGeneric("record")
})

#' @rdname record 
#' @return The execution identifier for this run
#' @examples
#' \dontrun{
#' rc <- new("Recordr")
#' executionId <- record(rc, file="myscript.R", tag="first run of myscript.R")
#' }
setMethod("record", signature("Recordr"), function(recordr, file, tag="", ...) {
  # Check if a recording session didn't clean up properly by removing the
  # temporary environments. If yes, then remove them now. The recordr pacakge
  # does not allow concurrent execution of two record() sessions.
  if ( is.element(".recordr", base::search())) {
    detach(".recordr")
  }

  if(!file.exists(file)) {
    stop(sprintf("Error, file \"%s\" does not exist\n", file))
  }
  
  execId <- startRecord(recordr, tag, .file=file, .console=FALSE)
  recordrEnv <- as.environment(".recordr")
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
    # return the execution identifier
    return(execId)
  })
})

#' Select runs that match search parameters
#' @description This method is used to retrieve execution metadata for 
#' runs that match the search parameters.
#' @details This method is used internally by the \emph{recordr} package.
#' @param recordr A Recordr instance
#' @param ... additional parameters
#' @seealso \code{\link[=Recordr-class]{Recordr}} { class description}
setGeneric("selectRuns", function(recordr, ...) {
  standardGeneric("selectRuns")
})

#' @rdname selectRuns
#' @param runId An execution identifiers
#' @param script The flle name of script to match.
#' @param startTime Match executions that started after this time (inclusive)
#' @param endTime Match executions that ended before this time (inclusive)
#' @param tag The text of tag to match.
#' @param errorMessage The text of error message to match.
#' @param seq The run sequence number
#' @param orderBy The column that will be used to sort the output. This can include a minus sign before the name, e.g. -startTime
#' @param delete A logical value, if TRUE then the selected runs are deleted from the Recordr database.
#' @return A data.frame that contains execution metadata for executions that matched the search criteria
setMethod("selectRuns", signature("Recordr"), function(recordr, runId=as.character(NA), script=as.character(NA), startTime=as.character(NA), endTime=as.character(NA), 
                                                       tag=as.character(NA), errorMessage=as.character(NA), seq=as.integer(NA), orderBy="-startTime", delete=FALSE) {
  
  colNames = c("script", "tag", "startTime", "endTime", "runId", "packageId", "publishTime", "errorMessage", "console", "seq")
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
  
  # Retrieve the execution metadata entries that match the search criteria
  execMeta <- readExecMeta(recordr, executionId=runId, script=script, 
    startTime=startTime, endTime=endTime,
    tag=tag, errorMessage=errorMessage,
    seq=seq, orderBy=orderBy, sortOrder=sortOrder, delete)
    
    return(execMeta)
})

#' @title Delete runs that match search parameters
#' @description The execution metadata and all archived files associated with
#' each matching run are permanently deleted from the file system. No backup is maintained
#' by the recordr package, so this deletion is irreversible, unless the user
#' maintains their own backup.
#' @param recordr A Recordr instance
#' @param ... additional arguments
#' @seealso \code{\link[=Recordr-class]{Recordr}}{ class description}
#' @export
setGeneric("deleteRuns", function(recordr, ...) {
  standardGeneric("deleteRuns")
})

#' @rdname deleteRuns
#' @param id An execution identifier
#' @param file The name of script to match.
#' @param start A one or two element character list specifying a date range to match for run start time
#' @param end A one or tow element character list specifying a date range to match for run end time
#' @param tag The text of the tags to match.
#' @param error The text of the error message to match.
#' @param seq The run sequence number (can be a single value or a range, e.g \code{seq="1:10"})
#' @param noop Don't delete any date, just show what would be deleted.
#' @return A data.frame containing execution metadata for the runs that were deleted.
setMethod("deleteRuns", signature("Recordr"), function(recordr, id = as.character(NA), file = as.character(NA), 
                                                       start = as.character(NA), end = as.character(NA), 
                                                       tag = as.character(NA), error = as.character(NA), 
                                                       seq = as.integer(NA), noop = FALSE) {

  runs <- selectRuns(recordr, runId=id, script=file, startTime=start, endTime=end, tag=tag, errorMessage=error, seq=seq, delete=TRUE)
  # selectRuns returns a list of ExecMeta objects
  if (length(runs) == 0) {
    message(sprintf("No runs matched search criteria."))
    return(invisible(runs))
  } else {
    if (noop) {
      message(sprintf("The following %d runs would have been deleted:\n", length(runs)))
    } else {
      message(sprintf("The following %d runs will be deleted:\n", length(runs)))
    }
  }
  
  printRun(headerOnly=TRUE)
  
  # Loop through selected runs
  for(i in 1:length(runs)) {
    row <- runs[[i]]
    thisRunId <- row@executionId
    thisRunDir <- sprintf("%s/runs/%s", recordr@recordrDir, thisRunId)
    if (!noop) {
      if(thisRunDir == sprintf("%s/runs", recordr@recordrDir) || thisRunDir == "") {
        stop(sprintf("Error determining directory to remove, directory: %s", thisRunDir))
      }
      unlink(thisRunDir, recursive = TRUE)
      # Delete all file access entries for this execution, for any type of access,
      # i.e. "read", "write", "execute"
      # The file info for the deleted entries is returned
      fstatsAll <- readFileMeta(recordr, executionId=thisRunId)
      # Loop through the deleted file entries and unarchive any file associated with
      # this run, i.e. files read, wriiten, executed, ect.
      if(nrow(fstatsAll) > 0) {
        for (ifile in 1:nrow(fstatsAll)) {
          thisFileId <- fstatsAll[ifile, 'fileId']
          # First delete the file in the archive, if no other executions are refering to it.
          unArchiveFile(recordr, thisFileId)
          # Then delete the database entry for it.
          fdel <- readFileMeta(recordr, fileId=thisFileId, delete=TRUE)
        }
      }
    }
    printRun(row)
  }
  
  invisible(execMetaTodata.frame(runs))
})
  
#' List all runs recorded by record() or startRecord()
#' @description If no search terms are specified, then all runs are listed. The
#' method arguments are search terms that limit the runs listed, with anly runs listed that
#' match all arguments. 
#' @details The \code{"start"} and \code{"end"} parameters can be used to specify a time
#' range to find runs that started execution and ended in the specified time range. For examples, specifying
#' \code{"start=c("2015-01-01, "2015-01-31")} will cause the search to return any execution with a starting
#' time in the first month of 2015. 
#' @param recordr A Recordr instance
#' @param ... additional parameters
#' @seealso \code{\link[=Recordr-class]{Recordr}}{ class description}
#' @export
setGeneric("listRuns", function(recordr, ...) {
  standardGeneric("listRuns")
})

#' @rdname listRuns
#' @param id a \code{"character"}, the identifier to match
#' @param script  \code{"character"},the name of the script to match 
#' @param start \code{"character"}, Match runs that started in this time range (inclusive)
#' Times must be entered in the form 'YYYY-MM-DD HH:MM:SS' but can be shortened to not less that "YYYY"
#' @param end a \code{"character"}, Match runs that ended in this time range (inclusive)
#' Times must be entered in the form 'YYYY-MM-DD HH:MM:SS' but can be shortened to not less that "YYYY"
#' @param tag \code{"character"} Text of tag to match
#' @param error \code{"character"} Text of error message to match 
#' @param seq \code{"integer"} A run sequence number (can be a range, e.g \code{seq=1:10})
#' @param orderBy The column that will be used to sort the output. This can include a minus sign before the name, e.g. -startTime
#' @return data frame containing information for each run
#' @examples \dontrun {
#' rc <- new("Recordr")
#' # List runs that started in January 2015
#' listRuns(rc, start=c("2015-01-01", "2015-01-31))
#' # List runs that started on or after March 1, 2014
#' listruns(rc, start="2014-03-01")
#' # List runs that contain a tag with the string "analysis v1.3")
#' listRuns(rc, tag="analysis v1.3")
#' }
#
setMethod("listRuns", signature("Recordr"), function(recordr, id=as.character(NA), script=as.character(NA), start = as.character(NA), end=as.character(NA), tag=as.character(NA), 
                                                     error=as.character(NA), seq=as.character(NA), orderBy = "-startTime") {
  
  runs <- selectRuns(recordr, runId=id, script=script, startTime=start, endTime=end, tag=tag, errorMessage=error, seq=as.character(seq), orderBy=orderBy)
  if (length(runs) == 0) {
    message(sprintf("No runs matched search criteria."))
    return(invisible(runs))
  }
  
  # Print header line
  printRun(headerOnly = TRUE)
  # Loop through selected runs
  for(i in 1:length(runs)) {
    printRun(runs[[i]])
  }
  
  # Return invisible copy of runs, as we have already printed out a formatted version of the run info.
  invisible(execMetaTodata.frame(runs))
})

# Internal function used to print execution metadata for a single run
# For the fields that can have variable width content, specify a maximum
# length that will be displayed.
# @param row the row (character vector) to print
# @param headerOnly if TRUE then only the header line is printed, if FALSE then only the row is printed
# authoer: slaughter

printRun <- function(run=NA, headerOnly = FALSE)  {
  
  tagLength = 20
  scriptNameLength = 30
  errorMsgLength = 30
 
  #fmt <- "%-20s %-20s %-19s %-19s %-36s %-36s %-19s %-30s\n"
  fmt <- paste("%-6s", "%-", sprintf("%2d", scriptNameLength), "s", 
               " %-", sprintf("%2d", tagLength), "s",
               # " %-19s %-19s %-45s %-19s",
               " %-19s %-19s %-13s %-19s",
               " %-", sprintf("%2d", errorMsgLength), "s", "\n", sep="")
  # Padding for blank values for seq & script name
  paddingLength <- 6 + scriptNameLength 
  padding <- paste(character(paddingLength), collapse=" ") 
  fmtSecondary <-  paste("%-", sprintf("%2d", paddingLength), "s", 
                     " %-", sprintf("%2d", tagLength), "s", "\n", sep="")
  
  # Print only the column headings
  if (headerOnly) {
    cat(sprintf(fmt, "Seq", "Script", "Tag", "Start Time", "End Time", "Run Id", "Published Time", "Error Message"), sep = " ")
    # Print additional lines, i.e. column values from child tables
  } else {
    console <- run@console
    if(console) {
      thisScript <- "Console log"
    } else {
      thisScript <- run@softwareApplication
      # Print shortened form of script name, e.g. "/home/slaugh...ocalReadWrite.R"
      thisScript <- condenseStr(run@softwareApplication, 30)
    }
    thisStartTime    <- run@startTime
    thisEndTime      <- run@endTime
    thisRunId       <- run@executionId
    # Only print the last 10 digits of the runid, because people don't like to see a long uuid, and
    # the entire string takes up too much space.
    thisRunId       <- sprintf("...%s", substring(thisRunId, nchar(thisRunId)-9, nchar(thisRunId)))
    #thisPackageId    <- run@datapackageId
    thisPublishTime  <- run@publishTime
    thisErrorMessage <- run@errorMessage
    thisTag         <- run@tag[[1]]
    thisSeq         <- run@seq
    cat(sprintf(fmt, thisSeq, strtrim(thisScript, scriptNameLength), strtrim(thisTag, tagLength), thisStartTime, 
                thisEndTime, thisRunId, thisPublishTime, strtrim(thisErrorMessage, errorMsgLength)), sep = " ")
    # Print additional tag values for this executionId, with only the tag displayed on the line (which is beneath the .
    if(length(run@tag) > 1) {
      for(iTag in 2:length(run@tag)) {
        thisTag <- run@tag[[iTag]]
        cat(sprintf(fmtSecondary, padding, strtrim(thisTag, tagLength)))
      }
    }
  }
}

#' View detailed information for an execution
#' @description Detailed information for an execution is printed to the display.
#' @param recordr A Recordr instance
#' @param ... additional parameter
#' @export
#' @seealso \code{\link[=Recordr-class]{Recordr}} { class description}
setGeneric("viewRuns", function(recordr, ...) {
  standardGeneric("viewRuns")
})

#' @rdname viewRuns 
#' @details The execution and file information for runs that match the search criteria are 
#' printed to the console. The output is divided into three sections: "details", "used"
#' and "generated". The "details" section shows execution information such as the start and end time
#' of the run, run identifier, etc. The "used" section lists files that were read by a run. The
#' "generated" section lists files that were created by a run. The list that is returned from \code{"viewRuns"}
#' contains two elements - a data.frame with the execution information, and a data.frame that contains
#' file information.
#' @return A list that contains information about all selected runs.
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
#' @param verbose a \code{"logical"}, if TRUE then extra information is printed.
#' @param page A logical value - if TRUE then pause after each run is displayed.
#' @param output a \code{"logical"}, if FALSE then no output is printed to the console (useful if only the returned object is needed).
#' @examples
#' \dontrun{
#' rc <- new("Recordr")
#' # View the tenth run that was recorded
#' viewRuns(rc, seq=10)
#' # View the first ten runs, with only the files "generated" section displayed
#' info <- viewRuns(rc, seq="1:10", sections="generated")
#' nrow(info$runs)
#' nrow(info$files)
#' }
setMethod("viewRuns", signature("Recordr"), function(recordr, id=as.character(NA), file=as.character(NA), start=as.character(NA), end=as.character(NA), tag=as.character(NA), error=as.character(NA),
                                                 seq=as.character(NA), orderBy="-startTime", sections=c("details","used","generated"), verbose=FALSE, page=TRUE,
                                                 output=TRUE) {
  
  # User requested no output, so send output to a temp file. 
  if (!output) {
    tf <- tempfile()
    sink(file=tf)
    on.exit(expr=sink(file=NULL))
  }
  
  runs <- selectRuns(recordr, runId=id, script=file, startTime=start, endTime=end, tag=tag, errorMessage=error, seq=seq, orderBy=orderBy)
  # selectRuns returns a list of ExecMetadata objects
  if (length(runs) == 0) {
      message(sprintf("No runs matched search criteria."))
    return(runs)
  }
        
  filesdf <-  data.frame(row.names=NULL, stringsAsFactors=F)
  # Loop through selected runs
  for(i in 1:length(runs)) {     
    thisRow <- runs[[i]]       
    executionId         <- thisRow@executionId
    metadataId          <- thisRow@metadataId
    tag                 <- thisRow@tag
    datapackageId       <- thisRow@datapackageId
    user                <- thisRow@user
    subject             <- thisRow@subject
    hostId              <- thisRow@hostId
    startTime           <- thisRow@startTime
    operatingSystem     <- thisRow@operatingSystem
    runtime             <- thisRow@runtime
    softwareApplication <- thisRow@softwareApplication
    moduleDependencies  <- thisRow@moduleDependencies
    endTime             <- thisRow@endTime
    errorMessage        <- thisRow@errorMessage
    publishTime         <- thisRow@publishTime
    console             <- as.logical(thisRow@console)
    publishNodeId       <- thisRow@publishNodeId
    publishId           <- thisRow@publishId
    seq                 <- thisRow@seq
    thisRunDir <- sprintf("%s/runs/%s", recordr@recordrDir, executionId)
    
    # Read the RDF relationships that were saved to a file. For space considerations, the
    # entire data package is not serialized to disk. 
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
        cat(sprintf("%s was executed on %s\n", dQuote(condenseStr(thisScript, 30)), startTime))
      }
      cat(sprintf("Tag: %s\n", dQuote(paste(tag, collapse=","))))
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
      cat(sprintf("R version: %s\n", runtime))
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

    fstatsAll <-  data.frame(row.names=NULL, stringsAsFactors=F)
    # Read the info file once, and prepare this dfs of read files and generated files
    if(is.element("used", sections) || is.element("generated", sections)) {
      fstatsRead <- readFileMeta(recordr, executionId=executionId, access="read")
      if(nrow(fstatsRead) > 0) {
        fstatsAll <- rbind(fstatsAll, fstatsRead)
      }
      #fstatsRead <- fstats[order(basename(fstatsRead$filePath)),]
      fstatsWrite <- readFileMeta(recordr, executionId=executionId, access="write")
      #fstatsWrite <- fstats[order(basename(fstatsWrite$filePath)),]
      if(nrow(fstatsWrite) > 0) {
        fstatsAll <- rbind(fstatsAll , fstatsWrite)
      }
    }
    # "%-30s %-10d %-19s\n"
    fileNameLength = 30    
    filePathLength = 60
    # Print out the [used] section, i.e. files used by this execution.
    if (is.element("used", sections)) {
      cat(sprintf("\n[used]: %d items used by this run\n", nrow(fstatsRead)))
      #if(nrow(fstatsRead) > 0 || length(inIds) > 0) {
      if(nrow(fstatsRead) > 0) {
        cat(sprintf("-----------------------------------\n"))
        fmt <- paste("%-", sprintf("%2d", filePathLength), "s",  " %-12s %-19s\n", sep="")
        cat(sprintf(fmt, "Location", "Size (kb)", "Modified time"), sep = " ")
        for (i in 1:nrow(fstatsRead)) {
          cat(sprintf(fmt, condenseStr(fstatsRead[i, "filePath"], filePathLength), fstatsRead[i, "size"], fstatsRead[i, "modifyTime"]), sep = "")
        }
      }
    }
    
    # Print out the [generated] section, i.e. files generated by this execution.
    if (is.element("generated", sections)) {
      cat(sprintf("\n[generated]: %d items generated by this run\n", nrow(fstatsWrite)))
      if(nrow(fstatsWrite) > 0) {
        cat(sprintf("-----------------------------------------\n"))
        fmt <- paste("%-", sprintf("%2d", filePathLength), "s",  " %-12s %-19s\n", sep="")
        cat(sprintf(fmt, "Location", "Size (kb)", "Modified time"), sep = "")
        #for (i in 1:nrow(fstatsWrite) || length(outIds) > 0) {
        for (i in 1:nrow(fstatsWrite)) {
            cat(sprintf(fmt, condenseStr(fstatsWrite[i, "filePath"], filePathLength), fstatsWrite[i, "size"], fstatsWrite[i, "modifyTime"]), sep = "")
        }
      }
    }
    
    # Accumulate file entries for this run into the data.frame that holds 
    # file entries for all selected runs.
    filesdf <- rbind(filesdf, fstatsAll)
    # Print provenance relationships
    if(verbose) {
      cat(sprintf("\nProvenance relationships:\n"))
      print(relations)
    }
    
    # Process next run if output is being suppressed
    if(!output) next
    
    # Page console output if multiple runs are being viewed
    if (length(runs) > 1 && page) {
      inputLine <- readline("enter <return> to continue, q<return> to quit: ")
      if (inputLine == "q") break
      # Clear the console screen
      cat("\014")
      next
    }
  }
  
  rundf <- execMetaTodata.frame(runs)
  invisible(list(runs = rundf, files = filesdf))
})

#' Publish a recordr'd execution to DataONE
#' @param recordr a Recordr instance
#' @param ... additional parameters
#' seealso \code{\link[=Recordr-class]{Recordr}} { class description}
#' @export
setGeneric("publishRun", function(recordr, ...) {
  standardGeneric("publishRun")
})

#' @rdname publishRun
#' @param id the run identifier for the execution to upload to DataONE
#' @param seq The sequence number for the execution to upload to DataONE
#' @param assignDOI a boolean value: if TRUE, assign DOI values for system metadata, otherwise assign uuid values
#' @param update a boolean value: if TRUE, republish a previously published execution
#' @param quiet A boolean value: if TRUE, informational messages are not printed (default=TRUE)
#' @return The published identifier of the uploaded package
setMethod("publishRun", signature("Recordr"), function(recordr, id=as.character(NA), 
                                                       seq=as.character(NA), 
                                                       assignDOI=FALSE, update=FALSE, quiet=TRUE) {
  
  if (!requireNamespace("dataone", quietly = TRUE)) {
    stop("Package \"dataone\" needed for function \"publishRun\" to work. Please install it.",
         call. = FALSE)
  }
  if (!requireNamespace("EML", quietly = TRUE)) {
    stop("Package EML needed for function \"publishRun\" to work. Please install it.",
         call. = FALSE)
  }
  if(is.na(id) && is.na(seq) ||
     !is.na(id) && !is.na(seq)) {
    stop("Please specify either \"seq\" or \"id\" parameter")
  }
  
  # readExecMeta returns a list of ExecMetadata objects
  if(!is.na(seq)) {
    thisExecMeta <- readExecMeta(recordr, seq=seq)
  } else if (!is.na(id)) {
    thisExecMeta <- readExecMeta(recordr, id=id)
  }
  
  if(length(thisExecMeta) == 0) {
      stop(sprintf("No exeuction found\n"))
  } else {
    # Get the first (and only) ExecMetadata object
    thisExecMeta <- thisExecMeta[[1]]
  }
  if(is.na(id)) id <- thisExecMeta@executionId
 
  runDir <- sprintf("%s/runs/%s", recordr@recordrDir, id)
  if (! file.exists(runDir)) {
    msg <- sprintf("A directory was not found for execution identifier: %s\n", id)
    stop(msg)
  }
  # See if this execution has been published before
  if (!is.na(thisExecMeta@publishTime)) {
    msg <- sprintf("The datapackage for this execution was published on %s\n", thisExecMeta@publishTime)
    stop(msg)
  }
  
  # Get configuration parameters, use defaults if a value not set
  public <- getOption("public_read_allowed")
  if(is.null(public)) public <- TRUE
  replicationAllowed <- getOption("replication_allowed") 
  if(is.null(replicationAllowed)) replicationAllowed <- TRUE
  numberOfReplicas <- getOption("number_of_replicas")
  if(is.null(numberOfReplicas)) numberOfReplicas <- 3
  preferredNodes <- getOption("preferred_replica_node_list")
  if(is.null(preferredNodes)) preferredNodes <- as.character(NA)
  mnId <- getOption("target_member_node_id")
  if(is.null(mnId)) mnId <- "urn:node:mnStageUCSB2"
  d1Env <- getOption("dataone_env")
  if(is.null(d1Env)) d1Env <- "STAGING"
  # PublishTime for EML 
  publishDay <- format(Sys.time(), format="%Y-%m-%d")
  publishTime <- Sys.time()

  # Read the options value for the DataONE environment to use,
  # e.g. "PROD" for production, "STAGING", "SANDBOX", "DEV"
  if(!quiet) cat(sprintf("Contacting coordinating node for environment %s...\n", d1Env))
  if(!quiet) cat(sprintf("Getting member node url for memober node id: %s...\n", mnId))
  d1c <- D1Client(d1Env, mnId)
  if (is.null(d1c@mn)) {
    stop(sprintf("Unable to contact member node \"%s\".\nUnable to publish run.\n", mnId))
  }
  resolveURI <- sprintf("%s/resolve", d1c@cn@endpoint)
  
  # Check the user's DataONE authentication.
  am <- AuthenticationManager()
  if(!dataone:::isAuthValid(am, d1c@mn)) {
    msg <- sprintf("Please see \"DataONE Authentication\" in \"intro_recordr\" vignette.")
    msg <- sprintf("%sEnter this command at the R console: \"vignette(\"intro_recordr\")", msg)
    stop(msg)
  }
  
  subject <- dataone:::getAuthSubject(am, d1c@mn)
  if (!quiet) cat(sprintf("Publishing execution %s to %s\n", id, mnId))
 
  packageId <- thisExecMeta@datapackageId
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
  metadataId <- thisExecMeta@metadataId
  # TODO: use getMetadata() output when read_eml accepts
  # XMLInternalDocument, as the documentation says it should
  #metadata <- getMetadata(recordr, id=id, as="parsed")
  metadataFile <- sprintf("%s/%s.xml", runDir, metadataId)
  emlObj <- read_eml(metadataFile)
  
  # Check options to see if the default DataONE submitter and rightsholder
  # should be overriden by user supplied values.
  rightsHolder <- getOption("rights_holder")
  submitter <- getOption("submitter")
  if(is.null(rightsHolder)) rightsHolder <- as.character(NA)
  if(is.null(submitter)) submitter <- as.character(NA)
  
  # Upload each data object that was used or geneated by the datapackage
  if(!quiet) cat(sprintf("Getting file info for execution %s\n", id))
  files <- readFileMeta(recordr, executionId=id)
  if(nrow(files) > 0) {
    for (iRow in 1:nrow(files)) {
      thisFile <- files[iRow,]
      format <- thisFile[['format']]
      fileId <- thisFile[['fileId']]
      access <- thisFile[['access']]
      origFilename <- thisFile[['filePath']]
      filePath <- sprintf("%s/%s", recordr@recordrDir, thisFile[['archivedFilePath']])
      # Create DataObject for the science dataone
      # sysmeta@sumitter and rightsholder will be set to subject from auth token or X.509 certificate
      sciObj <- new("DataObject", id=fileId, format=format, mnNodeId=mnId, filename=filePath, suggestedFilename=basename(origFilename))
      if(!is.na(submitter)) sciObj@sysmeta@submitter <- submitter
      if(!is.na(rightsHolder)) sciObj@sysmeta@rightsHolder <- rightsHolder
      if (public) sciObj <- setPublicAccess(sciObj)
      # During endRecord(), each science object was associated with the metadata object
      # via insertRelationship() with the 'documetns' relationship. These relationships
      # were stored with the rest of the package relationships, so we don't have to add them
      # in again.
      if(!quiet) cat(sprintf("Adding science object with id: %s, file: %s\n", 
                             getIdentifier(sciObj), basename(thisFile[['filePath']])))
      addData(pkg, sciObj)
      # Now update the metadata object corresponding to this dataset in order to set the
      # Online distribution value so that MetacatUI can properly identify and display this item.
      # The 'additionalInfo' value was stored during endRecord() so that we could match up the
      # file and the eml element after the run was finished.
      for(iEntity in 1:length(emlObj@dataset@otherEntity)) {
        thisDatasetId <- emlObj@dataset@otherEntity[[iEntity]]@EntityGroup@alternateIdentifier[[1]]@character
        if(fileId == thisDatasetId) {
          url <- sprintf("%s/%s", resolveURI, fileId)
          distrib <- new("distribution", online = new("online", url=url))
          emlObj@dataset@otherEntity[[iEntity]]@EntityGroup@physical[[1]]@distribution <- as(list(distrib), "ListOfdistribution")
        }
      }
    }
  }
  
  # Now that we have used the alternate identifier, blank it out so that the uploaded EML won't have it. Currently
  # the EML parser in Metacat doesn't allow alternate identifiers.
  if(length(emlObj@dataset@otherEntity) > 0) {
    for(iEntity in 1:length(emlObj@dataset@otherEntity)) {
      emlObj@dataset@otherEntity[[iEntity]]@EntityGroup@alternateIdentifier <- new("ListOfalternateIdentifier")
    }
  }
  emlObj@dataset@ResourceGroup@pubDate <- as(publishDay, "pubDate")
  # Update the metadata stored for this run. The putMetadata() function
  # can't read eml objects yet, so have to write it to a file.
  tempMetadataFile <- tempfile()
  write_eml(emlObj, tempMetadataFile)
  putMetadata(recordr, id=id, metadata=tempMetadataFile, asText=FALSE)
  # Use windows friendly filenames, i.e. no ":"
  metaObj <- new("DataObject", id=metadataId, format=EML_211_FORMAT, mnNodeId=mnId, filename=tempMetadataFile,
                 suggestedFilename=gsub(":", "_", basename(metadataFile)))
  if(!is.na(submitter)) metaObj@sysmeta@submitter <- submitter
  if(!is.na(rightsHolder)) metaObj@sysmeta@rightsHolder <- rightsHolder
  addData(pkg, metaObj)
  
  #
  # Add the saved relationships back in the data package
  relationshipFile <- sprintf("%s/%s.csv", runDir, packageId)
  relationships <- read.csv(relationshipFile, stringsAsFactors=FALSE)
  if (nrow(relationships) > 0) {
    for(i in 1:nrow(relationships)) {
      thisRelationship <- relationships[i,]
      thisSubject <- thisRelationship[["subject"]]
      thisPredicate <- thisRelationship[["predicate"]]
      thisObject <- thisRelationship[["object"]]
      thisSubjectType <- thisRelationship[["subjectType"]]
      thisObjectType <- thisRelationship[["objectType"]]
      thisDataTypeURI <- thisRelationship[["dataTypeURI"]]
      insertRelationship(pkg, subjectID=thisSubject, objectIDs=thisObject, predicate=thisPredicate, 
                         subjectType=thisSubjectType, objectTypes=thisObjectType, dataTypeURIs=thisDataTypeURI)
    }
  }
  
  if(!quiet) cat(sprintf("Uploading data package...\n"))
  # uploadDataPackage returns the "package pid" 'published identifier' for this datapackage. This will be displayed in the 
  # viewRuns() output for this under "Published ID".
  resourceMapId <- uploadDataPackage(d1c, pkg, replicate=replicationAllowed, numberReplicas=numberOfReplicas, 
                                     preferredNodes=preferredNodes, public=public, quiet=quiet, resolveURI=resolveURI)
  
  if(!quiet) cat(sprintf("Uploaded data package with resource map id: %s", resourceMapId))
  # Record the time that this execution was published, the published id, subject that submitted the data.
  updateExecMeta(recordr, executionId=id, subject=subject, publishTime=publishTime, publishNodeId=mnId, 
                 publishId=metadataId) 
  return(metadataId)
})

#' Retrieve the metadata object for a run
#' @description When a script or console session is recorded (see record() and startrecord()), 
#' a metadata object is created that describes the objects associated with the run, using the
#' Ecological Metadata Language \url{https://knb.ecoinformatics.org/#external//emlparser/docs/index.html}.
#' This metadata can be retrieved from the recordr cache for review or editing if desired. If the metadata
#' is updated, it can be re-inserted into the recordr cache using the \code{putMetadata} method.
#' @param recordr a Recordr instance
#' @param ... additional parameters
#' seealso \code{\link[=Recordr-class]{Recordr}} { class description}
#' @export
setGeneric("getMetadata", function(recordr, ...) {
  standardGeneric("getMetadata")
})

#' @rdname getMetadata
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
  
  # readExecMeta returns a list of ExecMetadata objects
  if(!is.na(seq)) {
    execMeta <- readExecMeta(recordr, seq=seq)
  } else if (!is.na(id)) {
    execMeta <- readExecMeta(recordr, executionId=id)
  }
  
  # Was an execution found for this seq or id?
  if(length(execMeta) == 0) {
      stop(sprintf("No exeuction found\n"))
  } else {
    execMeta <- execMeta[[1]]
  }
  
  # Locate the metadata file
  if(is.na(id)) id <- execMeta@executionId
  runDir <- sprintf("%s/runs/%s", recordr@recordrDir, id)
  if (! file.exists(runDir)) {
    msg <- sprintf("A directory was not found for execution identifier: %s\n", id)
    stop(msg)
  }
  
  metadataId <- execMeta@metadataId
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
#' @param ... additional parameters
#' @seealso \code{\link[=Recordr-class]{Recordr}} { class description}
#' @export
setGeneric("putMetadata", function(recordr, ...) {
  standardGeneric("putMetadata")
})

#' @rdname putMetadata
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
  
  # readExecMeta returns a list of ExecMetadata objects
  if(!is.na(seq)) {
    execMeta <- readExecMeta(recordr, seq=seq)
  } else if (!is.na(id)) {
    execMeta <- readExecMeta(recordr, executionId=id)
  }
  
  # Is this a vaiid id or seq?
  if(length(execMeta) == 0) {
      stop(sprintf("No execution found for the specified execution or sequence number\n"))
  } else {
    execMeta <- execMeta[[1]]
  }
  
  # Locate the metadata file
  if(is.na(id)) id <- execMeta@executionId
  runDir <- sprintf("%s/runs/%s", recordr@recordrDir, id)
  if (! file.exists(runDir)) {
    msg <- sprintf("A directory was not found for execution identifier: %s\n", id)
    stop(msg)
  }
  
  metadataId <- execMeta@metadataId
  metadataFile <- sprintf("%s/%s.xml", runDir, metadataId)
  metadataFileBackup <- sprintf("%s.bak", metadataFile)
  
  # Either writeout the metadata to the run directory, or copy the user file to that location
  if(asText) {
    # Validate the EML
    result = tryCatch({
      xmlDoc <- xmlParse(metadata)
      checkEML <- read_eml(xmlDoc)
    }, warning = function(w) {
      stop(sprintf("Error re-inserting EML into execution %s", id))
    }, error = function(e) {
      stop(sprintf("Error re-inserting EML into execution %s", id))
    }, finally = {
    })
    
    # Save a backup copy first
    file.rename(metadataFile, metadataFileBackup)
    # Overwrite the existing metadata file
    setProvCapture(FALSE)
    writeLines(metadata, metadataFile)
    setProvCapture(TRUE)
  } else {
    if(!file.exists(metadata)) {
      stop(sprintf("Metadata file %s does not exists, unable to update run metadata\n", metadata))
    } else {
      result = tryCatch({
        checkEML <- read_eml(metadata)
      }, warning = function(w) {
        stop(sprintf("Error re-inserting EML into execution %s", id))
      }, error = function(e) {
        stop(sprintf("Error re-inserting EML into execution %s", id))
      }, finally = {
      })
      
      file.rename(metadataFile, metadataFileBackup)
      file.copy(metadata, metadataFile)
    }
  }
})

recordrShutdown <- function() {
  
  if (is.element(".recordr", base::search())) {
    recordrEnv <- as.environment(".recordr")
    detach(".recordr")
  }
}

#' Create a minimal EML document.
#' @description An EML document is create from the values passed in.
#' @param recordr A Recordr object.
#' @param id The identifier for the EML document.
#' @param system The system for the document.
#' @param title The document title.
#' @param creators A list of creator elements.
#' @param abstract The document abstract.
#' @param methodDescription The dataset method description.
#' @param geo_coverage The geographic coverage element.
#' @param temp_coverage The temporal coverage element.
#' @param endpoint The online distribution URL.
makeEML <- function(recordr, id, system, title, creators, abstract=NA, methodDescription=NA, geo_coverage=NA, temp_coverage=NA, endpoint=NA) {
  #dt <- eml_dataTable(dat, description=description)
  oeList <- as(list(), "ListOfotherEntity")
  # readExecMeta returns a list of execMeta objects, so get the first result, which should be the only result
  # when an executionId is specified.
  execMeta <- readExecMeta(recordr, executionId=id)[[1]]
  metadataId <- execMeta@metadataId
  fileMeta <- readFileMeta(recordr, executionId=id)
  
  # Loop through each file for this run and add an EML "otherEntity"
  # entry for each output file and script that was run.
  if (nrow(fileMeta) > 0) {
    for (fileNum in 1:nrow(fileMeta)) {
      thisFile <- fileMeta[fileNum,]
      fileId <- thisFile[["fileId"]]
      access <- thisFile[["access"]]
      # Skip this file if it was not run or generated by this execution. Recordr
      # also tracks input files, but those should not be included in the metadata
      # object as this execution did not create them.
      if(!is.element(access, c("read", "write", "execute"))) next
      filePath <- thisFile[["filePath"]]
      format <- thisFile[["format"]]
      fileSize <- thisFile[["size"]]
      
      distList <- new("ListOfdistribution")
      if (!is.na(endpoint)) {
        dist <- new("distribution", online="online", url = paste(endpoint, id, sep="/"))
        distList <- c(distList, dist)
      } else {
        dist <- new("distribution")
        distList <- c(distList, dist)
      }
      
      if(!is.na(format)) {
        formatCitation <- new("citation")
        f <- new("externallyDefinedFormat", formatName=format, citation=formatCitation)
        df <- new("dataFormat", externallyDefinedFormat=f)
        dataFormat <- df
      } else {
        df <- new("dataFormat")
      }
      
      phys <- new("physical", objectName = basename(filePath), size = new("size", as.character(fileSize), unit="bytes"),
                  distribution = distList, dataFormat = dataFormat)
      # Store the unique identifier for this entity, so that we can find it and update it later during the publish step. 
      # Turns out that DataONE doesn't recognize<otherEntity><alternateIdentifier as a valid element, so it will be
      # removed during the publishing process.
      altId <- new("alternateIdentifier", character = fileId, system = "UUID")
      eg <- new("EntityGroup", entityName=basename(filePath), physical = phys, alternateIdentifier = as(list(altId), "ListOfalternateIdentifier"))
      oe <- new("otherEntity", EntityGroup=eg, entityType=format)
      
      oeList[[length(oeList) + 1]] <- oe
    }
  }
  creatorList <- list()
  if(nrow(creators) > 0) {
    for (irow in 1:nrow(creators)) {
      individual <- new("individualName", givenName=creators[irow, 'given'], surName=creators[irow,'surname'])
      individualList <- as(list(individual), "ListOfindividualName")
      creator <- new("creator", individual = individualList, electronicMailAddress = creators[irow, 'email'])
      creatorList[[length(creatorList)+1]] <- creator
    }
  }
  
  titleObj <- new("title", value=title)
  titleList <- as(list(titleObj), "ListOftitle")
  coverage <- coverageElement(geo_coverage, temp_coverage)
  rg <- new("ResourceGroup", title = titleList, creator = as(creatorList, "ListOfcreator"), pubDate = as.character(Sys.Date()), abstract = abstract, coverage = coverage)
  
  # Create a contact for the dataset, use the first contact in the passed in data.frame
  individual <- new("individualName", givenName=creators[1, 'given'], surName=creators[1, 'surname'])
  individualList <- as(list(individual), "ListOfindividualName")
  contact <- new("contact", individual = individualList, electronicMailAddress = creators[1, 'email'])
  contactList <- as(list(contact), "ListOfcontact")
  
  if (!is.na(methodDescription)) {
    ps <- new("proceduralStep", description=methodDescription)
    ms <- new("methodStep", ProcedureStepType = ps)
    loMethodStep <- new("ListOfmethodStep", list(ms))
    methods <- new("methods", methodStep=loMethodStep)
  }
  
  ds <- new("dataset",
            ResourceGroup = rg,
            contact = contactList,
            methods = methods,
            otherEntity = as(oeList, "ListOfotherEntity")
  )

  eml <- new("eml",
             packageId = metadataId,
             system = system,
             dataset = ds)
}

#' Create a geographic coverage element from a description and bounding coordinates
#' @param geoDescription a character string containing the description of the geogragraphic covereage
#' @param west a character string containing the western most coordinate of the coverage (ex. "-134.32")
#' @param east a character string containing the eastern most coordinate of the coverage (ex. "-120.42")
#' @param north a character string containing the northern most coordinate of the coverage (ex. "34.32")
#' @param south a character string containing the southern ost coordinate of the coverage (ex. "30.14")
geoCoverage <- function(geoDescription, west, east, north, south) {
  bc <- new("boundingCoordinates", westBoundingCoordinate=as.character(west), 
            eastBoundingCoordinate=as.character(east), 
            northBoundingCoordinate=as.character(north), 
            southBoundingCoordinate=as.character(south))
  gc <- new("geographicCoverage", geographicDescription=geoDescription, boundingCoordinates=bc)
  return(gc)
}

## Create a temporal coverage object
temporalCoverage <- function(begin, end) {
  #bsd <- new("singleDateTime", calendarDate=begin)
  b <- new("beginDate", calendarDate=begin)
  #esd <- new("singleDateTime", calendarDate=end)
  e <- new("endDate", calendarDate=end)
  rod <- new("rangeOfDates", beginDate=b, endDate=e)
  temp_coverage <- new("temporalCoverage", rangeOfDates=rod)
  return(temp_coverage)
}

#' Create a coverage element
#' @param gc An EML::geographicCoverage object
#' @param tempc  A EML::temporalCoverage object
#' @return An EML::Coverage object
coverageElement <- function(gc, tempc) {
  gcList <- as(list(), "ListOfgeographicCoverage")
  gcList[[1]] <- gc
  tcList <- as(list(), "ListOftemporalCoverage")
  tcList[[1]] <- tempc
  coverage <- new("Coverage", geographicCoverage=gcList, temporalCoverage=tcList)
  return(coverage)
}

#' Get a database connection
#' @import RSQLite
#' @param dbFile the path to the recordr database file (default: ~/.recordr/recordr.sqlite)
getDBconnection <- function(dbFile) {
  dbDir <- dirname(dbFile)
  if(!file.exists(dbDir)) {
    dir.create(dbDir, recursive=T)
  }
  dbConn <- dbConnect(RSQLite::SQLite(), dbFile)
  if (dbIsValid(dbConn)) {
    return(dbConn)
  } else {
    stop(sprintf("Error opening database connection to %s\n", dbFile))
  }
}

createAdminTable <- function(recordr, newDbVersion) {
  
  if (!is.element("admin", dbListTables(recordr@dbConn))) {
    #cat(sprintf("create: %s\n", createStatement))
    createStatement <- "CREATE TABLE admin
              (id INTEGER PRIMARY KEY,
              version             TEXT not null,
              unique(version));"
    result <- dbSendQuery(conn=recordr@dbConn, statement=createStatement)
    dbClearResult(result)
  } 
  
  # Insert the new database version into the admin table
  quoteOption <- getOption("useFancyQuotes")
  options(useFancyQuotes="FALSE")
  insertStatement <- sprintf("INSERT into admin (version) VALUES (%s);", sQuote(newDbVersion))
  options(useFancyQuotes=quoteOption)
  result <- dbSendQuery(conn=recordr@dbConn, statement=insertStatement)
  dbClearResult(result)
}

#' Update the recordr database to the current version
#' @param recordr A recordr object
#' @return logical TRUE if the upgrade was successful, FALSE if a problem was encountered.
#' @export
upgradeRecordr <- function(recordr) {
  
  # User can pass in a record instance, but not required.
  if(missing(recordr)) {
    recordr <- new("Recordr", quiet=TRUE)
  }
  
  dbVersion <- getRecordrDbVersion(recordr)
  # Get recordr version from admin table. 
  recordrVersion <- packageDescription(pkg="recordr")$Version
  verInfo <- unlist(strsplit(recordrVersion, "\\.", perl=TRUE))
  rcVersionMajor <- verInfo[[1]]
  rcVersionMinor <- verInfo[[2]]
  rcVersionPatch <- verInfo[[3]]
  # Is this a development version?
  if (length(verInfo) > 4) {
    rcVersionDev <- verInfo[[4]]
  } else {
    rcVersionDev <- as.character(NA)
  }
  
  if (rcVersionMajor == "0" && rcVersionMinor == "9" && rcVersionPatch == "0") {
    if(is.na(dbVersion)) {
      upgradeToDbv090(recordr)
    }
  } 
}
  
upgradeToDbv090 <- function(recordr) {
  
  # The database version number to use with the new version or recordr.
  newDbVersion <- "0.9.0"
  # Backup database file
  oldDbFile <- recordr@dbFile
  backupDbFile <- sprintf("%s.bck", recordr@dbFile)
  file.copy(oldDbFile, backupDbFile)
  # Add tags table
  if(!createTagsTable(recordr)) {
    stop("Unable to create tags table during upgrade.")
  }
  
  tmpDBconn <- FALSE
  if (!dbIsValid(recordr@dbConn)) {
    cat(sprintf("v090: getting db conn\n"))
    dbConn <- getDBconnection(dbFile=recordr@dbFile)
    if(is.null(dbConn)) {
      stop(sprintf("Error reconnecting to database file %s\n", recordr@dbFile))
    }
    tmpDBconn <- TRUE
  } else {
    dbConn <- recordr@dbConn
  }
  
  # Populate the tags table with values from exemeta table (executionId, tag)
  selectStatement <- "SELECT executionId, tag from execmeta;"
  result <- dbSendQuery(conn=dbConn, statement=selectStatement)
  resultdf <- dbFetch(result)
  dbClearResult(result)
  
  # SQLite doesn't like the 'fancy' quotes that R uses for output, so switch to standard quotes
  quoteOption <- getOption("useFancyQuotes")
  options(useFancyQuotes="FALSE")
  
  for(irow in 1:nrow(resultdf)) {
    execId <- sQuote(resultdf[irow,"executionId"])
    tag <- sQuote(resultdf[irow,"tag"])
    insertStatement <- sprintf("INSERT into tags (executionId, tag) VALUES (%s, %s);", execId, tag)
    result <- dbSendQuery(conn=dbConn, statement=insertStatement)
    dbClearResult(result)
  }
  
  
  # Retrieve all recorded runs and order by startTime (ascending).
  runs <- selectRuns(recordr, startTime="1900-01-01",  orderBy="+startTime")
  
  # Hoy! SQLite doesn't allow you to drop a column from a table, so you
  # Have to rename the old table, create the new table without the undesired
  # column, then copy all dadta from the old table to the new.
  alterStatement <- "ALTER TABLE execmeta rename to execmeta_old;"
  result <- dbSendQuery(conn=dbConn, statement=alterStatement)
  dbClearResult(result)
  
  for(i in 1:length(runs)) {
    thisExecMeta <- new("ExecMetadata")
    thisRun <- runs[[i]]       
    thisExecMeta@executionId         <- thisRun@executionId
    thisExecMeta@metadataId          <- thisRun@metadataId
    thisExecMeta@tag                 <- thisRun@tag
    thisExecMeta@datapackageId       <- thisRun@datapackageId
    thisExecMeta@user                <- thisRun@user
    thisExecMeta@subject             <- thisRun@subject
    thisExecMeta@hostId              <- thisRun@hostId
    thisExecMeta@startTime           <- thisRun@startTime
    thisExecMeta@operatingSystem     <- thisRun@operatingSystem
    thisExecMeta@runtime             <- thisRun@runtime
    thisExecMeta@softwareApplication <- thisRun@softwareApplication
    thisExecMeta@moduleDependencies  <- thisRun@moduleDependencies
    thisExecMeta@endTime             <- thisRun@endTime
    thisExecMeta@errorMessage        <- thisRun@errorMessage
    thisExecMeta@publishTime         <- thisRun@publishTime
    thisExecMeta@console             <- as.logical(thisRun@console)
    thisExecMeta@publishNodeId       <- thisRun@publishNodeId
    thisExecMeta@publishId           <- thisRun@publishId
    thisExecMeta@seq                 <- thisRun@seq
    # Create a new 'execmeta' table by writing out a new execmeta object.
    writeExecMeta(recordr, thisExecMeta)
  }
  
  # TODO: check that the new table has the same of rows as the old table.
  alterStatement <- "DROP TABLE if exists execmeta_old;"
  result <- dbSendQuery(conn=dbConn, statement=alterStatement)
  dbClearResult(result)
  
  createAdminTable(recordr, newDbVersion)
  # Update database version number in 
  insertStatement <- sprintf("INSERT into admin (version) VALUES (%s);", sQuote(newDbVersion))
  result <- dbSendQuery(conn=dbConn, statement=alterStatement)
  dbClearResult(result)
  
  options(useFancyQuotes=quoteOption)
  
  # We can't return this database connection we just opened, as we are not returning the recordr object with a
  # slot that contains the new database connection, so just disconnect. 
  if(tmpDBconn) dbDisconnect(dbConn)
  message(sprintf("The recordr database has been upgraded to %s", newDbVersion))
  invisible(return(TRUE))
}

getRecordrDbVersion <- function(recordr) {
  # Check if the connection to the database is still working and if
  # not, create a new, temporary connection.
  tmpDBconn <- FALSE
  dbVersion <- as.character(NA)
  if (!dbIsValid(recordr@dbConn)) {
    dbConn <- getDBconnection(dbFile=recordr@dbFile)
    if(is.null(dbConn)) {
      return(as.character(NA))
    }
    tmpDBconn <- TRUE
  } else {
    dbConn <- recordr@dbConn
  }
  
  if (!is.element("admin", dbListTables(dbConn))) {
    return(as.character(NA))
  }
  
  selectStatement <- "SELECT version from admin DESC;"
  result <- dbSendQuery(conn=dbConn, statement=selectStatement)
  resultdf <- dbFetch(result)
  dbClearResult(result)
  # We can't return this database connection we just opened, as we are not returning the recordr object with a
  # slot that contains the new database connection, so just disconnect. 
  if(tmpDBconn) dbDisconnect(dbConn)
  
  # If no version found, return NA
  if(nrow(resultdf) == 0) {
    dbVersion <- as.character(NA)
  } else {
    dbVersion <- resultdf[1,"version"]
  }
  
  if(tmpDBconn) {
    dbDisconnect(dbConn)
  }
  return(dbVersion)
}

execMetaTodata.frame <- function(execMetaList) {
  # Now conver the list of ExecMetadata objects to a data.frame for the user's consumption.
  rundf <- data.frame()
  if (length(execMetaList) > 0) {
    for (iRun in 1:length(execMetaList)) {
      thisRun <- execMetaList[[iRun]]
      #slotDataTypes <- getSlots("ExecMetadata")
      thisRunList <- list()
      #slotName <- execSlotNames[[i]]
      #slotDataType <- slotDataTypes[[i]]
      execSlotNames <- slotNames("ExecMetadata")
      for (i in 1:length(execSlotNames)) {
        thisSlotName <- execSlotNames[[i]]
        if (thisSlotName == "tag") {
          thisRunList[length(thisRunList) + 1] <- paste(slot(thisRun, thisSlotName), collapse=",")
        } else {
          thisRunList[length(thisRunList) + 1] <- slot(thisRun, thisSlotName)
        }
      }
      names(thisRunList) <- slotNames("ExecMetadata")
      if (iRun == 1) {
        rundf <- as.data.frame(thisRunList, row.names=NULL, stringsAsFactors=F)
      } else {
        newdf <- as.data.frame(thisRunList, row.names=NULL, stringsAsFactors=F)
        rundf <- rbind(rundf, newdf)
      }
    }
  }
  return(rundf)
}

# Return a shortened version of a filename to the specified length. The
# beginning and end of the filename is returned, with ellipses inbetween
# to denote the removed portion, e.g. 
#    filename <- "/Users/smith/data/urn:uuid:a84c2234-d07f-41d6-8c53-61b570afc79f.csv", 30)
# filename set to "/Users/smith...1b570afc79f.csv"
condenseStr <- function(filePath, newLength) {
  fnLen <- nchar(filePath)[[1]]
  if(newLength >= fnLen) return(filePath)
  # Requested length too short, so return first part of string
  if(newLength < 5) return(substr(filePath, 1, newLength))
  # Substract space for ellipses
  charLen <- as.integer(newLength - 3)
  # Get str before ellipses
  len1 <- as.integer(charLen / 2)
  # Add additional char at end if desired length is odd
  len2 <- as.integer(charLen / 2) + charLen %% 2
  # Get str after ellipses
  str1 <- substr(filePath, 1, len1)
  str2 <- substr(filePath, fnLen-(len2-1), fnLen)
  newStr <- sprintf("%s...%s", str1, str2)
  return(newStr)
}

#' Remove a file from the recordr archive directory
#' @param recordr A Recordr object
#' @param fileId The fileId to remove from the archive
#' @return A logical value - TRUE if the file is remove, FALSE if not
#' @import uuid
#' @note This function is intended to run only during a record() session, i.e. the
#' recordr environment needs to be available.
unArchiveFile <- function(recordr, fileId) {
  # Delete a file from the file archive if no other execution is still 
  # referencing it. Note that when executions archive files that they use, 
  # if the file already is in the archive, it will just be referenced by the 
  # new execution, and not re-archived.
  archivedFilePath <- as.character(NA)
  fm <- readFileMeta(recordr, fileId=fileId)
  if(nrow(fm) == 0) {
    warning("File not found in database, unable to delete file from archive with fileId: %s", fileId)
  } else {
    # Are more that the current execution referencing the file? If yes, then don't delete it.
    checksum <- fm[1,'sha256']
    frefs <- readFileMeta(recordr, sha256=checksum)
    if(nrow(frefs) == 1) {
      archivedFilePath <- sprintf("%s/%s", recordr@recordrDir, fm[1,'archivedFilePath'])
      unlink(archivedFilePath, force=TRUE)
    }
  }
  invisible(archivedFilePath)
}
