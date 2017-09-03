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
#' @import datapack
#' @import methods
#' @import uuid
#' @import digest
#' @import rappdirs
#' @import EML
#' @import RSQLite
#' @import XML
#' @import hash
#' @import DiagrammeR
#' @importFrom utils URLdecode packageDescription read.csv savehistory sessionInfo timestamp write.csv
#' @include Constants.R
#' @section Methods:
#' \itemize{
#'  \item{\code{\link[=initialize-Recordr]{initialize}}}{: Initialize a Recordr object}
#'  \item{\code{\link{startRecord}}}{: Begin recording provenance for an R session}
#'  \item{\code{\link{endRecord}}}{: Get the Identifiers of Package Members}
#'  \item{\code{\link{record}}}{: Get the data content of a specified data object}
#'  \item{\code{\link{listRuns}}}{: Output a list of recorded runs to the console}
#'  \item{\code{\link{viewRuns}}}{: Record relationships of objects in a DataPackage}
#'  \item{\code{\link{deleteRuns}}}{: Record derivation relationships between objects in a DataPackage}
#'  \item{\code{\link{publishRun}}}{: Upload all objects associated with a run to a repository}
#'  \item{\code{\link{traceRuns}}}{: Trace processing lineage by finding related executions.}
#'  \item{\code{\link{plotRuns}}}{: Trace processing lineage for a run and plot it.}
#' }
#' @seealso \code{\link{recordr}}{ package description.}
#' @export
setClass("Recordr", slots = c(recordrDir = "character",
                              dbConn = "SQLiteConnection",
                              dbFile = "character"))

#' Initialize a Recorder object
#' @details A recordr object is returned that can be used with other \code{recordr} package
#' methods. When the optional \code{newDir} argument is used, the recordr home directory is
#' changed to the new value. The default behaviour is to have data copied from the old
#' home directory to the new one, but this can be changed by using the \code{copy} argument, i.e.
#' See the recordr vignette \code{'recordr Package Introduction'} for more information about 
#' information that recordr stores in the recordr home directory.
#' @rdname initialize-Recordr
#' @aliases initialize-Recordr
#' @param .Object The Recordr object
#' @param newDir The recordr home directory is changed to the new location.
#' @seealso \code{\link[=Recordr-class]{Recordr}} { class description}
setMethod("initialize", signature = "Recordr", definition = function(.Object, newDir=as.character(NA), 
                                                                     copy=T, ...) {
  
  # Get the current recordr home directory. If this is the first time that this function
  # has been callled, then the default initial directory is selected.
  recordrDir <- getRecordrDir()
  
  # The user has specified the recordr directory, which may be a new location from
  # the previously used directory.
  if(!is.na(newDir)) {
    recordrDir <- changeHome(.Object, recordrDir, newDir=newDir, copy, ...) 
    message(sprintf("The recordr home directory has been chagned to \"%s\".", recordrDir))
  }
  
  # When creating a new home directory, don't print the msg if it is a temp directory
  if(!file.exists(recordrDir)) {
    dir.create(recordrDir, recursive = TRUE)
    message(sprintf("A new recordr home directory has been created at:\n\n\t%s\n", recordrDir))
    if(grepl(tempdir(), recordrDir)) {
      message("The recordr package will save run information to this directory, which is under")
      message("the R session temporary directory. Therefore the information that recordr collects")
      message("will be removed by R when the current R session ends.")
      message("\nIf you wish to change the recordr home directory so that information is saved to a")
      message("permanent location please use the \"newDir\" argument, for example")
      message("\n\t\"rc <- new(\"Recordr\", newDir=\"/Users/bobsmith/recordr\")")
    }
  } 
  
  .Object@recordrDir <- recordrDir
  # Open a connection to the database that contains execution metadata,
  .Object@dbFile <- normalizePath(file.path(recordrDir, "recordr.sqlite"), mustWork=FALSE)
  dbConn = NULL
  dbConn <- getDBconnection(dbFile=.Object@dbFile)
  if (is.null(dbConn)) {
    stop("Unable to create a Recordr object\n")
  } else {
    .Object@dbConn <- dbConn
  }

  if (!dbIsValid(dbConn)) {
    if(is.null(dbConn)) {
      stop(sprintf("Error reconnecting to database file %s\n", .Object@dbFile))
    }
  }
  # 
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
 
  dbDisconnect(.Object@dbConn)
   
  return(.Object)
})

# Get the recordr home directory, which may be in one of several different
# locations, depending on user choice.
getRecordrDir <- function() {
  
  recordrDir <- ""
  # The default recordr home directory is the R session temp direcory. 
  # If the user has setup a permanent home directory using configHome(), then the
  # home directory will always be located at the value returned from rappdir::user_data_dir(). 
  # This location can either be a directory or a link to another directory, for example on
  # a large disk
  appDir <- rappdirs::user_data_dir(appname="recordr", appauthor = "NCEAS")
  appDirBase <- rappdirs::user_data_dir(appauthor = "NCEAS")
  defaultDir <- sprintf("%s/recordr", appDirBase)
  symlink <- Sys.readlink(sprintf("%s/recordr", appDirBase))
  isTmp <- FALSE
  # Directory location if we are in demo mode (required by CRAN, i.e. can't write to home dir 
  # unless user has explicitly consented to it.)
  tmpDir <- sprintf("%s/recordr", tempdir())
  # If the default directory, as determined by rappdirs, has been symbolically 
  # linked to another directory, then rappdis will return the directory being
  # linked to. This link is created when 'changeHome()' is called and the user
  # specifies a customer directory location.
  if (file.exists(appDir)) {
    # Use the default directory if it has been created. This directory will only exist
    # if the user has previously agreed to have recordr create it.
    recordrDir <- appDir
    # Now do an additional check to see if the defaultDir is a link
    symlink <- Sys.readlink(defaultDir)
    
    if(symlink != "") {
      attr(recordrDir, "isDefault") <- FALSE
      attr(recordrDir, "linkedTo") <- symlink
    } else {
      attr(recordrDir, "isSymlink") <- FALSE
      attr(recordrDir, "isDefault") <- TRUE
      attr(recordrDir, "linkedTo") <- as.character(NA)
    }
  } else {
    # Use the temporary directory
    recordrDir <- tmpDir
    attr(recordrDir, "isDefault") <- FALSE
    attr(recordrDir, "linkedTo") <- as.character(NA)
    isTmp <- TRUE
  }
  
  attr(recordrDir, "default") <- defaultDir
  attr(recordrDir, "isTmp") <- isTmp
  return(recordrDir)
}

#' Change the recordr home directory
#' @param recordr A recordr object
#' @param currentDir A character value specifying the current recordr home directory
#' @param newDir A character value, specifying the new recordr home directory
#' @param copy A logical value. A value of TRUE causes data to be copied from the old
#' directory to the new one. A default value is not set.
changeHome <- function(recordr, currentDir, newDir=as.character(NA), copy, ...) {
  
  saveToDir <- currentDir
  makeLink <- FALSE
  
  # Get info about the current directory. This directory may not have been used by
  # recordr yet, if this is the first time that recordr is being initialized and the
  # user has specified a home directory location.
  defaultDir <- attr(currentDir, "default")
  isDefault <- attr(currentDir, "isDefault")
  linkedTo <- attr(currentDir, "linkedTo") 
  isTmp <- attr(currentDir, "isTmp")
  
  # The previous dir is a symlink from the OS dependant default dir. This needs to be
  # removed so we can use the new location.
  if(!is.na(linkedTo)) {
    file.remove(defaultDir) 
    fromDir <- linkedTo
    # Don't need to move the previous dir to save it - just get it out of the way for the 
    # new one.
    saveToDir <- as.character(NA)
  } else {
    fromDir <- currentDir
    # The previous dir is the R session temp dir
    if(isTmp) {
      saveToDir <- as.character(NA)
    } else {
      # The previous dir is the OS default dir, not linked
      saveToDir <- sprintf("%s.save", fromDir)
      if(file.exists(saveToDir)) {
        tmp <- gsub(" ", "", Sys.time())
        saveToDir <- sprintf("%s.%s", saveToDir, gsub(":", "", tmp))
      }
    }
  }
  
  # User has specified that the OS dependent default dir be used.
  if(newDir == "default") {
    recordrDir <- defaultDir
    toDir <- dirname(defaultDir)
  } else {
    # Use the user specified new directory
    recordrDir <- newDir
    if(basename(recordrDir) != "recordr") {
      recordrDir <- sprintf("%s/recordr", newDir)
      # For the copy, have to have the 'recordr' parent dir, otherwise 'recordr/record' gets created.
      makeLink <- TRUE
      #file.symlink(attr(currentDir, "default"), recordrDir)
    }
    toDir <- dirname(recordrDir)
  }

  # Don't need to copy if the recordr db was never created, i.e. this is the first time init has
  # been called.
  if(file.exists(sprintf("%s/recordr.sqlite", fromDir))) {
    # Default is to copy the old dir, user can specify not to.
    if(copy) {
      if(!file.exists(toDir)) dir.create(toDir, recursive=T)
      file.copy(from=fromDir, to=toDir, overwrite = T, recursive = T, copy.mode = T, copy.date = T) 
      message(sprintf("The recordr data has been copied to the new directory at %s", recordrDir))
    } else {
      message(sprintf("No data to copy from the previous recordr directory %s", fromDir))
    }
  }
  
  # Do we need to move the old dir?
  if(!is.na(saveToDir)) {
    file.rename(fromDir, saveToDir)
    message(sprintf("The old recordr directory is located at %s, \nwhich you may remove if desired.", saveToDir))
  } else {
    message(sprintf("The old recordr directory is located at %s, \nwhich you may remove if desired.", fromDir))
  }
    
  if(!file.exists(recordrDir)) dir.create(recordrDir, recursive = T)
  # Can't make the link until the linked to dir exists.
  if(makeLink) file.symlink(recordrDir, attr(currentDir, "default"))
  return(recordrDir)
}

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
#' @param log A character string. If .console=TRUE, the file to log console commands to. The default is 'console.log'.

#' @return execution identifier that uniquely identifies this recorded session
#' @examples 
#' \dontrun{
#' rc <- new("Recordr")
#' startRecord(rc, tag="my first console run")
#' x <- read.csv(file="./test.csv")
#' runIdentifier <- endRecord(rc)
#' }
setMethod("startRecord", signature("Recordr"), function(recordr, tag=as.character(NA), .file=as.character(NA), .console=TRUE,
                                                        log=as.character(NA)) {
  
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
  #assign(".recordrEnv", list2env(list(.recordr=TRUE)), envir=globalenv(), inherits=FALSE)
  env <- globalenv()
  env$.recordrEnv <- new.env()
  env$.recordrEnv$.recordr <- TRUE
  #assign(".recordrEnv", list2env(list(.recordr=TRUE)), envir=globalenv(), inherits=FALSE)
  # Put a copy of the recordr object itself info the recordr environment, in case
  # to the overriding functions need any information that it contains, such as the db connection
  # Note: The recordr object contains the db connection that has to be open in the Recordr class
  # initialization because it also needs to be available to functions like listRuns() that
  # operate outside the startRecord -> endRecord execution.
  #recordrEnv <- findEnv("recordrEnv")
  recordrEnv <- as.environment(get(".recordrEnv", envir=globalenv()))
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
      recordrEnv$scriptPath <- normalizePath(.file, mustWork=TRUE)
    }
  }
  
  if(.console) {
    if(!is.na(log)) {
      recordrEnv$log <- basename(log)
    } else {
      recordrEnv$log <- "console.log"
    }
    recordrEnv$execMeta <- new("ExecMetadata", programName=recordrEnv$log, tag=tag)
  } else {
    recordrEnv$execMeta <- new("ExecMetadata", programName=recordrEnv$scriptPath, tag=tag)
  }
  
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
  
  # trace DataONE versioj 2.0 functions
  # Trace calls specifying 'exit=' so that any error checking that the traced call
  # performs will get done before the tracer routine is called. This works well unless the
  # traced call itself calls 'on.exit', in which case we have to set a recordr 'deferred'
  # trace, as our only option is to trace at the beginning of the call, so the file may not
  # be created yet, for calls that create files, and we cannot get info or orchive the file.
  # Deferred traces are done during 'endRecord', when the file should have been created by.
  traceOn <- getOption("traceOn")
  if(is.null(traceOn)) traceOn = TRUE
  tracingState(on=traceOn)
  traceVerbose=getOption("traceVerbose")
  if(is.null(traceVerbose)) traceVerbose=FALSE
  if(requireNamespace("dataone")) {
    if(!is.element("package:dataone", search())) env <- attachNamespace("dataone")
    #suppressMessages(trace("getObject", exit=recordr::recordr_getObject, where=getNamespace("dataone")))
    #suppressMessages(trace("createObject", exit=recordr::recordr_createObject, where=getNamespace("dataone")))
    #suppressMessages(trace("updateObject", exit=recordr::recordr_updateObject, where=getNamespace("dataone")))
    trace("getObject", exit=recordr::recordr_getObject, print=traceVerbose, where=globalenv())
    trace("createObject", exit=recordr::recordr_createObject, print=traceVerbose, where=globalenv())
    trace("updateObject", exit=recordr::recordr_updateObject, print=traceVerbose, where=globalenv())
  }
  
  # trace R functions
  if(requireNamespace("utils")) {
    if(!is.element("package:utils", search())) env <- attachNamespace("utils")
    #trace("read.csv", tracer=recordr::recordr_read.csv, print=TRUE, where=parent.env(environment()))
    # works only with utils::read.csv
    trace("read.csv", tracer=recordr::recordr_read.csv, print=traceVerbose, where=globalenv())
    # works with utils::read.csv
    #trace("read.csv", tracer=recordr::recordr_read.csv, print=TRUE, where=getNamespace("utils"))
    # works with utils::read.csv only
    #trace(read.csv, tracer=recordr::recordr_read.csv, print=TRUE)
    trace("write.csv", exit=recordr::recordr_write.csv, print=traceVerbose, where=globalenv())
  }
  #suppressMessages(trace(base::scan, exit=recordr::recordr_scan))
  #suppressMessages(trace(base::readLines, exit=recordr::recordr_readLines))
  #trace(base::writeLines, exit=recordr::recordr_writeLines)
  if(requireNamespace("ggplot2", quietly=TRUE)) {
    if(!is.element("package:ggplot2", search())) env <- attachNamespace("ggplot2")
    # ggsave() calls 'on.exit' call, so we can't set a tracer using the parameter `exit=`
    trace("ggsave", tracer=recordr::recordr_ggsave, print=traceVerbose, where=globalenv())
  }
  if(requireNamespace("png", quietly=TRUE)) {
    if(!is.element("package:png", search())) env <- attachNamespace("png")
    trace("readPNG", exit=recordr::recordr_readPNG, print=traceVerbose, where=globalenv())
    #trace("writePNG", exit=recordr::recordr_writePNG, where=getNamespace("png"))
    trace("writePNG", exit=recordr::recordr_writePNG, print=traceVerbose, where=globalenv())
  }
  #if(requireNamespace("raster", quietly=TRUE)) {
  #  env <- attachNamespace("raster")
  #  trace("raster", exit=recordr::recordr_raster, where=globalenv())
  #  trace("writeRaster", exit=recordr::recordr_writeRaster, where=globalenv())
  #}
  if(requireNamespace("rgdal", quietly=TRUE)) {
    if(!is.element("package:rgdal", search())) env <- attachNamespace("rgdal")
    trace("readOGR", exit=recordr::recordr_readOGR, print=traceVerbose, where=globalenv())
    trace("writeOGR", exit=recordr::recordr_writeOGR, print=traceVerbose, where=globalenv())
  }
  
  # Create the run metadata directory for this execution
  runDir <- getRunDir(recordr, recordrEnv$execMeta@executionId)
  dir.create(runDir, recursive = TRUE)
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
    # Calling 'timestamp' from batch mode on windows causes R to hang. Nice!
    # The result of this is that recordr will not be able to mark the history file
    # and include the relevant history in a console log with the script.
    if(.Platform$OS.type != "windows") {
      timestamp (stamp = c(date(), startMarker), quiet = TRUE)
    }
  }
  setProvCapture(TRUE)
  tracingState(on=TRUE)
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
  # Pause any prov tracing of any functio
  tracingState(on=FALSE)
  on.exit(recordrShutdown())
  # Check if a recording session is active
  if (!exists(".recordrEnv", where = globalenv(), inherits = FALSE )) {
    message("A Recordr session is not currently active.")
    return(NULL)
  }
  #recordrEnv <- as.environment(".recordr")
  recordrEnv <- as.environment(get(".recordrEnv", envir=globalenv()))
  runDir <- getRunDir(recordr, recordrEnv$execMeta@executionId)
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
    # Calling 'timestamp' from batch mode on windows causes R to hang. Nice!
    # The result of this is that recordr will not be able to mark the history file
    # and include the relevant history in a console log with the script.
    if(.Platform$OS.type != "windows") {
      timestamp (stamp = c(date(), endMarker), quiet = TRUE)
    }
    tmpFile <- normalizePath(tempfile(pattern = "file", tmpdir = tempdir(), fileext = ".log"), 
                             mustWork=FALSE)
    savehistory(file=tmpFile)
    # Sometimes windows doesn't actually write out the history, so create an empty file.
    if(!file.exists(tmpFile)) {
      writeLines(c(startMarker, "Saving history not supported on windows.", endMarker), tmpFile)
    }
    recordrHistory <- ""
    allHistory <- ""
    foundStart <- FALSE
    foundEnd <- FALSE
    # If recording console input, the default name will be "console.log". The user can
    # override this using the 'log' parameter. The scriptPath will be set to this value,
    # so that the commands typed in by the user are saved to this file.
    consoleLogFile <- file.path(runDir, recordrEnv$log)
    recordrEnv$scriptPath <- consoleLogFile
    
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
        # Maybe didn't find both beginning and end markers, but the
        # history file is non-empty.
        recordrHistory <- c(headerLine, recordrHistory)
        writeLines(recordrHistory, consoleLogFile)
    } else {
        allHistory <- c(headerLine, recordrHistory)
        writeLines(recordrHistory, consoleLogFile)
    }
  }
  
  # Archive the script that was executed, or the console log if we
  # were recording console commands. The script can be retrieved
  # by searching for access="execute"
  archivedFilePath <- normalizePath(archiveFile(file=recordrEnv$scriptPath, force=TRUE), mustWork=FALSE)
  fpInfo <- file.info(recordrEnv$scriptPath)
  if (is.null(fpInfo[["uname"]])) fpInfo[["uname"]] <- as.character(NA)
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
  scriptFmt <- "application/R"
  programD1Obj <- new("DataObject", id=recordrEnv$programId, dataobj=script, format=scriptFmt, user=recordrEnv$execMeta@user, mnNodeId=recordrEnv$mnNodeId)    
  # TODO: Set access control on the action object to be public
  addMember(recordrEnv$dataPkg, programD1Obj)
  # Save the package relationships to disk so that we can recreate this package
  # at a later date.
  provRels <- getRelationships(recordrEnv$dataPkg)
  #filePath <- normalizePath(file.path(runDir, paste0(
  # cleanFilename(recordrEnv$execMeta@datapackageId), ".csv")), mustWork=FALSE)
  #write.csv(provRels, file=filePath, row.names=FALSE)
  
  if(nrow(provRels) > 0) {
    for (iRel in 1:nrow(provRels)) {
      # Convert the provenance relationships to a standard format and save to the 'provrels' db table
      provRelsObj <- new("ProvRels",  executionId = recordrEnv$execMeta@executionId, 
                         subject = provRels[iRel, "subject"],
                         predicate = provRels[iRel, "predicate"], 
                         object = provRels[iRel, "object"],
                         subjectType = provRels[iRel, "subjectType"], 
                         objectType = provRels[iRel, "objectType"],
                         dataTypeURI = provRels[iRel, "dataTypeURI"])
      writeProvRel(recordr, provRelsObj)
    }
  }
  
  # Save execution metadata to the recordr database
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
  # Ensure that the .recordr environment is detached from the search path.
  on.exit(recordrShutdown())
  
  # Execute the script specified by the user, making sure to catch any error encountered.
  result = tryCatch ({
    if (exists(".recordrEnv", where = globalenv(), inherits = FALSE )) {
      rm(globalenv()$.recordrEnv)
    }
    if(!file.exists(file)) {
      stop(sprintf("Error, file \"%s\" does not exist\n", file))
    }
    file <- normalizePath(file, mustWork=TRUE)
    execId <- startRecord(recordr, tag, .file=file, .console=FALSE)
    #recordrEnv <- as.environment(".recordr")
    recordrEnv <- as.environment(get(".recordrEnv", envir=globalenv()))
    setProvCapture(TRUE)
    # Source the user's script, passing in arguments that they intended for the 'source' call.  
    # Because we are calling the 'source' function with the packageId, the overridden function
    # for 'source' will not be called, and a provenance entry for this 'source' will not be
    # recorded.
    # Note: ellipse argument is passed from method call so user can pass args to source, if desired.
    # Check the warning level so that the user's script isn't terminated early due to 
    # a warning - i.e. because we are in a tryCatch, a warning in the user's script will cause
    # the script to terminate as soon as a warning is encountered. If the user warn level is set
    # to 1 or lower, disable this behavour for the user's script, to allow the same
    # warning behaviour that the user would have if they sourced the script manually.. 
    if(getOption("warn") < 2) {
      suppressWarnings(base::source(file, ...))
    } else {
      base::source(file, ...)
    }
  }, warning = function(warningCond) {
    if(exists("recordrEnv") && !is.na(recordrEnv$execMeta@executionId)) {
      slot(recordrEnv$execMeta, "errorMessage") <- warningCond$message
    }
    cat(sprintf("Warning:: %s\n", warningCond$message))
  }, error = function(errorCond) {
    slot(recordrEnv$execMeta, "errorMessage") <- errorCond$message
  }, finally = {
    # Disable provenance capture while some housekeeping is done
    setProvCapture(FALSE)
    # Stop recording provenance and finalize the data package. If the
    # recordr environment wasn't setup properly, then we won't
    # be able to properly end recording.
    if (is.element(".recordr", base::search())) {
      endRecord(recordr)
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
#' @param quiet A \code{logical} if TRUE then output is not printed. Useful if only the return value is desired.
#' @return A data.frame containing execution metadata for the runs that were deleted.
setMethod("deleteRuns", signature("Recordr"), function(recordr, id = as.character(NA), file = as.character(NA), 
                                                       start = as.character(NA), end = as.character(NA), 
                                                       tag = as.character(NA), error = as.character(NA), 
                                                       seq = as.integer(NA), noop = FALSE, quiet=FALSE) {

  runs <- selectRuns(recordr, runId=id, script=file, startTime=start, endTime=end, tag=tag, errorMessage=error, seq=seq, delete=TRUE)
  # selectRuns returns a list of ExecMeta objects
  if (length(runs) == 0) {
    message(sprintf("No runs matched search criteria."))
    return(invisible(runs))
  } else {
    if (noop) {
      if(!quiet) message(sprintf("The following %d runs would have been deleted:\n", length(runs)))
    } else {
      if (!quiet) message(sprintf("The following %d runs will be deleted:\n", length(runs)))
    }
  }
  
  if(!quiet) printRun(headerOnly=TRUE)
  
  # Loop through selected runs
  for(i in 1:length(runs)) {
    row <- runs[[i]]
    thisRunId <- row@executionId
    thisRunDir <- normalizePath(file.path(recordr@recordrDir, "runs", cleanFilename(thisRunId)), mustWork=FALSE)
    if (!noop) {
      if(thisRunDir == normalizePath(file.path(recordr@recordrDir, "runs"), mustWork=FALSE) || thisRunDir == "") {
        stop(sprintf("Error determining directory to remove, directory: %s", thisRunDir))
      }
      if(!is.null(thisRunDir) && !is.na(thisRunDir) && thisRunDir != "") unlink(thisRunDir, recursive = TRUE)
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
    if (!quiet) printRun(row)
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
#' @param quiet A \code{logical}, if TRUE then output is not printed to the console. Default is FALSE.
#' @param full A \code{logical}, if TRUE then all output columns are printed, regardless of console width.
#' @return data frame containing information for each run
#' @examples \dontrun{
#' rc <- new("Recordr")
#' # List runs that started in January 2015
#' listRuns(rc, start=c("2015-01-01", "2015-01-31"))
#' # List runs that started on or after March 1, 2014
#' listruns(rc, start="2014-03-01")
#' # List runs that contain a tag with the string "analysis v1.3")
#' listRuns(rc, tag="analysis v1.3")
#' }
#
setMethod("listRuns", signature("Recordr"), function(recordr, id=as.character(NA), script=as.character(NA), start = as.character(NA), end=as.character(NA), tag=as.character(NA), 
                                                     error=as.character(NA), seq=as.character(NA), orderBy = "-startTime", quiet=FALSE, full=FALSE) {
  
  runs <- selectRuns(recordr, runId=id, script=script, startTime=start, endTime=end, tag=tag, errorMessage=error, seq=as.character(seq), orderBy=orderBy)
  if (length(runs) == 0) {
    message(sprintf("No runs matched search criteria."))
    return(invisible(runs))
  }
  
  # Print header line
  if (!quiet) printRun(headerOnly = TRUE, full=full)
  # Loop through selected runs
  if(!quiet) {
  for(i in 1:length(runs)) {
    printRun(runs[[i]], full=full)
  }
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

printRun <- function(run=NA, headerOnly = FALSE, full=FALSE)  {
  
  tagLength = 20
  scriptNameLength = 30
  errorMsgLength = 30
  consoleWidth <- getOption("width")
  if(full) {
    # fields widths: 6 + 30 + 20 + 23 + 23 + 13 + 19 + 30 
    # fields: "Seq", "Script", "Tag", "Start Time", "End Time", "Run Id", "Published Time", "Error Message"), sep = " ")
    listingWidth <- "wide"
    fmt <- paste("%-6s", "%-", sprintf("%2d", scriptNameLength), "s",  " %-", sprintf("%2d", tagLength), "s",
                 " %-23s %-23s %-13s %-19s", " %-", sprintf("%2d", errorMsgLength), "s", "\n", sep="")
    headerLine <- sprintf(fmt, "Seq", "Script", "Tag", "Start Time", "End Time", "Run Id", "Published Time", "Error Message")
  } else if (is.null(consoleWidth)) {
    # Screen width <= 80 (80 is as narrow as we go)
    # fields widths: 6 + 30 + 20 + 23
    # fields: "Seq", "Script", "Tag", "Start Time"
    listingWidth <- "narrow"
    fmt <- paste("%-6s", "%-", sprintf("%2d", scriptNameLength), "s",  " %-", sprintf("%2d", tagLength), "s",
                 " %-23s ", "\n", sep="")
    headerLine <- sprintf(fmt, "Seq", "Script", "Tag", "Start Time")
  } else if (consoleWidth >= 158) {
    # Screen width >= 158 
    # fields widths: 6 + 30 + 20 + 23 + 23 + 13 + 19 + 30 
    # fields: "Seq", "Script", "Tag", "Start Time", "End Time", "Run Id", "Published Time", "Error Message"), sep = " ")
    listingWidth <- "wide"
    fmt <- paste("%-6s", "%-", sprintf("%2d", scriptNameLength), "s",  " %-", sprintf("%2d", tagLength), "s",
                 " %-23s %-23s %-13s %-19s", " %-", sprintf("%2d", errorMsgLength), "s", "\n", sep="")
    headerLine <- sprintf(fmt, "Seq", "Script", "Tag", "Start Time", "End Time", "Run Id", "Published Time", "Error Message")
  } else if (consoleWidth >= 115) {
    # Screen width >= 120, <= 158
    # fields widths: 6 + 30 + 20 + 23 + 23 + 13 = 115
    # fields: "Seq", "Script", "Tag", "Start Time", "End Time", "Run Id"
    listingWidth <- "medium"
    fmt <- paste("%-6s", "%-", sprintf("%2d", scriptNameLength), "s",  " %-", sprintf("%2d", tagLength), "s",
                 " %-23s %-23s %-13s ", "\n", sep="")
    headerLine <- sprintf(fmt, "Seq", "Script", "Tag", "Start Time", "End Time", "Run Id")
  } else {
    # Screen width < 115 
    # fields widths: 6 + 30 + 20 + 23 = 79
    # fields: "Seq", "Script", "Tag", "Start Time"
    listingWidth <- "narrow"
    fmt <- paste("%-6s", "%-", sprintf("%2d", scriptNameLength), "s",  " %-", sprintf("%2d", tagLength), "s",
                 " %-23s ", "\n", sep="")
    headerLine <- sprintf(fmt, "Seq", "Script", "Tag", "Start Time")
  }
  
  #fmt <- "%-20s %-20s %-19s %-19s %-36s %-36s %-19s %-30s\n"
  #fmt <- paste("%-6s", "%-", sprintf("%2d", scriptNameLength), "s",  " %-", sprintf("%2d", tagLength), "s",
               #" %-19s %-19s %-13s %-19s", " %-", sprintf("%2d", errorMsgLength), "s", "\n", sep="")
  
  # Padding for blank values for seq & script name
  paddingLength <- 6 + scriptNameLength 
  padding <- paste(character(paddingLength), collapse=" ") 
  fmtSecondary <-  paste("%-", sprintf("%2d", paddingLength), "s", 
                     " %-", sprintf("%2d", tagLength), "s", "\n", sep="")
  
  # Print only the column headings
  if (headerOnly) {
    cat(headerLine)
    # Print additional lines, i.e. column values from child tables
  } else {
    console <- run@console
    if(console) {
      thisScript <- condenseStr(sprintf("Console log: %s", basename(run@softwareApplication)), scriptNameLength)
    } else {
      thisScript <- condenseStr(run@softwareApplication, scriptNameLength)
      # Print shortened form of script name, e.g. "/home/slaugh...ocalReadWrite.R"
      thisScript <- condenseStr(run@softwareApplication, scriptNameLength)
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
    if(listingWidth == "narrow") {
      cat(sprintf(fmt, thisSeq, strtrim(thisScript, scriptNameLength), strtrim(thisTag, tagLength), thisStartTime), sep = " ")
    } else if (listingWidth == "medium") {
      cat(sprintf(fmt, thisSeq, strtrim(thisScript, scriptNameLength), strtrim(thisTag, tagLength), thisStartTime, 
                thisEndTime, thisRunId), sep = " ")
    } else if (listingWidth == "wide") {
      cat(sprintf(fmt, thisSeq, strtrim(thisScript, scriptNameLength), strtrim(thisTag, tagLength), thisStartTime, 
                thisEndTime, thisRunId, thisPublishTime, strtrim(thisErrorMessage, errorMsgLength)), sep = " ")
    }
    # Print additional tag values for this executionId, with only the tag displayed on the line (which is beneath the .
    # This is the same for all console widths
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
    thisRunDir <- normalizePath(file.path(recordr@recordrDir, "runs", cleanFilename(executionId)), mustWork=FALSE)
    
    # Read the RDF relationships that were saved to a file. For space considerations, the
    # entire data package is not serialized to disk. 
    #relations <- read.csv(normalizePath(file.path(thisRunDir, paste0(cleanFilename(datapackageId), ".csv"))), stringsAsFactors=FALSE)
    provRels <- new("ProvRels")
    relations <- readProvRels(recordr, executionId=thisRow@executionId)
    scriptURL <- relations[relations$predicate == "http://www.w3.org/ns/prov#hadPlan","object"]
    # Clear screen before showing results if we are paging the results
    if (i == 1 && page) cat("\014")
    # Print out [DETAILS] section
    scriptNameLength=70
    if (is.element("details", sections)) {
      cat(sprintf("[details]: Run details\n"))
      cat(sprintf("----------------------\n"))
      thisScript <- softwareApplication
      if(console) {
        cat(sprintf("Started recording console input at %s\n", startTime))
      } else {      
        cat(sprintf("%s was executed on %s\n", dQuote(condenseStr(thisScript, scriptNameLength)), startTime))
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
  
  executionId <- id
  if (!requireNamespace("dataone", quietly = TRUE)) {
    stop("Package \"dataone\" needed for function \"publishRun\" to work. Please install it.",
         call. = FALSE)
  }
  if (!requireNamespace("EML", quietly = TRUE)) {
    stop("Package EML needed for function \"publishRun\" to work. Please install it.",
         call. = FALSE)
  }
  if(is.na(executionId) && is.na(seq) ||
     !is.na(executionId) && !is.na(seq)) {
    stop("Please specify either \"seq\" or \"id\" parameter")
  }
  
  # readExecMeta returns a list of ExecMetadata objects
  if(!is.na(seq)) {
    thisExecMeta <- readExecMeta(recordr, seq=seq)
  } else if (!is.na(executionId)) {
    thisExecMeta <- readExecMeta(recordr, executionId=executionId)
  }
  
  if(length(thisExecMeta) == 0) {
      stop(sprintf("No exeuction found\n"))
  } else {
    # Get the first (and only) ExecMetadata object
    thisExecMeta <- thisExecMeta[[1]]
  }
  if(is.na(executionId)) executionId <- thisExecMeta@executionId
  runDir <- getRunDir(recordr, executionId)
  if (! file.exists(runDir)) {
    msg <- sprintf("A directory was not found for execution identifier: %s\n", executionId)
    stop(msg)
  }
  
  # See if this execution has been published before
  if (!is.na(thisExecMeta@publishTime)) {
    if(update) {
      if(!quiet) {
         msg <- sprintf("The datapackage for this execution was published on %s\nbut because 'update' was specified this package will be republished now\n", thisExecMeta@publishTime)
      }
    } else {
      msg <- sprintf("The datapackage for this execution was published on %s\n", thisExecMeta@publishTime)
      stop(msg)
    }
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
  d1c <- dataone::D1Client(d1Env, mnId)
  if (is.null(d1c@mn)) {
    stop(sprintf("Unable to contact member node \"%s\".\nUnable to publish run.\n", mnId))
  }
  resolveURI <- sprintf("%s/resolve", d1c@cn@endpoint)
  
  # Check the user's DataONE authentication.
  am <- dataone::AuthenticationManager()
  if(!dataone:::isAuthValid(am, d1c@mn)) {
    msg <- sprintf("Please see \"DataONE Authentication\" in \"intro_recordr\" vignette.")
    msg <- sprintf("%sEnter this command at the R console: \"vignette(\"intro_recordr\")", msg)
    stop(msg)
  }
  
  subject <- dataone:::getAuthSubject(am, d1c@mn)
  if (!quiet) cat(sprintf("Publishing execution %s to %s\n", executionId, mnId))
 
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
  metadataId <- thisExecMeta@metadataId
  emlObj <- getMetadata(recordr, id=executionId, as="EML")
  
  # Check options to see if the default DataONE submitter and rightsholder
  # should be overriden by user supplied values.
  rightsHolder <- getOption("rights_holder")
  submitter <- getOption("submitter")
  if(is.null(rightsHolder)) rightsHolder <- as.character(NA)
  if(is.null(submitter)) submitter <- as.character(NA)
  
  # Upload each data object that was used or geneated by the datapackage
  if(!quiet) cat(sprintf("Getting file info for execution %s\n", executionId))
  files <- readFileMeta(recordr, executionId=executionId)
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
      if(!quiet) cat(sprintf("Adding science object with id: %s, file: %s\n", 
                             getIdentifier(sciObj), basename(thisFile[['filePath']])))
      addMember(pkg, sciObj)
      # Now update the metadata object corresponding to this dataset in order to set the
      # Online distribution value so that MetacatUI can properly identify and display this item.
      # The 'additionalInfo' value was stored during endRecord() so that we could match up the
      # file and the eml element after the run was finished.
      for(iEntity in 1:length(emlObj@dataset@otherEntity)) {
        thisDatasetId <- emlObj@dataset@otherEntity[[iEntity]]@alternateIdentifier[[1]]@character
        if(fileId == thisDatasetId) {
          url <- sprintf("%s/%s", resolveURI, fileId)
          distrib <- new("distribution", online = new("online", url=url))
          emlObj@dataset@otherEntity[[iEntity]]@physical[[1]]@distribution <- as(list(distrib), "ListOfdistribution")
        }
      }
    }
  }
  
  # Now that we have used the alternate identifier, blank it out so thatEntityGroup the uploaded EML won't have it. Currently
  # the EML parser in Metacat doesn't allow alternate identifiers.
  if(length(emlObj@dataset@otherEntity) > 0) {
    for(iEntity in 1:length(emlObj@dataset@otherEntity)) {
      emlObj@dataset@otherEntity[[iEntity]]@alternateIdentifier <- new("ListOfalternateIdentifier")
    }
  }
  emlObj@dataset@pubDate <- as(publishDay, "pubDate")
  # Update the metadata stored for this run. The putMetadata() function
  # can't read eml objects yet, so have to write it to a file.
  tempMetadataFile <- tempfile()
  write_eml(emlObj, tempMetadataFile)
  putMetadata(recordr, id=executionId, metadata=tempMetadataFile, asText=FALSE)
  # Use windows friendly filenames, i.e. no ":"
  metaObj <- new("DataObject", id=metadataId, format=EML_211_FORMAT, mnNodeId=mnId, filename=tempMetadataFile, suggestedFilename="metadata.xml")
  if(!is.na(submitter)) metaObj@sysmeta@submitter <- submitter
  if(!is.na(rightsHolder)) metaObj@sysmeta@rightsHolder <- rightsHolder
  addMember(pkg, metaObj)
  # Now add the relationships between the science objects in the package and the metadata object
  # "metaObj documents sciObj"
  sciObjIds <- as.character(list())
  for (thisId in getIdentifiers(pkg)) {
    if(thisId == metadataId) next()
    sciObjIds <- c(sciObjIds, thisId)
  }
  
  if (length(sciObjIds) > 0) insertRelationship(pkg, metadataId, sciObjIds)
  
  #
  # Add the saved relationships back in the data package
  relationships <- readProvRels(recordr, executionId=executionId)
  if (nrow(relationships) > 0) {
    for(i in 1:nrow(relationships)) {
      thisRelationship <- relationships[i,]
      thisSubject <- thisRelationship[["subject"]]
      thisPredicate <- thisRelationship[["predicate"]]
      thisObject <- thisRelationship[["object"]]
      thisSubjectType <- thisRelationship[["subjectType"]]
      thisObjectType <- thisRelationship[["objectType"]]
      thisDataTypeURI <- thisRelationship[["dataTypeURI"]]
      pkg <- insertRelationship(pkg, subjectID=thisSubject, objectIDs=thisObject, predicate=thisPredicate, 
                         subjectType=thisSubjectType, objectTypes=thisObjectType, dataTypeURIs=thisDataTypeURI)
    }
  }
  
  if(!quiet) cat(sprintf("Uploading data package...\n"))
  # uploadDataPackage returns the "package pid" 'published identifier' for this datapackage. This will be displayed in the 
  # viewRuns() output for this under "Published ID".
  resourceMapId <- dataone::uploadDataPackage(d1c, pkg, replicate=replicationAllowed, numberReplicas=numberOfReplicas, 
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
#' @param as Form to return the metadata as. Possible values are: "text", "parsed" (for parsed XML), or "EML" (for an EML R package S4 object)
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
  runDir <- getRunDir(recordr, id)
  if (! file.exists(runDir)) {
    msg <- sprintf("A directory was not found for execution identifier: %s\n", id)
    stop(msg)
  }
  
  metadataFile <- sprintf("%s/%s.xml", runDir, cleanFilename(execMeta@metadataId))
  
  if(!file.exists(metadataFile)) {
    # Check if the metadata template file exists, if no, then copy the initial version
    # to the user's .recordr directory.
    metadataTemplateFile <- getOption("package_metadata_template_path")
    if(is.null(metadataTemplateFile) || !file.exists(metadataTemplateFile)) {
      metadataTemplateFile <- sprintf("%s/package_metadata_template.R", recordr@recordrDir)
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
    title <- as.character(NA)
    abstract <- as.character(NA)
    methodDescription <- as.character(NA)
    geo_coverage <- geoCoverage("global", west="-180", east="180", north="90", south="-90")
    currentYear <- format(Sys.Date(), "%Y")
    temp_coverage <- temporalCoverage(currentYear, currentYear)
    #mdfile <- sprintf("%s/metadata.R", path.expand("~"), recordr@recordrDir)
    success <- source(metadataTemplateFile, local=TRUE)
    # Set the identifier scheme to "uuid" for now, the user might specify "doi" during the
    # publish step.
    system <- "uuid"
    eml <- makeEML(recordr, id=id, system, title, creators, abstract, 
                   methodDescription, geo_coverage, temp_coverage)
    # Write the eml file to the execution directory
    eml_file <- sprintf("%s/%s.xml", runDir, cleanFilename(execMeta@metadataId))
    write_eml(eml, file = eml_file)
  } 
  
  if(as == "text") {
    metadata <- readLines(metadataFile, warn=FALSE)
    return(metadata)
  } else if (as == "parsed") {
    metadata <- readLines(metadataFile, warn=FALSE)
    return(xmlInternalTreeParse(metadata, asText=TRUE))
  } else if (as == "EML") {
    return(read_eml(metadataFile))
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
  runDir <- getRunDir(recordr, id)
  if (! file.exists(runDir)) {
    msg <- sprintf("A directory was not found for execution identifier: %s\n", id)
    stop(msg)
  }
  
  metadataId <- execMeta@metadataId
  metadataFile <- sprintf("%s/%s.xml", runDir, cleanFilename(metadataId))
  metadataFileBackup <- sprintf("%s.bak", metadataFile)
  
  # Either writeout the metadata to the run directory, or copy the user file to that location
  if(asText) {
    # Validate the EML
    result = tryCatch({
      tf <- tempfile()
      writeLines(metadata, tf)
      checkEML <- read_eml(tf)
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
  if (exists(".recordrEnv", where = globalenv(), inherits = FALSE )) {
    rm(".recordrEnv", envir=globalenv())
  }
  if(requireNamespace("dataone", quietly=TRUE)) {
    #untrace("createObject", where = getNamespace("dataone"))
    #untrace("getObject", where = getNamespace("dataone"))
    #untrace("updateObject", where = getNamespace("dataone"))
    untrace("createObject", where = globalenv())
    untrace("getObject", where = globalenv())
    untrace("updateObject", where = globalenv())
  }
  suppressMessages(untrace(base::readLines))
  untrace(base::writeLines)
  #suppressMessages(untrace(base::scan))
  untrace("read.csv")
  untrace("write.csv")
  if(requireNamespace("ggplot2", quietly=TRUE)) {
    untrace("ggsave")
  }
  if(requireNamespace("png", quietly=TRUE)) {
    untrace("readPNG")
    untrace("writePNG")
  }
  untrace("scan")
  if(isNamespaceLoaded("raster")) {
    untrace("raster")
    untrace("writeRaster")
  }
  if(requireNamespace("rgdal", quietly=TRUE)) {
    untrace("readOGR")
    untrace("writeOGR")
  }
  # TODO: save previous state (before record) and restore to that state
  tracingState(on=FALSE)
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
      #eg <- new("EntityGroup", entityName=basename(filePath), physical = phys, alternateIdentifier = as(list(altId), "ListOfalternateIdentifier"))
      #oe <- new("otherEntity", EntityGroup=eg, entityType=format)
      oe <- new("otherEntity", entityName=basename(filePath), entityType=format, physical = phys, alternateIdentifier = as(list(altId), "ListOfalternateIdentifier"))
      
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
  
  #titleObj <- new("title", value=title)
  #titleList <- as(list(titleObj), "ListOftitle")
  coverage <- coverageElement(geo_coverage, temp_coverage)
  #rg <- new("ResourceGroup", title = titleList, creator = as(creatorList, "ListOfcreator"), 
  #pubDate = as.character(Sys.Date()), abstract = abstract, coverage = coverage)
  
  # Create a contact for the dataset, use the first contact in the passed in data.frame
  individual <- new("individualName", givenName=creators[1, 'given'], surName=creators[1, 'surname'])
  individualList <- as(list(individual), "ListOfindividualName")
  contact <- new("contact", individual = individualList, electronicMailAddress = creators[1, 'email'])
  contactList <- as(list(contact), "ListOfcontact")
  
  if (!is.na(methodDescription)) {
    #ps <- new("proceduralStep", description=methodDescription)
    #ms <- new("methodStep", ProcedureStepType = ps)
    ms <- new("methodStep", description=methodDescription)
    loMethodStep <- new("ListOfmethodStep", list(ms))
    methods <- new("methods", methodStep=loMethodStep)
  }
  
  ds <- new("dataset",
            title = title,
            creator = as(creatorList, "ListOfcreator"),
            pubDate = as.character(Sys.Date()), 
            abstract = abstract, 
            coverage = coverage,
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
  dbDir <- normalizePath(dirname(dbFile), mustWork=FALSE)
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
    relFilePath <- fm[1,'archivedFilePath']
    frefs <- readFileMeta(recordr, sha256=checksum)
    if(nrow(frefs) == 1) {
      archivedFilePath <- sprintf("%s/%s", recordr@recordrDir, relFilePath)
      # One final sanity check before deleting file. Make sure we are only deleting files from
      # the recordr 'archive' directory
      if(nchar(relFilePath) > 0) {
        if(grepl("^archive/", relFilePath)) unlink(archivedFilePath, force=TRUE)
      }
    }
  }
  invisible(archivedFilePath)
}

# Get the execution specific working directory.
getRunDir <- function(recordr, id) {
    runDir <- normalizePath(file.path(recordr@recordrDir, "runs",  cleanFilename(id)), mustWork=FALSE)
    return(runDir)
}

# Remove characters that are illegal for certain operating systems, i.e. ":" for windows
cleanFilename <- function(filename) {
    gsub(":", "_", filename)
}

#' Trace processing lineage for a run and plot it.
#' @description A data processing workflow might include multiple processing steps, with
#' each step being performed by a separate R script. These multiple steps are linked by
#' the files that one step writes and the next step in the workflow reads. The \code{plotRuns}
#' method finds these connections between executions to determine the executions that
#' comprise a processing workflow.
#' @param recordr a Recordr instance
#' @param ... additional parameters
#' @seealso \code{\link[=Recordr-class]{Recordr}} { class description}
#' @export
setGeneric("plotRuns", function(recordr, ...) {
  standardGeneric("plotRuns")
})

#' @rdname plotRuns
#' @details If the run \code{id} or \code{seq} number is know for the run to be traced, then one or the
#' other of these values can be used. Alternatively, other run attributes can be used to determine the run to be traced,
#' such as \code{file}, \code{start}, etc. If these other search parameters are used and multiple runs are selected,
#' only the first run selected will be traced. These search parameters can be used together to easily find certain runs, 
#' for example, the latest run of a particular script, the latest run with a specified tag specified, etc. (see examples).
#' @param id The identifier for a run. Either \code{id} or \code{seq} can be specified, not both.
#' @param seq The sequence number for a run.
#' #' @param id The execution identifier of a run to view
#' @param file The name of script to match 
#' @param start Match runs that started in this time range (inclusive)
#' Times must be entered in the form 'YYYY-MM-DD HH:MM:SS' but can be shortened to not less that "YYYY"
#' @param end Match runs that ended in this time range (inclusive)
#' Times must be entered in the form 'YYYY-MM-DD HH:MM:SS' but can be shortened to not less that "YYYY"
#' @param tag The text of tag to match 
#' @param error The text of error message to match. 
#' @param orderBy Sort the results according to the specified column. A hypen ('-') prepended to the column name 
#' denoes a descending sort. The default value is "-startTime"
#' @param direction The direction to trace the lineage, either \code{fowward}, \code{backward}, or \code{both}. 
#'                  The default is \code{both}
#' @param quiet A \code{logical} if TRUE then output is not printed. 
#' @return A list of the execution identifiers that are in the processing workflow.
#' @export
#' @examples 
#' \dontrun{
#' # Plot processing workflow for the run with sequence number '101'
#' plotRuns(recordr, seq=101)
#' # Plot processing workflow for the last execution of script "runModel.R"
#' plotRuns(recordr, file="runModel.R", orderBy="-startTime")
#' # Plot processing workflow for the last execution with the tag 'best run yet!' specified.
#' plotRuns(recordr, tag="best run yet!", orderBy="-startTime")
#' }
setMethod("plotRuns", signature("Recordr"), function(recordr, id=as.character(NA), file=as.character(NA), 
                                                     start=as.character(NA), end=as.character(NA), tag=as.character(NA), error=as.character(NA),
                                                     seq=as.character(NA), orderBy="-startTime", 
                                                     direction="both", quiet=TRUE, ...) {
  
  if (!requireNamespace("DiagrammeR", quietly = TRUE)) {
    stop("Package \"DiagrammeR\" is needed for function \"plotRuns\" to work. Please install it.",
         call. = FALSE)
  }
  # traceRuns returns a list of ExecMetadata objects based on the user's search parameters.
  # The user can search for a run by using any run attribute, but only the first run returned will be traced.
  # The user can specify a sort order to control which run is first, for example, the latest run of a particular
  # script could be selected.
  retVals <- traceRuns(recordr, id=id, file=file, start=start, end=end, tag=tag, error=error, seq=seq, 
                       orderBy=orderBy, direction=direction, quiet=quiet)
  
  linkedIds <- retVals[[1]]
  execMetas <- retVals[[2]]
  usedFiles <- retVals[[3]]
  genFiles <- retVals[[4]]
  nodes <- hash()
  
  # DiagrammeR has changed it's API in version 0.9.0, so it is no longer possible
  # to specify the node id for a node. It is therefor required to keep a lookup
  # table between the node values that DiagrammeR gives a node and the corresponding
  # value for the recordr object, i.e. executionId.
  idsToDgrmR <- hash()
  
  graph <- DiagrammeR::create_graph()
  graph <- DiagrammeR::set_global_graph_attrs(graph, attr = "layout", value = "dot", attr_type="graph") 
  graph <- DiagrammeR::set_global_graph_attrs(graph, attr = "fontname", value = "Helvetica", attr_type="node")
  graph <- DiagrammeR::set_global_graph_attrs(graph, attr = "color", value = "gray20", attr_type="edge")
  
  # Loop through the list of executions, adding nodes for each execution
  for (execId in keys(linkedIds)) {
    em <- execMetas[[execId]]
    ufs <- usedFiles[[execId]]
    gfs <- genFiles[[execId]]
    if(em@softwareApplication == "") {
      execLabel <- execId
    } else {
      execLabel <- basename(em@softwareApplication)
    }
    # Add node if it hasn't been added to the lookup table or it hasn't been added to the graph.
    if(!has.key(execId, idsToDgrmR) || !DiagrammeR::node_present(graph, node=idsToDgrmR[[execId]])) {
      graph <- add_node_with_id(graph, id=execId, label=execLabel, idLookup=idsToDgrmR)
      graph <- set_node_attrs(graph, node_attr= "shape", values="rectangle", nodes=idsToDgrmR[[execId]])
    }
    # Create nodes and links for input files
    if(nrow(ufs) > 0) {
      for(iFile in 1:nrow(ufs)) {
        fileName <- basename(ufs[iFile, 'filePath'])
        sha256 <- ufs[iFile, 'sha256']
        ctime <- ufs[iFile, 'createTime']
        fileKey <- sprintf("%s-%s", sha256, ctime)
        # Has a node in the graph been created for this file already?
        if(!has.key(fileKey, nodes)) {
          graph <- add_node_with_id(graph, id=fileKey, label=fileName, idLookup=idsToDgrmR)
          nodes[[fileKey]] <- TRUE
        }
        if(DiagrammeR::edge_present(graph, from=idsToDgrmR[[fileKey]], to=idsToDgrmR[[execId]])) {
          graph <- add_edge_with_ids(graph, from=fileKey, to=execId, idLookup=idsToDgrmR)
        }
      }
    }
    # Create nodes and links for the output files
    if(nrow(gfs) > 0) {
      for(iFile in 1:nrow(gfs)) {
        fileName <- basename(gfs[iFile, 'filePath'])
        sha256 <- gfs[iFile, 'sha256']
        ctime <- gfs[iFile, 'createTime']
        fileKey <- sprintf("%s-%s", sha256, ctime)
        # Has a node in the graph been created fo
        # Has a node in the graph been created for this file already?
        if(!has.key(fileKey, nodes)) {
          graph <- add_node_with_id(graph, id=fileKey, label=fileName, idLookup=idsToDgrmR)
          graph <- DiagrammeR::set_node_attrs(graph, node_attr= "shape", values="ellipse", nodes=idsToDgrmR[[fileKey]])
          nodes[[fileKey]] <- TRUE
        } 
        if(!DiagrammeR::edge_present(graph, from=idsToDgrmR[[fileKey]], to=idsToDgrmR[[execId]])) {
          graph <- add_edge_with_ids(graph, from=execId, to=fileKey, idLookup=idsToDgrmR)
        }
      }
    }
  }
  
  # Render the graph using GraphViz. Out is sent to the RStudio viewer and
  # can be exported using the RStudio viewer panel. Other output options will
  # be added.
  DiagrammeR::render_graph(graph)
})

#' Trace processing lineage by finding related executions
#' @description A data processing workflow might include multiple processing steps, with
#' each step being performed by a separate R script. These multiple steps are linked by
#' the files that one step writes and the next step in the workflow reads. The \code{traceRuns}
#' method finds these connections between executions to determine the executions that
#' comprise a processing workflow, and returns information for each run in the processing workflow
#' including all files that were read and written by each script.
#' @param recordr a Recordr instance
#' @param ... additional parameters
#' @seealso \code{\link[=Recordr-class]{Recordr}} { class description}
#' @export
setGeneric("traceRuns", function(recordr, ...) {
  standardGeneric("traceRuns")
})

#' @rdname traceRuns
#' @details If the run \code{id} or \code{seq} number is know for the run to be traced, then one or the
#' other of these values can be used. Alternatively, other run attributes can be used to determine the run to be traced,
#' such as \code{file}, \code{start}, etc. If these other search parameters are used and multiple runs are selected,
#' only the first run selected will be traced. These search parameters can be used together to easily find certain runs, 
#' for example, the latest run of a particular script, the latest run with a specified tag specified, etc. (see examples).
#' @param id The identifier for a run. Either \code{id} or \code{seq} can be specified, not both.
#' @param seq The sequence number for a run.
#' #' @param id The execution identifier of a run to view
#' @param file The name of script to match 
#' @param start Match runs that started in this time range (inclusive)
#' Times must be entered in the form 'YYYY-MM-DD HH:MM:SS' but can be shortened to not less that "YYYY"
#' @param end Match runs that ended in this time range (inclusive)
#' Times must be entered in the form 'YYYY-MM-DD HH:MM:SS' but can be shortened to not less that "YYYY"
#' @param tag The text of tag to match 
#' @param error The text of error message to match. 
#' @param orderBy Sort the results according to the specified column. A hypen ('-') prepended to the column name 
#' denoes a descending sort. The default value is "-startTime"
#' @param direction The direction to trace the lineage, either \code{fowward}, \code{backward}, or \code{both}. 
#'                  The default is \code{both}
#' @param quiet A \code{logical} if TRUE then output is not printed. 
#' @return A list of the execution identifiers that are in the processing workflow.
#' @export
#' @examples 
#' \dontrun{
#' # Trace lineage for the run with sequence number '101'
#' linkedRuns <- traceRuns(recordr, seq=101)
#' # Trace lineage for the last execution of script "runModel.R"
#' linkedRuns <- traceRuns(recordr, file="runModel.R", orderBy="-startTime")
#' # Trace lineage for the last execution with the tag 'best run yet!' specified.
#' linkedRuns <- traceRuns(recordr, tag="best run yet!", orderBy="-startTime")
#' }
setMethod("traceRuns", signature("Recordr"), function(recordr, id=as.character(NA), file=as.character(NA), 
                                                      start=as.character(NA), end=as.character(NA), tag=as.character(NA), error=as.character(NA),
                                                      seq=as.character(NA), orderBy="-startTime", 
                                                      direction="both", quiet=TRUE, ...) {
  
  # selectRuns returns a list of ExecMetadata objects based on the user's search parameters.
  # The user can search for a run by using any run attribute, but only the first run returned will be traced.
  # The user can specify a sort order to control which run is first, for example, the latest run of a particular
  # script could be selected.
  runs <- selectRuns(recordr, runId=id, script=file, startTime=start, endTime=end, tag=tag, errorMessage=error, seq=seq, orderBy=orderBy)
  if (length(runs) == 0) {
    message(sprintf("No runs matched search criteria."))
    return(list())
  }
  
  # Loop through selected runs
  runToTrace <- runs[[1]]
  executionId <- runToTrace@executionId
  retVals <- traverseExecs(recordr, executionId=executionId, direction=direction)
  visitedIds <- retVals[[1]]
  linkedIds <- retVals[[2]]
  execMetas <- retVals[[3]]
  usedFiles <- retVals[[4]]
  genFiles <- retVals[[5]]
  invisible(list(linkedIds=linkedIds, execMetas=execMetas, usedFiles=usedFiles, genFiles=genFiles))
})

traverseExecs <- function(recordr, executionId, direction="both", visitedIds=hash(), 
                          linkedIds=hash(), execMetas=hash(), usedFiles=hash(), 
                          genFiles=hash(), quiet=TRUE, ...) {
  
  # Skip this execution if we have visited it before, i.e. there may be multiple files shared
  # between executions, so only traverse to an execution once.
  if(has.key(executionId, visitedIds)) {
    return(list(visitedIds=visitedIds, linkedIds=linkedIds, execMetas=execMetas, 
                usedFiles=usedFiles, genFiles=genFiles))
  }
  # Read metadata for this execution. The function readExecMeta returns a list.
  execMetaList <- readExecMeta(recordr, executionId=executionId)
  execMeta <- execMetaList[[1]]
  startTime <- as.POSIXct(execMeta@startTime)
  visitedIds[[executionId]] <- TRUE
  linkedIds[[executionId]] <- TRUE
  execMetas[[executionId]] <- execMeta
  haveReadFiles <- FALSE
  haveWriteFiles <- FALSE
  
  # Traverse backward.
  if(direction == "backward" || direction == "both") {
    # get all files that were read by an execution
    filesRead <- readFileMeta(recordr, executionId=executionId, access="read")
    usedFiles[[executionId]] <- filesRead
    haveReadFiles <- TRUE
    # Check each file that was read, for connections to ancestor executions
    if(nrow(filesRead) > 0) {
      for (irow in 1:nrow(filesRead)) {
        thisFile <- as.list(filesRead[irow,])
        # Get all file access entries for a file with this checksum
        nextExecs <- getLinkedExecs(recordr, fromFileAccess=thisFile, visitedIds=visitedIds, direction="backward")
        if (length(nextExecs) > 0) {
          for (iExec in 1:length(nextExecs)) {
            thisExec <- nextExecs[[iExec]]
            traverseExecs(recordr, executionId=thisExec@executionId, direction=direction, visitedIds=visitedIds, 
                          linkedIds=linkedIds,  execMetas=execMetas, usedFiles=usedFiles, genFiles=genFiles, 
                          quiet, ...)
          }
        }
      } 
    }
  }
  
  # Traverse forward
  if(direction == "forward" || direction == "both") {
    # get all files that were read by an execution
    filesWritten <- readFileMeta(recordr, executionId=executionId, access="write")
    # Store all files written by this execution id to return to the calling program.
    genFiles[[executionId]] <- filesWritten
    haveWriteFiles <- TRUE
    # For each file that was written by this execution, use the checksum to
    # lookup which executions read a file with the same checksum (i.e. one step
    # later in the lineage chain.).
    if(nrow(filesWritten) > 0) {
      for (irow in 1:nrow(filesWritten)) {
        thisFile <- as.list(filesWritten[irow,])
        # Get all file access entries for a file with this checksum
        nextExecs <- getLinkedExecs(recordr, fromFileAccess=thisFile, visitedIds=visitedIds, direction="forward")
        if (length(nextExecs) > 0) {
          for (iExec in 1:length(nextExecs)) {
            thisExec <- nextExecs[[iExec]]
            traverseExecs(recordr, executionId=thisExec@executionId, direction=direction, visitedIds=visitedIds, 
                          linkedIds=linkedIds,  execMetas=execMetas, usedFiles=usedFiles, genFiles=genFiles, 
                          quiet, ...)
          }
        }
      }
    }
  }
  
  # Read file metadata for all 'read' files for this execution, if they have not been read
  # already. The traversal may not have included any input links from this exec, so read them
  # now so that they will be available for display in plotRuns, etc.
  if(!haveReadFiles) {
    usedFiles[[executionId]] <- readFileMeta(recordr, executionId=executionId, access="read")
  }
  # Read file metadata for all 'written' files for this execution.
  if(!haveWriteFiles) {
    genFiles[[executionId]] <- readFileMeta(recordr, executionId=executionId, access="write")
  }
  return(list(visitedIds=visitedIds, linkedIds=linkedIds, execMetas=execMetas, 
              usedFiles=usedFiles, genFiles=genFiles))
}

getLinkedExecs <- function (recordr, fromFileAccess, visitedIds, direction=as.character(NA)) {
  linkedExecs <- list()
  
  if(is.na(direction)) {
    stop("A direction must be specified.")
  }
  if (direction=="backward") {
    fileWritesToCheck <- readFileMeta(recordr, sha256=fromFileAccess$sha256, access="write")
    if(nrow(fileWritesToCheck) > 0) {
      for (iFile in 1:nrow(fileWritesToCheck)) {
        # Don't check this execution if it has been checked previously, i.e. the execution
        # has multiple links to this file (shouldn't happen).
        thisFileAccess <- as.list(fileWritesToCheck[iFile,])
        # Get the execution info for the candidate execution to link to
        thisExecMeta <- readExecMeta(recordr, executionId=thisFileAccess$executionId)[[1]]
        # Skip this execution if we have already traversed to it
        if(has.key(thisExecMeta@executionId, visitedIds)) {
          next
        }
        # Are we trying to traverse to the same execution?
        if(thisFileAccess$executionId == fromFileAccess$executionId) {
          next
        }
        # Have we seen this softwareApplication or checksum before?
        
        # If the read and write access both accessed the same file, then the checksum
        # and the file creation time should match.
        # the read access, so they aren't the same file
        # TODO: use this only if in 'strict' mode
        # Last check 
        if(as.POSIXct(thisFileAccess$createTime) != as.POSIXct(fromFileAccess$createTime)) {
          next
        }
        # Passed all checks, add this exec to the list of execs to traverse to.
        linkedExecs[[length(linkedExecs)+1]] <- thisExecMeta
      }
    }
  } 
  # Get next exec ids in the forward direction
  
  if (direction=="forward") {
    fileReadsToCheck <- readFileMeta(recordr, sha256=fromFileAccess$sha256, access="read")
    if(nrow(fileReadsToCheck) > 0) {
      for (iFile in 1:nrow(fileReadsToCheck)) {
        # Don't check this execution if it has been checked previously, i.e. the execution
        # has multiple links to this file (shouldn't happen).
        thisFileAccess <- as.list(fileReadsToCheck[iFile,])
        # Get the execution info for the candidate execution to link to
        thisExecMeta <- readExecMeta(recordr, executionId=thisFileAccess$executionId)[[1]]
        # Skip this execution if we have already traversed to it
        if(has.key(thisExecMeta@executionId, visitedIds)) {
          next
        }
        # Are we trying to traverse to the same execution?
        if(thisFileAccess$executionId == fromFileAccess$executionId) {
          next
        }
        # Have we seen this softwareApplication or checksum before?
        
        # If the read and write access both accessed the same file, then the checksum
        # and the file creation time should match.
        # the read access, so they aren't the same file
        # TODO: use this only if in 'strict' mode
        # Last check
        if(as.POSIXct(thisFileAccess$createTime) != as.POSIXct(fromFileAccess$createTime)) {
          next
        }
        # Passed all checks, add this exec to the list of execs to traverse to.
        linkedExecs[[length(linkedExecs)+1]] <- thisExecMeta
      }
    }
  } 
  return(linkedExecs)
}

add_node_with_id <- function(graph, id, type=NULL, label=NULL, idLookup) {
  # DiagrammeR won't let you assign id values to a node, so you have
  # to see what id is given to a node, and then remember that so we
  # can map the DiagrammeR node ids to our fileIds
  orig_node_ids <- get_node_ids(graph)
  graph <- add_node(graph, type=type, label=label)
  new_node_ids <- get_node_ids(graph)
  node_id <- base::setdiff(new_node_ids, orig_node_ids)
  idLookup[[id]] <- node_id
  return(graph)
}

add_edge_with_ids <- function(graph, from, to, idLookup) {
  fromId <- idLookup[[from]]
  toId <- idLookup[[to]]
  graph <-add_edge(graph, from=fromId, to=toId)
  return(graph)
}