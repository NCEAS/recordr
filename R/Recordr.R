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

library(dataone)

#' Recordr provides methods for capturing, managing and publishing R processing provenance.
#' @slot recordrDir ion value of type \code{"character"}, containing a path to the Recordr working directory
#' @slot runDir value of type \code{"character"}, containing the path of the sub-directory that contains every run directory
#' @rdname Recordr-class
#' @author slaughter
#' @export
setClass("Recordr", slots = c(recordrDir = "character",
                              runDir = "character")
)
#########################
## Recordr constructors
#########################

#' @return the Recordr object
#' @author slaughter
#' @export
setGeneric("Recordr", function(x) {
  standardGeneric("Recordr")
})

setMethod("Recordr", signature(), function(x) {
  
  # Create a new Recordr object
  recordr <- new("Recordr")  
  recordr@recordrDir <- "~/.recordr"
  recordr@runDir <- sprintf("%s/runs", recordr@recordrDir)
  return(recordr)
})

##########################
## Methods
##########################

#' Record provenance for an R script execution and create a DataONE DataPackage that
#' contains this provenance information and all derived products create by the script
#' @param Recordr object
#' @param The filename of the R script to run and collect provenance information for
#' @return the identifier for the DataONE datapackge created by this run
#' @author slaughter
#' @export
setGeneric("record", function(recordr, filePath, ...) {
  standardGeneric("record")
})

#' @export
setMethod("record", signature("Recordr", "character"), function(recordr, filePath, ...) {

  execMeta <- ExecMetadata(filePath)
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
  
  # Remove the environment from the R search path. 
  on.exit(detach(".recordr"))
  assign("runDir", recordr@runDir, envir = as.environment(".recordr"))
  # Create an empty D1 datapackage object and make it globally available, i.e. available
  # to the masking functions.
  mnNodeId <- "urn:node:mnStageUCSB2"
  d1Client <- D1Client("STAGING", mnNodeId)
  d1Pkg <- new(Class="DataPackage", packageId=execMeta@datapackageId)
  
  # Build a D1Object that will contain the script we are recording
  currentTime <- format(Sys.time(), "%Y%m%d%H%M%s")
  programId <- paste("r_test_program", currentTime, "1", sep=".")
  script <- paste(readLines(filePath), collapse = '')
  scriptFmt <- "text/plain"
  programD1Obj <- new(Class="D1Object", programId, script, scriptFmt, mnNodeId)
  
  # Set access control on the action object to be public
  setPublicAccess(programD1Obj)
  # Create a metadata object
  metadata <- paste(readLines("~/.recordr/metadata.xml"), collapse = '')
  metadataFmt <- "eml://ecoinformatics.org/eml-2.1.1"
  
  ## Build a D1Object for the metadata, and upload it to the MN
  metadataId <- paste("r_test_mta", currentTime, "1", sep=".")
  metadataD1Obj <- new("D1Object", metadataId, metadata, metadataFmt, mnNodeId)
  addData(d1Pkg,programD1Obj)
  addData(d1Pkg,metadataD1Obj)
  ##insertRelationship(d1Pkg, metadataId, c(id.dat, id.result, programId))
  
  # Copy the client and data package to the ".recordr" environment so that they
  # will be available to the overriding functions, i.e. "recordr_write.csv"
  assign("d1Client", d1Client, envir = as.environment(".recordr"))
  assign("d1Pkg", d1Pkg, envir = as.environment(".recordr"))
  # Remove the original copies, as only the copies in the ".recordr" environment
  # will be used now.
  rm(d1Client)
  rm(d1Pkg)

  assign("scriptPath", filePath, envir = as.environment(".recordr"))
  assign("source", recordr::recordr_source, envir = as.environment(".recordr"))
  # override DataONE V1.1.0 methods
  assign("getD1Object", recordr::recordr_getD1Object, envir = as.environment(".recordr"))
  assign("createD1Object", recordr::recordr_createD1Object, envir = as.environment(".recordr"))
  # override R functions
  assign("read.table",  recordr::recordr_read.table,  envir = as.environment(".recordr"))
  assign("write.table", recordr::recordr_write.table, envir = as.environment(".recordr"))
  # Make a copy of the execution metadata object to our temp environment, so it is globally accessable
  # Warning: changes to this local object will NOT be made in the copy in env ".recordr"
  assign("execMeta", execMeta, envir = as.environment(".recordr"))
  
  # Create the run metadata directory for this record()
  dir.create(sprintf("%s/%s", recordr@runDir, execMeta@executionId), recursive = TRUE)
  file.create(sprintf("%s/%s/prov.txt", recordr@runDir, execMeta@executionId))
  
  # Source the user's script, passing in arguments that they intended for the 'source' call.
  setProvCapture(TRUE)
  base::source(filePath, ...)
  setProvCapture(FALSE)
  execMeta@endTime <- as.character(Sys.time())
  # return a datapackage object, but for now just return the id
  writeExecMeta(recordr, execMeta)
  d1Pkg <- get("d1Pkg", envir = as.environment(".recordr"))
  resourceMap <- d1Pkg@jDataPackage$serializePackage()
  write(resourceMap, file = sprintf("%s/%s/resourceMap.xml", recordr@runDir, execMeta@executionId))
  #return(execMeta@executionId)
  return(execMeta@datapackageId)
})

#' List all recorded runs
#' @param quiet if TRUE, don't print information to the script
## @returnType data frame containing the run information  
## 
## @author slaughter
#' @export
setGeneric("listRuns", function(recordr, quiet=FALSE) {
  standardGeneric("listRuns")
})

setMethod("listRuns", signature("Recordr"), function(recordr, quiet=FALSE) {
  # Find all run directories, i.e. (~/.recordr/runs/*)
  dirs <- list.files(recordr@runDir)
  runMeta <- data.frame()
  fmt <- "%-20s %-19s %-19s %-36s %-36s\n"
  # Loop through the run directories. The sub-directories are the name
  # of the executionId for that execution.
  if (! quiet) cat(sprintf(fmt, "Script", "StartTime", "EndTime", "Run Identifier", "Package Identifier"))
  for (d in dirs) {
      execMeta <- readExecMeta(recordr, d)
      if (! is.null(execMeta)) {
        emValues <- execMeta[["value"]]
        names(emValues) <- execMeta[["name"]]
        # TODO: get pubTime
        script <- emValues["softwareApplication"]
        startTime <-  emValues["startTime"]
        endTime <-  emValues["endTime"]
        execId <-  emValues["executionId"]
        packageId <-  emValues["datapackageId"]
        if (! quiet) cat(sprintf(fmt, script, startTime, endTime, execId, packageId))
        runMeta <- rbind(runMeta, c(script, startTime, endTime, execId, packageId))
      }
  }
  names(runMeta) <- c("Script", "StartTime", "EndTime", "Run Identifier", "Package Identifier")
  return(runMeta)
})

#' View the contents of a DataONE data package
#' @param identifier of the data package
## @returnType DataPackage  
## 
#' @author slaughter
#' @export
setGeneric("view", function(recordr, id) {
  standardGeneric("view")
})

setMethod("view", signature("Recordr"), function(recordr, id) {
  cat(sprintf("DataOne DataPackage\n"))
  cat(sprintf("===================\n"))
  cat(sprintf("Package identifier: %s\n", id))
  
  # Find the data package in the recordr run directories
  dirs <- list.files(recordr@runDir)
  for (d in dirs) {
    execMeta <- readExecMeta(recordr, d)
    if (! is.null(execMeta)) {
      emValues <- execMeta[["value"]]
      names(emValues) <- execMeta[["name"]]
      packageId <-  emValues["datapackageId"]
      if (id == packageId) {
        cat(sprintf("This package was created by run: %s\n", d))
        thisRunDir <- sprintf("%s/%s", recordr@runDir, d)
        # Print out the text file that contains the relationships
        # TODO: read prov relationships directly from the data package object
        #       that was read in
        provFile <- sprintf("%s/prov.txt", thisRunDir)
        if(file.exists(provFile)) {
          provData <- readLines(provFile)
          cat(sprintf("\nProvenance\n"))
          cat(sprintf("----------\n"))
          writeLines(provData)
        }
        break
      }
    }
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

setMethod("publish", signature("Recordr", "character", "MNode"), function(recordr, packageId, MNode) {
  print(paste("publishing package: ", packageId))
})