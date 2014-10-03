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

#if (!(require(uuid, character.only=T, quietly=T))) {
#  install.packages(uuid)
#  library(uuid, character.only=T)
#}

## A class representing a script execution run manager
#' @slot name (not currently used)
#' @author slaughter
#' @export
setClass("Recordr", slots = c(name = "character"))

#########################
## Recordr constructors
#########################

#' @param ... (not yet used)
#' @return the Recordr object
#' @author slaughter
#' @export
setGeneric("Recordr", function(x) {
  standardGeneric("Recordr")
})

setMethod("Recordr", signature("character"), function(x) {
  
  ## create new MNode object and insert uri endpoint
  recordr <- new("Recordr")
  print("Initializing run manager")
  
  return(recordr)
})

##########################
## Methods
##########################

#' Record provenance for an R script execution and create a DataONE DataPackage that
#' contains this provenance information and all derived products create by the script
#' 
#' @param Recordr object
#' @param The filename of the R script to run and collect provenance information for
#' @return the identifier for the DataONE datapackge created by this run
#' @author slaughter
#' @export
setGeneric("record", function(recordr, fileName, ...) {
  standardGeneric("record")
})

#' @export
setMethod("record", signature("Recordr", "character"), function(recordr, fileName, ...) {
  print(paste("record: sourcing file", fileName))
  
  # Create an environment on the search path that will store the overridden 
  # funnction names. These overridden functions are the ones that Recordr will
  # record provenance information for. This mechanism is similiar to a callback,
  # so that when the user script calls these functions, the Recordr version will be
  # called first, provenance relationships will be determined and recorded by Recordr,
  # then the Recordr version will call the native R function.
  # Using this mechanism, the DataONE methods
  # are only overridden while the record function is running, allowing the user to use DataONE
  # normally from their interactive R session, for example.
  attach(NULL, name=".recordr")
  
  # Remove the environment from the R search path. 
  on.exit(detach(".recordr"))
  
  # DataONE V1.1.0 methods
  assign("source", recordr::recordr_source, envir = as.environment(".recordr"))
  assign("getD1Object", recordr::recordr_getD1Object, envir = as.environment(".recordr"))
  assign("createD1Object", recordr::recordr_createD1Object, envir = as.environment(".recordr"))
  
  # DataONE V2 rdataone methods
  #assign("create", recordr::recordr_create, envir = as.environment(".recordr"))
  # Used only for testing
  #assign("getCapabilities", recordr::recordr_getCapabilities, envir = as.environment(".recordr"))
    
  # Define variables in .recordr environment so they are available globally
  assign("runId", UUIDgenerate(), envir = as.environment(".recordr"))
  assign("dataPackageId", UUIDgenerate(), envir = as.environment(".recordr"))
  dpId <- get("dataPackageId", envir= as.environment(".recordr"))
  print(paste("data package id: ", dpId))
  
  #runman@runId <= UUIDgenerate()
  
  # Source the user's script, passing in arguments that they intended for the 'source' call.
  base::source(fileName, ...)

  print(paste("package id: ", dpId))
  # return a datapackage object, but for now just return the id
  return(dpId)
})


## @param identifier The node identifier with which this node is registered in DataONE
## @returnType DataPackage  
## @return the DataPackage object containing all provenance relationships and derived data for this run
## 
## @author slaughter
## @export
setGeneric("lst", function(recordr) {
  standardGeneric("lst")
})

setMethod("lst", signature("Recordr"), function(recordr) {
  print(paste("list"))
  
})


## @param identifier The node identifier with which this node is registered in DataONE
## @returnType DataPackage  
## @return the DataPackage object containing all provenance relationships and derived data for this run
## 
#' @author slaughter
#' @export
setGeneric("view", function(recordr) {
  standardGeneric("view")
})

setMethod("view", signature("Recordr"), function(recordr) {
  print(paste("view: "))
  
})

## @param identifier The node identifier with which this node is registered in DataONE
## @returnType DataPackage  
## @return the DataPackage object containing all provenance relationships and derived data for this run
## 
## @author slaughter
## @export
#setGeneric("publish", function(recordr, packageId, MNode) {
#  standardGeneric("publish")
#})

#setMethod("publish", signature("Recordr", "character", "MNode"), function(recordr, packageId, MNode) {
#  print(paste("publishing package: ", packageId))
#  
#  return(1)
#})