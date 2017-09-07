# 
# This file contains recordr functions that override the corresponding functions from R and DataONE.
# These functions are overriden so that provenance information can be recorded for the files that
# are read and written.
#
#' @include Constants.R
#' @importFrom tools file_ext
#' @title Provenance wrapper function for dataone::getObject
#' @description Override the dataone::getObject method and record a provenance relationship
#' for the object that was downloaded.
#' @note This function is not intended to be called directly by a user.
#' @export
recordr_getObject <- function() {
  #message("in recordr_getObject")
  tracingState(on=FALSE)
  node <- getCallArgFromStack(sys.nframe(), functionName="getObject", argName="x", argPos=1)
  pid  <- getCallArgFromStack(sys.nframe(), functionName="getObject", argName="pid", argPos=2)
  cat(sprintf("node: %s\n", node@identifier))
  cat(sprintf("pid: %s\n", pid))
  
  # Get the option that controls whether or not DataONE read operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture reads.
  capture_d1_reads <- getOption("capture_dataone_reads")
  if(is.null(capture_d1_reads)) capture_d1_reads <- TRUE
  
  # Write provenance info for this object to the DataPackage object.
  if (getProvCapture() && capture_d1_reads) {
    setProvCapture(FALSE)
    #recordrEnv <- as.environment(".recordr")
    recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
    # Record the DataONE resolve service endpoint + pid for the object of the RDF triple
    # Decode the URL that will be added to the resource map
    D1_URL <- URLdecode(sprintf("%s/object/%s", node@endpoint, pid))
    # Record prov:used relationship between the input dataset and the execution
    insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=pid, predicate=provUsed)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=pid, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    recordrEnv$execInputIds <- c(recordrEnv$execInputIds, pid)
    # Write file metadata only, don't locally archive the file sent to DataONE, as this
    # operation is 'reading' the file from DataONE. If the script did read this file
    # locally by some other means (e.g. "readLines", "read.csv") then a separate prov entry
    # will be captured for that read.
    fId<- sprintf("urn:uuid:%s", UUIDgenerate())
    filemeta <- new("FileMetadata", file=D1_URL, 
                    fileId=fId, 
                    executionId=recordrEnv$execMeta@executionId, 
                    access="read", format="application/octet-stream",
                    archivedFilePath=as.character(NA))
    writeFileMeta(recordrEnv$recordr, filemeta)
    setProvCapture(TRUE)
  }
  tracingState(on=TRUE)
  return()
}

#' Provenance wrapper function for dataone::createObject method
#' @description Override the dataone::createOjbect method and record a provenance relationship
#' for the object created.
#' @note This function is not intended to be called directly by a user.
#' @export
recordr_createObject <- function() {
  tracingState(on=FALSE)
  mnode <- getCallArgFromStack(sys.nframe(), functionName="createObject", argName="x", argPos=1)
  pid <- getCallArgFromStack(sys.nframe(), functionName="createObject", argName="pid", argPos=2)
  file <- getCallArgFromStack(sys.nframe(), functionName="createObject", argName="file", argPos=3)
  
  D1_URL <- URLdecode(sprintf("%s/object/%s", mnode@endpoint, pid))
  # Get the option that controls whether or not DataONE write operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture writes.
  capture_d1_writes <- getOption("capture_dataone_writes")
  if(is.null(capture_d1_writes)) capture_d1_writes <- TRUE
  
  # Record provenance if not disabled 
  if (getProvCapture() && capture_d1_writes) {
    setProvCapture(FALSE)
    #recordrEnv <- as.environment(".recordr")
    recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
    # Record the DataONE endpoint + pid for the object of the RDF triple
    # Record prov:wasGeneratedByrelationship between the created dataset and the execution
    insertRelationship(recordrEnv$dataPkg, subjectID=pid, objectIDs=recordrEnv$execMeta@executionId, predicate=provWasGeneratedBy)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=pid, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    recordrEnv$execOutputIds <- c(recordrEnv$execOutputIds, pid)
    # The DataONE URL is the subject of the 'wasGeneratedBy' relationship, i.e. D1 URL -> wasGeneratedBy -> script,
    # so the URL is used instead of the filename. The file info will be collected from the local file however, i.e.
    # the size, checksum, etc. Also, this local file will not be archived, as it is available from DataONE.
    fId <- sprintf("urn:uuid:%s", UUIDgenerate())
    fpInfo <- file.info(file)
    filemeta <- new("FileMetadata", file=D1_URL, 
                    fileId=fId,
                    sha256=digest(object=file, algo="sha256", file=TRUE)[[1]],
                    size=as.numeric(fpInfo[["size"]]),
                    user=fpInfo[["uname"]],
                    createTime=as.character(fpInfo[["ctime"]]),
                    modifyTime=as.character(fpInfo[["mtime"]]),
                    executionId=recordrEnv$execMeta@executionId, 
                    access="write", format="application/octet-stream",
                    archivedFilePath=as.character(NA))
    writeFileMeta(recordrEnv$recordr, filemeta)
    setProvCapture(TRUE)
  }
  # Return the value from the overridden function
  tracingState(on=TRUE)
  return()
}

#' Provenance wrapper function for dataone::updateObject
#' @description Override the dataone::updateObject method and record a provenance
#' relationship for the object uploaded.
#' @note This function is not intended to be called directly by a user.
#' @export
recordr_updateObject <- function() {
  tracingState(on=FALSE)
  mnode <- getCallArgFromStack(sys.nframe(), functionName="updateObject", argName="x", argPos=1)
  newpid <- getCallArgFromStack(sys.nframe(), functionName="updateObject", argName="newpid", argPos=2)
  file <- getCallArgFromStack(sys.nframe(), functionName="updateObject", argName="file", argPos=3)
  
  D1_URL <- URLdecode(sprintf("%s/object/%s", mnode@endpoint, newpid))
  # Get the option that controls whether or not DataONE write operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture writes.
  capture_d1_writes <- getOption("capture_dataone_writes")
  if(is.null(capture_d1_writes)) capture_d1_writes <- TRUE
  
  if (getProvCapture() && capture_d1_writes) {
    setProvCapture(FALSE)
    #recordrEnv <- as.environment(".recordr")
    recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
    # Record prov:wasGeneratedByrelationship between the input dataset and the execution
    insertRelationship(recordrEnv$dataPkg, subjectID=newpid, objectIDs=recordrEnv$execMeta@executionId, predicate=provWasGeneratedBy)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=newpid, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    # Record the execution outputs that will be used to assert 'prov:wasDerivedFrom' relationships
    recordrEnv$execOutputIds <- c(recordrEnv$execOutputIds, newpid)
    # The DataONE URL is the subject of the 'wasGeneratedBy' relationship, i.e. D1 URL -> wasGeneratedBy -> script,
    # so the URL is used instead of the filename. The file info will be collected from the local file however, i.e.
    # the size, checksum, etc. Also, this local file will not be archived, as it is available from DataONE.
    fId <- sprintf("urn:uuid:%s", UUIDgenerate())
    fpInfo <- file.info(file)
    filemeta <- new("FileMetadata", file=D1_URL, 
                    fileId=fId,
                    sha256=digest(object=file, algo="sha256", file=TRUE)[[1]],
                    size=as.numeric(fpInfo[["size"]]),
                    user=fpInfo[["uname"]],
                    createTime=as.character(fpInfo[["ctime"]]),
                    modifyTime=as.character(fpInfo[["mtime"]]),
                    executionId=recordrEnv$execMeta@executionId, 
                    access="write", format="application/octet-stream",
                    archivedFilePath=as.character(NA))
    writeFileMeta(recordrEnv$recordr, filemeta)
    setProvCapture(TRUE) 
  }
  tracingState(on=TRUE)
  return()
}

## @export
#recordr_source <- function (file, local = FALSE, echo = verbose, print.eval = echo,
#                            verbose = getOption("verbose"), prompt.echo = getOption("prompt"),
#                            max.deparse.length = 150, chdir = FALSE, encoding = getOption("encoding"),
#                            continue.echo = getOption("continue"), skip.echo = 0,
#                            keep.source = getOption("keep.source")) {
#  if(length(verbose) == 0)
#    verbose = FALSE
#  
#  if(chdir) {
#    cwd = getwd()
#    on.exit(setwd(cwd))
#    setwd(dirname(file))
#  }
#  
#  #cat(sprintf("recordr_source: Sourcing file: %s\n", file))
#  
#  base::source(file, local, echo, print.eval, verbose, prompt.echo,
#               max.deparse.length, chdir, encoding,continue.echo, skip.echo,
#               keep.source)
#  
#  # Record the provenance relationship between the sourcing script and the sourced script
#  # as 'sourced script <- wasInfluenceddBy <- sourcing script
#  # i.e. insertRelationship
#}

#' Provenance wrapper for the R write.csv function
#' @description Override the utils::write.csv function and record a provenance relationship
#' for the written file.
#' @note This function is not intended to be called directly by a user.
#' @export
recordr_write.csv <- function() {
  #cat(sprintf("In recordr_write.csv\n"))
  tracingState(on=FALSE)
  file <- getCallArgFromStack(sys.nframe(), functionName="write.csv", argName="file", argPos=2)
  # The file should have been created, because this tracer function is fired at the exit of 'write.csv'
  # the 'file=' argument could be a complex expression, i.e. 'file=file.path(dir, filename)', so evaluate it.
    
  # Get the option that controls whether or not file write operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file writes.
  capture_file_writes <- getOption("capture_file_writes")
  if(is.null(capture_file_writes)) capture_file_writes <- TRUE
  datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
  
  # Record the provenance relationship between the user's script and the derived data file
  if (getProvCapture() && capture_file_writes) {
    if(is.element("connection", class(file))) {
      message(sprintf("Tracing write.csv from a connection is not supported by the recordr package."))
      tracingState(on=TRUE)
      return()
    }
    #recordrEnv <- as.environment(".recordr")
    recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
    setProvCapture(FALSE)
    # Create a data package object for the derived dataset
    dataFmt <- "text/csv"
    dataObj <- new("DataObject", id=datasetId, file=file, format=dataFmt)
    # Record prov:wasGeneratedBy relationship between the execution and the output dataset
    addMember(recordrEnv$dataPkg, dataObj)
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=recordrEnv$execMeta@executionId, predicate = provWasGeneratedBy)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    # Record the execution outputs that will be used to assert 'prov:wasDerivedFrom' relationships
    recordrEnv$execOutputIds <- c(recordrEnv$execOutputIds, datasetId)
    # Save a copy of this generated file to the recordr archiv
    archivedFilePath <- archiveFile(file=file)
    filemeta <- new("FileMetadata", file=file, 
                    fileId=datasetId, 
                    executionId=recordrEnv$execMeta@executionId, 
                    access="write", format="text/csv",
                    archivedFilePath=archivedFilePath)
    writeFileMeta(recordrEnv$recordr, filemeta)
    setProvCapture(TRUE)
  }
  tracingState(on=TRUE)
  return()
}

#' Provenance wrapper for the R utils::read.csv function
#' @description Override the utils::read.csv function and record a provenance relationship
#' for the file that was read.
#' @param ... function parameters
#' @note This function is not intended to be called directly by a user.
#' @export
recordr_read.csv <- function() {
  tracingState(on=FALSE)
  #cat(sprintf("In recordr_read.csv\n"))
  file <- getCallArgFromStack(sys.nframe(), functionName="read.csv", argName="file", argPos=2)
  text <- getCallArgFromStack(sys.nframe(), functionName="read.csv", argName="text", argPos=7)
  
  # Record the provenance relationship between the user's script and an input data file.
  # If the user didn't specify a data file, i.e. they are reading from a text connection,
  # then exit, as we don't track provenance for text connections. With read.csv, a
  # text connection can be specified by omitting the 'file' argument and specifying the
  # 'text' argument.
  # read.csv() args: no "file=", but "text="
  if (is.na(file) && (!is.na(text))) {
    #cat(sprintf("text connection: %s", argList$text))
    tracingState(on=TRUE)
    return()
  } 
  
  # Currently connections are not traced
  if(is.element("connection", class(file))) {
    message(sprintf("Tracing read.csv from a connection is not supported by the recordr package."))
  }
  
  # Get the option that controls whether or not file read operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file reads.
  capture_file_reads <- getOption("capture_file_reads")
  if(is.null(capture_file_reads)) capture_file_reads <- TRUE
  
  if (getProvCapture() && capture_file_reads) {
    #recordrEnv <- as.environment(".recordr")
    recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
    setProvCapture(FALSE)
    user <- recordrEnv$execMeta@user
    # TODO: replace this with a user configurable faciltiy to specify how to generate identifiers
    #datasetId <- sprintf("%s_%s", basename(fileArg), UUIDgenerate())
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    dataFmt <- "text/csv"
    dataObj <- new("DataObject", id=datasetId, format=dataFmt, user=user, mnNodeId=recordrEnv$mnNodeId, filename=file)
    # TODO: use file argument when file size is greater than a configuration value
    #dataObj <- new("DataObject", id=datasetId, filename=normalizePath(file), format=dataFmt, user=user, mnNodeId=recordrEnv$mnNodeId)    
    # Record prov:wasGeneratedBy relationship between the execution and the output dataset
    addMember(recordrEnv$dataPkg, dataObj)
    # Record prov:wasUsedBy relationship between the input dataset and the execution
    insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=datasetId, predicate = provUsed)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    # Record the execution inputs that will be used to assert 'prov:wasDerivedFrom' relationships
    recordrEnv$execInputIds <- c(recordrEnv$execInputIds, datasetId)
    # Save a copy of this input file into the recordr archive
    archivedFilePath <- archiveFile(file=file)
    # Save the file metadata to the database
    filemeta <- new("FileMetadata", file=file, 
                    fileId=datasetId, 
                    executionId=recordrEnv$execMeta@executionId, 
                    access="read", format="text/csv",
                    archivedFilePath=archivedFilePath)
    writeFileMeta(recordrEnv$recordr, filemeta)
    setProvCapture(TRUE)
  }
  tracingState(on=TRUE)
  return()
}

#' Provenance wrapper for the R readr::write_csv function
#' @description Override the readr::write_csv function and record a provenance relationship
#' for the written file.
#' @note This function is not intended to be called directly by a user.
#' @export
recordr_write_csv <- function() {
  cat(sprintf("In recordr_write_csv\n"))
  tracingState(on=FALSE)
  file <- getCallArgFromStack(sys.nframe(), functionName="write_csv", argName="path", argPos=2)
  # The file should have been created, because this tracer function is fired at the exit of 'write_csv'
  # the 'path=' argument could be a complex expression, i.e. 'path=file.path(dir, filename)', so evaluate it.
  
  # Get the option that controls whether or not file write operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file writes.
  capture_file_writes <- getOption("capture_file_writes")
  if(is.null(capture_file_writes)) capture_file_writes <- TRUE
  datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
  
  # Record the provenance relationship between the user's script and the derived data file
  if (getProvCapture() && capture_file_writes) {
    if(is.element("connection", class(file))) {
      message(sprintf("Tracing write_csv from a connection is not supported by the recordr package."))
      tracingState(on=TRUE)
      return()
    }
    #recordrEnv <- as.environment(".recordr")
    recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
    setProvCapture(FALSE)
    # Create a data package object for the derived dataset
    dataFmt <- "text/csv"
    dataObj <- new("DataObject", id=datasetId, file=file, format=dataFmt)
    # Record prov:wasGeneratedBy relationship between the execution and the output dataset
    addMember(recordrEnv$dataPkg, dataObj)
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=recordrEnv$execMeta@executionId, predicate = provWasGeneratedBy)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    # Record the execution outputs that will be used to assert 'prov:wasDerivedFrom' relationships
    recordrEnv$execOutputIds <- c(recordrEnv$execOutputIds, datasetId)
    # Save a copy of this generated file to the recordr archiv
    archivedFilePath <- archiveFile(file=file)
    filemeta <- new("FileMetadata", file=file, 
                    fileId=datasetId, 
                    executionId=recordrEnv$execMeta@executionId, 
                    access="write", format="text/csv",
                    archivedFilePath=archivedFilePath)
    writeFileMeta(recordrEnv$recordr, filemeta)
    setProvCapture(TRUE)
  }
  tracingState(on=TRUE)
  cat(sprintf("Exiting recordr_write_csv\n"))
  return()
}

#' Provenance wrapper for the R readr::read_csv function
#' @description Override the readr::read_csv function and record a provenance relationship
#' for the file that was read.
#' @param ... function parameters
#' @note This function is not intended to be called directly by a user.
#' @export
recordr_read_csv <- function() {
  tracingState(on=FALSE)
  cat(sprintf("In recordr_read_csv\n"))
  file <- getCallArgFromStack(sys.nframe(), functionName="read_csv", argName="path", argPos=1)
  
  # Record the provenance relationship between the user's script and an input data file.
  # If the user didn't specify a data file, i.e. they are reading from a text connection,
  # then exit, as we don't track provenance for text connections. With read.csv, a
  # text connection can be specified by omitting the 'file' argument and specifying the
  # 'text' argument.
  # read_csv() args: no "file=", but "text="
  if (is.na(file)) {
    #cat(sprintf("text connection: %s", argList$text))
    tracingState(on=TRUE)
    return()
  } 
  
  # Currently connections are not traced
  if(is.element("connection", class(file))) {
    message(sprintf("Tracing read_csv from a connection is not supported by the recordr package."))
  }
  
  # Get the option that controls whether or not file read operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file reads.
  capture_file_reads <- getOption("capture_file_reads")
  if(is.null(capture_file_reads)) capture_file_reads <- TRUE
  
  if (getProvCapture() && capture_file_reads) {
    #recordrEnv <- as.environment(".recordr")
    recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
    setProvCapture(FALSE)
    user <- recordrEnv$execMeta@user
    # TODO: replace this with a user configurable faciltiy to specify how to generate identifiers
    #datasetId <- sprintf("%s_%s", basename(fileArg), UUIDgenerate())
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    dataFmt <- "text/csv"
    dataObj <- new("DataObject", id=datasetId, format=dataFmt, user=user, mnNodeId=recordrEnv$mnNodeId, filename=file)
    # TODO: use file argument when file size is greater than a configuration value
    #dataObj <- new("DataObject", id=datasetId, filename=normalizePath(file), format=dataFmt, user=user, mnNodeId=recordrEnv$mnNodeId)    
    # Record prov:wasGeneratedBy relationship between the execution and the output dataset
    addMember(recordrEnv$dataPkg, dataObj)
    # Record prov:wasUsedBy relationship between the input dataset and the execution
    insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=datasetId, predicate = provUsed)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    # Record the execution inputs that will be used to assert 'prov:wasDerivedFrom' relationships
    recordrEnv$execInputIds <- c(recordrEnv$execInputIds, datasetId)
    # Save a copy of this input file into the recordr archive
    archivedFilePath <- archiveFile(file=file)
    # Save the file metadata to the database
    filemeta <- new("FileMetadata", file=file, 
                    fileId=datasetId, 
                    executionId=recordrEnv$execMeta@executionId, 
                    access="read", format="text/csv",
                    archivedFilePath=archivedFilePath)
    writeFileMeta(recordrEnv$recordr, filemeta)
    setProvCapture(TRUE)
  }
  tracingState(on=TRUE)
  cat(sprintf("Exiting recordr_read_csv\n"))
  return()
}  
  
#' Provenance wrapper for the ggplot2::ggsave function
#' @description Override the ggplot2::ggsave function and record a provenance relationship
#' for the file that was written.
#' @note This function is not intended to be called directly by a user.
#' @export
recordr_ggsave <- function() {
  tracingState(on=FALSE)
  #cat(sprintf("In recordr_ggsave\n"))
  filename <- getCallArgFromStack(sys.nframe(), functionName="ggsave", argName="filename", argPos=1)
  # Get the option that controls whether or not file write operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file writes.
  capture_file_writes <- getOption("capture_file_writes")
  if(is.null(capture_file_writes)) capture_file_writes <- TRUE
  # Record the provenance relationship between the user's script and the derived data file
    #message("tracing recordr_ggssave")
  if (getProvCapture() && capture_file_writes) {
    #recordrEnv <- as.environment(".recordr")
    recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
    setProvCapture(FALSE)
    user <- recordrEnv$execMeta@user
    #datasetId <- sprintf("%s_%s.%s", tools::file_path_sans_ext(basename(file)), UUIDgenerate(), tools::file_ext(file))
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    #TODO: determine format type for other image types, based on file extention
    if(file_ext(filename)[[1]] == "png") {
      dataFmt <- "image/png"
    } else if (file_ext(filename)[[1]] == "pdf") {
      dataFmt <- "application/pdf"
    } else if (file_ext(filename)[[1]] == "jpeg") {
      dataFmt <- "image/jpeg"
    } else if (file_ext(filename)[[1]] == "tiff") {
      dataFmt <- "image/tiff"
    } else if (file_ext(filename)[[1]] == "bmp") {
      dataFmt <- "image/bmp"
    } else if (file_ext(filename)[[1]] == "svg") {
      dataFmt <- "image/svg+xml"
    } else if (file_ext(filename)[[1]] == "wmf") {
      dataFmt <- "windows/metafile"
    } else if (file_ext(filename)[[1]] == "ps" || file_ext(filename)[[1]] == "eps") {
      dataFmt <- "application/postscript"
    } else {
      dataFmt <- "application/octet-stream"
    }
    
    dataObj <- new("DataObject", id=datasetId, format=dataFmt, filename=filename)
    # Record prov:wasGeneratedBy relationship between the execution and the output dataset
    addMember(recordrEnv$dataPkg, dataObj)
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=recordrEnv$execMeta@executionId, predicate = provWasGeneratedBy)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    # Record the execution outputs that will be used to assert 'prov:wasDerivedFrom' relationships
    recordrEnv$execOutputIds <- c(recordrEnv$execOutputIds, datasetId)
    # Save a copy of this generated file to the recordr archive
    archivedFilePath <- archiveFile(file=filename)
    filemeta <- new("FileMetadata", file=filename, 
                    fileId=datasetId, 
                    executionId=recordrEnv$execMeta@executionId, 
                    access="write", format=dataFmt,
                    archivedFilePath=archivedFilePath)
    writeFileMeta(recordrEnv$recordr, filemeta)
    setProvCapture(TRUE)
  }
  tracingState(on=TRUE)
  return()
}

#' Provenance wrapper for R base::readLines function
#' @description Override the base::readLines function and record a provenance relationship
#' for the file read.
#' @note This function is not intended to be called directly by a user.
#' @export
recordr_readLines <- function() {
  tracingState(on=FALSE)
  #cat(sprintf("In recordr_readLines\n"))
  con <- getCallArgFromStack(sys.nframe(), functionName="readLines", argName="con", argPos=1)
  # Get the option that controls whether or not file read operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file reads.
  capture_file_reads <- getOption("capture_file_reads")
  if(is.null(capture_file_reads)) capture_file_reads <- TRUE
  
  # TODO: if this is a connection, and not a string containing a filename, then check and
  # see if we have already recorded reading from the connection
  # Record the provenance relationship between the user's script and the derived data file
  
  if (getProvCapture() && capture_file_reads) {
    if(is.element("text", class(con))) {
      filePath <- summary(con)$description
      message(sprintf("Tracing readLines from a text connection is not supported by the recordr package."))
      tracingState(on=TRUE)
      return()
    } else {
      if(class(con) == "character") {
        filePath <- con 
      } else {
        filePath <- summary(con)$description
      }
    }
    #recordrEnv <- as.environment(".recordr")
    recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
    setProvCapture(FALSE)
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    dataFmt <- "application/octet-stream"
    dataObj <- new("DataObject", id=datasetId, format=dataFmt, file=filePath)
    # TODO: use file argument when file size is greater than a configuration value
    # Record prov:used relationship between the execution and the input dataset
    addMember(recordrEnv$dataPkg, dataObj)
    insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=datasetId, predicate = provUsed)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    # Record the execution inputs that will be used to assert 'prov:wasDerivedFrom' relationships
    recordrEnv$execInputIds <- c(recordrEnv$execInputIds, datasetId)
    # Save a copy of this generated file to the recordr archiv
    archivedFilePath <- archiveFile(file=filePath)
    filemeta <- new("FileMetadata", file=filePath, 
                    fileId=datasetId, 
                    executionId=recordrEnv$execMeta@executionId, 
                    access="read", format="application/octet-stream",
                    archivedFilePath=archivedFilePath)
    writeFileMeta(recordrEnv$recordr, filemeta)
    setProvCapture(TRUE)
  }
  tracingState(on=TRUE)
  return()
}

#' Provenance wrapper for R base::writeLines function
#' @description Override the base::writeLines function and record a provenance relationship
#' for the file that was written.
#' @note This function is not intended to be called directly by a user.
#' @export
recordr_writeLines <- function() {
  tracingState(on=FALSE)
  #cat(sprintf("In recordr_writeLines\n"))
  con <- getCallArgFromStack(sys.nframe(), functionName="writeLines", argName="filename", argPos=2)
  # Get the option that controls whether or not file write operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file writes.
  capture_file_writes <- getOption("capture_file_writes")
  if(is.null(capture_file_writes)) capture_file_writes <- TRUE
  
  # Record the provenance relationship between the user's script and the derived data file
  if (getProvCapture() && capture_file_writes) {
    if(is.element("text", class(con))) {
      filePath <- summary(con)$description
      message(sprintf("Tracing writeLines from a connection is not supported by the recordr package."))
      tracingState(on=TRUE)
      return()
    } else {
      filePath <- summary(con)$description
    }
    #recordrEnv <- as.environment(".recordr")
    recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
    setProvCapture(FALSE)
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    dataFmt <- "application/octet-stream"
    dataObj <- new("DataObject", id=datasetId, format=dataFmt, file=filePath)
    # TODO: use file argument when file size is greater than a configuration value
    # Record prov:wasGeneratedBy relationship between the execution and the output dataset
    addMember(recordrEnv$dataPkg, dataObj)
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=recordrEnv$execMeta@executionId, predicate = provWasGeneratedBy)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    # Record the execution outputs that will be used to assert 'prov:wasDerivedFrom' relationships
    recordrEnv$execOutputIds <- c(recordrEnv$execOutputIds, datasetId)
    # Save a copy of this generated file to the recordr archiv
    archivedFilePath <- archiveFile(file=filePath)
    filemeta <- new("FileMetadata", file=filePath, 
                    fileId=datasetId, 
                    executionId=recordrEnv$execMeta@executionId, 
                    access="write", format="application/octet-stream",
                    archivedFilePath=archivedFilePath)
    writeFileMeta(recordrEnv$recordr, filemeta)
    setProvCapture(TRUE)
  }
  tracingState(on=TRUE)
  return()
}

#' Provenance wrapper for the R base::scan function
#' @description Override the base::scan function and record a provenance relationship
#' for the scanned file.
#' @note This function is not intended to be called directly by a user.
#' @export
recordr_scan <- function() {
  tracingState(on=FALSE)
  #cat(sprintf("In recordr_scan\n"))
  file <- getCallArgFromStack(sys.nframe(), functionName="scan", argName="file", argPos=2)
  
  # Get the option that controls whether or not file read operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file reads.
  capture_file_reads <- getOption("capture_file_reads")
  if(is.null(capture_file_reads)) capture_file_reads <- TRUE
  
  # TODO: if this is a connection, and not a string containing a filename, then check and
  # see if we have already recorded reading from the connection
  # Record the provenance relationship between the user's script and the derived data file
  if (getProvCapture() && capture_file_reads) {
    if(is.element("connection", class(file))) {
      #filePath <- summary(file)$description
      message(sprintf("Tracing scan from a connection is not supported by the recordr package."))
      tracingState(on=TRUE)
      return()
    } else {
      filePath <- file
    }
    #recordrEnv <- as.environment(".recordr")
    recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
    setProvCapture(FALSE)
    user <- recordrEnv$execMeta@user
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    dataFmt <- "application/octet-stream"
    dataObj <- new("DataObject", id=datasetId, format=dataFmt, file=filePath)
    # TODO: use file argument when file size is greater than a configuration value
    # Record prov:used relationship between the execution and the input dataset
    addMember(recordrEnv$dataPkg, dataObj)
    insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=datasetId, predicate = provUsed)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    # Record the execution inputs that will be used to assert 'prov:wasDerivedFrom' relationships
    recordrEnv$execInputIds <- c(recordrEnv$execInputIds, datasetId)
    # Save a copy of this generated file to the recordr archiv
    archivedFilePath <- archiveFile(file=filePath)
    filemeta <- new("FileMetadata", file=filePath, 
                    fileId=datasetId, 
                    executionId=recordrEnv$execMeta@executionId, 
                    access="write", format="text/csv",
                    archivedFilePath=archivedFilePath)
    writeFileMeta(recordrEnv$recordr, filemeta)
    setProvCapture(TRUE)
  }
  tracingState(on=TRUE)
  return()
}

#' Provenance wrapper for the pnd::read function 
#' @description Override the png::read function and record a provenance relationship
#' for the file read. 
#' @note This function is not intended to be called directly by a user.
#' @export
recordr_readPNG <- function () {
  tracingState(on=FALSE)
  source <- getCallArgFromStack(sys.nframe(), functionName="readPNG", argName="source", argPos=1)
  # Get the option that controls whether or not file read operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file reads.
  capture_file_reads <- getOption("capture_file_reads")
  if(is.null(capture_file_reads)) capture_file_reads <- TRUE
  
  # Record the provenance relationship between the user's script and the derived data file
  if (getProvCapture() && capture_file_reads) {
    if(is.element("raw", class(source))) {
      message(sprintf("Tracing readPNG with source as a raw vector is not supported by the recordr package."))
      tracingState(on=TRUE)
      return()
    } else {
      filePath <- source
    }
    #recordrEnv <- as.environment(".recordr")
    recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
    setProvCapture(FALSE)
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    dataFmt <- "application/octet-stream"
    dataObj <- new("DataObject", id=datasetId, format=dataFmt, file=filePath)
    # TODO: use file argument when file size is greater than a configuration value
    # Record prov:used relationship between the execution and the input dataset
    addMember(recordrEnv$dataPkg, dataObj)
    insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=datasetId, predicate = provUsed)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    # Record the execution inputs that will be used to assert 'prov:wasDerivedFrom' relationships
    recordrEnv$execInputIds <- c(recordrEnv$execInputIds, datasetId)
    # Save a copy of this generated file to the recordr archiv
    archivedFilePath <- archiveFile(file=filePath)
    filemeta <- new("FileMetadata", file=filePath, 
                    fileId=datasetId, 
                    executionId=recordrEnv$execMeta@executionId, 
                    access="read", format="application/octet-stream",
                    archivedFilePath=archivedFilePath)
    writeFileMeta(recordrEnv$recordr, filemeta)
    setProvCapture(TRUE)
  }
  tracingState(on=TRUE)
  return()
}

#' Provenance wrapper for the png::write function
#' @description Override the png::write function and record a provenance relationship
#' for the file that was written.
#' @note This function is not intended to be called directly by a user.
#' @export
recordr_writePNG <- function() {
  tracingState(on=FALSE)
  target <- getCallArgFromStack(sys.nframe(), functionName="writePNG", argName="target", argPos=1)
  # Get the option that controls whether or not file write operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file writes.
  capture_file_writes <- getOption("capture_file_writes")
  if(is.null(capture_file_writes)) capture_file_writes <- TRUE
  
  # Record the provenance relationship between the user's script and the derived data file
  if (getProvCapture() && capture_file_writes) {
    # The argument 'target' can be a filename, a connection or a raw vector to which the
    # image will be written to.
    if(is.element("connection", class(target))) {
      filePath <- summary(target)$description
      message(sprintf("Tracing writePNG from a connection is not supported by the recordr package."))
      tracingState(on=TRUE)
      return()
    } else {
      filePath <- target
    }
    #recordrEnv <- as.environment(".recordr")
    recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
    setProvCapture(FALSE)
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    dataFmt <- "application/octet-stream"
    dataObj <- new("DataObject", id=datasetId, format=dataFmt, file=filePath)
    # TODO: use file argument when file size is greater than a configuration value
    # Record prov:wasGeneratedBy relationship between the execution and the output dataset
    addMember(recordrEnv$dataPkg, dataObj)
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=recordrEnv$execMeta@executionId, predicate = provWasGeneratedBy)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    # Record the execution outputs that will be used to assert 'prov:wasDerivedFrom' relationships
    recordrEnv$execOutputIds <- c(recordrEnv$execOutputIds, datasetId)
    # Save a copy of this generated file to the recordr archiv
    archivedFilePath <- archiveFile(file=filePath)
    filemeta <- new("FileMetadata", file=filePath, 
                    fileId=datasetId, 
                    executionId=recordrEnv$execMeta@executionId, 
                    access="write", format="application/octet-stream",
                    archivedFilePath=archivedFilePath)
    writeFileMeta(recordrEnv$recordr, filemeta)
    setProvCapture(TRUE)
  }
  tracingState(on=TRUE)
  return()
}


#' Provenance wrapper for the raster::raster function 
#' @description Override the raster::raster function and record a provenance relationship
#' for the file read. 
#' @note This function is not intended to be called directly by a user.
#' @export
recordr_raster <- function () {
  tracingState(on=FALSE)
  filePath <- getCallArgFromStack(sys.nframe(), functionName="raster", argName="x", argPos=1)
  
  if(class(filePath) != "character") {
    message("Don't trace raster() if arg is not class(character).")
    tracingState(on=TRUE)
    return() 
  }
  
  # Get the option that controls whether or not file read operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file reads.
  capture_file_reads <- getOption("capture_file_reads")
  if(is.null(capture_file_reads)) capture_file_reads <- TRUE
  
  # Record the provenance relationship between the user's script and the derived data file
  if (getProvCapture() && capture_file_reads) {
    #recordrEnv <- as.environment(".recordr")
    recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
    setProvCapture(FALSE)
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    dataFmt <- "application/octet-stream"
    dataObj <- new("DataObject", id=datasetId, format=dataFmt, file=filePath)
    # TODO: use file argument when file size is greater than a configuration value
    # Record prov:used relationship between the execution and the input dataset
    addMember(recordrEnv$dataPkg, dataObj)
    insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=datasetId, predicate = provUsed)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    # Record the execution inputs that will be used to assert 'prov:wasDerivedFrom' relationships
    recordrEnv$execInputIds <- c(recordrEnv$execInputIds, datasetId)
    # Save a copy of this generated file to the recordr archiv
    archivedFilePath <- archiveFile(file=filePath)
    message(sprintf("Capturing file data for fileId %s", datasetId))
    filemeta <- new("FileMetadata", file=filePath, 
                    fileId=datasetId, 
                    executionId=recordrEnv$execMeta@executionId, 
                    access="read", format="application/octet-stream",
                    archivedFilePath=archivedFilePath)
    writeFileMeta(recordrEnv$recordr, filemeta)
    setProvCapture(TRUE)
  }
  tracingState(on=TRUE)
  return()
}

#' Provenance wrapper for the raster::writeRaster function
#' @description Override the raster::writeRaster function and record a provenance relationship
#' for the file that was written.
#' @return The name of the output file
#' @note This function is not intended to be called directly by a user.
#' @export
recordr_writeRaster <- function() {
  tracingState(on=FALSE)
  filePath <- getCallArgFromStack(sys.nframe(), functionName="writeRaster", argName="filename", argPos=2)
  
  # Get the option that controls whether or not file write operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file writes.
  capture_file_writes <- getOption("capture_file_writes")
  if(is.null(capture_file_writes)) capture_file_writes <- TRUE
  # Record the provenance relationship between the user's script and the derived data file
  if (capture_file_writes) {
    #recordrEnv <- as.environment(".recordr")
    recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
    setProvCapture(FALSE)
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    dataFmt <- "application/octet-stream"
    dataObj <- new("DataObject", id=datasetId, format=dataFmt, file=filePath)
    # TODO: use file argument when file size is greater than a configuration value
    # Record prov:wasGeneratedBy relationship between the execution and the output dataset
    addMember(recordrEnv$dataPkg, dataObj)
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=recordrEnv$execMeta@executionId, predicate = provWasGeneratedBy)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    # Record the execution outputs that will be used to assert 'prov:wasDerivedFrom' relationships
    recordrEnv$execOutputIds <- c(recordrEnv$execOutputIds, datasetId)
    # Save a copy of this generated file to the recordr archiv
    archivedFilePath <- archiveFile(file=filePath)
    filemeta <- new("FileMetadata", file=filePath, 
                    fileId=datasetId, 
                    executionId=recordrEnv$execMeta@executionId, 
                    access="write", format="application/octet-stream",
                    archivedFilePath=archivedFilePath)
    writeFileMeta(recordrEnv$recordr, filemeta)
  }
  tracingState(on=TRUE)
  return()
}

#' Provenance wrapper for the rgdal::readOGR function 
#' @description Override the rgdal::readOGR function and record a provenance relationship
#' for the file read. 
#' @note This function is not intended to be called directly by a user.
#' @export
recordr_readOGR <- function () {
  tracingState(on=FALSE)
  dsn <- getCallArgFromStack(sys.nframe(), functionName="readOGR", argName="dsn", argPos=1)
  layer <- getCallArgFromStack(sys.nframe(), functionName="readOGR", argName="layer", argPos=2)
  
  # The dsn argument can be a file or a directory
  if(file.exists(dsn)) {
    fInfo <- file.info(dsn)
    if (fInfo$isdir) {
      # dsn is a directory, query rgdal to get the actual dsn, layer
      dsnInfo <- rgdal::ogrInfo(dsn, layer)
      filePath <- sprintf("%s/%s.shp", dsnInfo$dsn, dsnInfo$layer)
      #cat(sprintf("dsn is dir, filePath: %s\n", filePath))
    } else {
      # File exists and is not a directory, user specified specific file to read
      filePath <- dsn
      #cat(sprintf("dsn is file, filePath: %s\n", filePath))
    }
  } else {
    # file or dir doesn't exist, return the function results
    tracingState(on=TRUE)
    return()
  }
  
  # Get the option that controls whether or not file read operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file reads.
  capture_file_reads <- getOption("capture_file_reads")
  if(is.null(capture_file_reads)) capture_file_reads <- TRUE
  
  # Record the provenance relationship between the user's script and the derived data file
  if (getProvCapture() && capture_file_reads) {
    #recordrEnv <- as.environment(".recordr")
    recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
    setProvCapture(FALSE)
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    dataFmt <- "application/octet-stream"
    dataObj <- new("DataObject", id=datasetId, format=dataFmt, filename=filePath)
    # TODO: use file argument when file size is greater than a configuration value
    # Record prov:used relationship between the execution and the input dataset
    addMember(recordrEnv$dataPkg, dataObj)
    insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=datasetId, predicate = provUsed)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    # Record the execution inputs that will be used to assert 'prov:wasDerivedFrom' relationships
    recordrEnv$execInputIds <- c(recordrEnv$execInputIds, datasetId)
    # Save a copy of this generated file to the recordr archiv
    archivedFilePath <- archiveFile(file=filePath)
    message(sprintf("Capturing file data for fileId %s", datasetId))
    filemeta <- new("FileMetadata", file=filePath, 
                    fileId=datasetId, 
                    executionId=recordrEnv$execMeta@executionId, 
                    access="read", format="application/octet-stream",
                    archivedFilePath=archivedFilePath)
    writeFileMeta(recordrEnv$recordr, filemeta)
    setProvCapture(TRUE)
  }
  tracingState(on=TRUE)
  return()
}
 
#' Provenance wrapper for the rgdal::writeOGR function
#' @description Override the rgdal::writeOGR function and record a provenance relationship
#' for the file that was written.
#' @note This function is not intended to be called directly by a user.
#' @export
recordr_writeOGR <- function() {
  tracingState(on=FALSE)
  dsn <- getCallArgFromStack(sys.nframe(), functionName="writeOGR", argName="dsn", argPos=2)
  layer <- getCallArgFromStack(sys.nframe(), functionName="writeOGR", argName="layer", argPos=3)
  
  # The dsn argument can be a file or a directory
  filePath <- ""
  if(file.exists(dsn)) {
    fInfo <- file.info(dsn)
    if (fInfo$isdir) {
      # dsn is a directory, query rgdal to get the actual dsn, layer
      dsnInfo <- rgdal::ogrInfo(dsn, layer)
      filePath <- sprintf("%s/%s.shp", dsnInfo$dsn, dsnInfo$layer)
      #cat(sprintf("writeOGR, dsn is dir, filePath: %s\n", filePath))
    } else {
      # File exists and is not a directory, user specified specific file to read
      filePath <- dsn
      #cat(sprintf("writeOGR, dsn is file, filePath: %s\n", filePath))
    }
  } else {
    # file or dir doesn't exist, return the function results (NULL for writeOGR)
    tracingState(on=TRUE)
    return()
  }
  
  # Get the option that controls whether or not file write operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file writes.
  capture_file_writes <- getOption("capture_file_writes")
  if(is.null(capture_file_writes)) capture_file_writes <- TRUE
  
  # Record the provenance relationship between the user's script and the derived data file
  if (getProvCapture() && capture_file_writes) {
    #cat(sprintf("Tracing writeOGR with file: %s from package %s\n", filePath, environmentName(functionEnv)))
    #recordrEnv <- as.environment(".recordr")
    recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
    setProvCapture(FALSE)
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    dataFmt <- "application/octet-stream"
    dataObj <- new("DataObject", id=datasetId, format=dataFmt, file=filePath)
    # TODO: use file argument when file size is greater than a configuration value
    # Record prov:wasGeneratedBy relationship between the execution and the output dataset
    addMember(recordrEnv$dataPkg, dataObj)
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=recordrEnv$execMeta@executionId, predicate = provWasGeneratedBy)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    # Record the execution outputs that will be used to assert 'prov:wasDerivedFrom' relationships
    recordrEnv$execOutputIds <- c(recordrEnv$execOutputIds, datasetId)
    # Save a copy of this generated file to the recordr archiv
    archivedFilePath <- archiveFile(file=filePath)
    filemeta <- new("FileMetadata", file=filePath, 
                    fileId=datasetId, 
                    executionId=recordrEnv$execMeta@executionId, 
                    access="write", format="application/octet-stream",
                    archivedFilePath=archivedFilePath)
    writeFileMeta(recordrEnv$recordr, filemeta)
    setProvCapture(TRUE)
  }
  tracingState(on=TRUE)
  return()
}

# Disable or enable provenance capture temporarily
# It may be necessary to disable provenance capture temporarily, for example when
# record() is writting out a housekeeping file.
# A state variable in the ".recordr" environment is used to
# temporarily disable provenance capture so that housekeeping tasks
# will not have provenance information recorded for them.
# Return the state of provenance capture: TRUE is enalbed, FALSE is disabled
setProvCapture <- function(enable) {
  # If the '.recordr' environment hasn't been created, then we are calling this
  # function outside the context of record(), so don't attempt to update the environment'
  #if (exists("provCaptureEnabled", where = as.environment(recordrEnv), inherits = FALSE )) {
  #  if (is.element(".recordr", base::search())) {
  if (exists(".recordrEnv", where = globalenv(), inherits = FALSE )) {
    recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
    assign("provCaptureEnabled", enable, envir = recordrEnv)
    return(enable)
  } else {
    # If we were able to update "provCaptureEnabled" state variable because env ".recordr"
    # didn't exist, then provenance capture is certainly not enabled.    
    return(FALSE)
  }
}

# Return current state of provenance capture as a logical: TRUE=enabled, FALSE=disabled
getProvCapture <-  function() {
  # The default state for provenance capture is enabled = FALSE. Currently in this package,
  # provenance capture is only enabled when the record() function is running.
  #
  # If the '.recordr' environment hasn't been created, then we are calling this
  # function outside the context of record(), so don't attempt to read from the environment.
  recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
  #if (is.element(".recordr", base::search())) {
  if (exists("provCaptureEnabled", where = recordrEnv, inherits = FALSE )) {
    enabled <- base::get("provCaptureEnabled", envir = recordrEnv)
  } else {
    enabled <- FALSE
  }
  return(enabled)
}

# Archive a file into the recordr archive directory
archiveFile <- function(file, force=FALSE) {
  if(!file.exists(file)) {
    message("Cannot copy file %s to recordr archive, it does not exist\n", file)
    return(NULL)
  }
  
  recordrEnv <- as.environment(base::get(".recordrEnv", envir=globalenv()))
  # If force is trune, then always archive the file, regardless of user defined settings
  if(!force) {
    # Don't archive files that are bigger than a user specified maximum, in bytes
    maxArchiveFileSize <- getOption("recordr_max_archive_file_size")
    if(is.null(maxArchiveFileSize) || is.na(maxArchiveFileSize)) {
      maxArchiveFileSize <- 0.0
    } 
    maxArchiveFileSize <- as.double(maxArchiveFileSize)
    # Check if this file should be archived and if not, return NA
    if(maxArchiveFileSize > 0.0) {
      fpInfo <- file.info(file)
      if(!is.null(fpInfo[["size"]])) {
        cat(sprintf("checking file size"))
        if(fpInfo[["size"]] > maxArchiveFileSize) {
          return(as.character(NA))
        }
      }
    }
  }
  
  # First check if a file with the same sha256 has been accessed before.
  # If it has, then don't archive this file again, and return the
  # archived location of the previously archived file.
  fm <- readFileMeta(recordrEnv$recordr, sha256=digest::digest(object=file, algo="sha256", file=TRUE))
  if(nrow(fm) > 0) {
    archivedRelFilePath <- fm[1, "archivedFilePath"]
    return(archivedRelFilePath)
  }
  
  # The archive directory is specified relative to the recordr root
  # directory, so that if the recordr root directory has to be moved, the
  # database entries for archived directories does not have to be updated.
  # The archive directory is named simply for today's date. The data directory
  # is put at the top of the archive directory, just so that directory file
  # limits aren't exceeded. Directories on ext3 filesystems a directory can contain 
  # 32,000 entries, so this simple scheme should not run into any OS limits. Also, these directories
  # will not be searched, as the filepaths are contains in a database, so directory
  # lookup performance is not an issue. Note, the function 'unarchive()' depends on the
  # filename containing 'archive' in order to be deleted, so don't change this.
  archiveRelDir <- file.path("archive", substr(as.character(Sys.time()), 1, 10))
  fullDirPath <- normalizePath(file.path(recordrEnv$recordr@recordrDir, archiveRelDir), mustWork=FALSE)
  if (!file.exists(fullDirPath)) {
    dir.create(fullDirPath, recursive = TRUE)
  }  
  archivedRelFilePath <- file.path(archiveRelDir, UUIDgenerate())
  fullFilePath <- normalizePath(file.path(recordrEnv$recordr@recordrDir, archivedRelFilePath), mustWork=FALSE)
  # First check if the file has already been archived by searching for a file
  # with the same sha256 checksum. Each archived file must have a unique name
  # as a filename may be an input for many runs on the same day, with the
  # file being updated between each run
  file.copy(file, fullFilePath, overwrite = FALSE, recursive = FALSE,
            copy.mode = TRUE, copy.date = TRUE)
  # TODO: Check if the file was actually copied
  return(archivedRelFilePath)
}

getCallArgFromStack <- function(nFrame, functionName, argName, argPos) {
  # Go backward in the call stack and find the call for the function we are 
  # searching for. The first list element from 'sys.call()' will be the
  # function and unevaluated arguments.
  # The call we are looking for will look something like this
  #   write.csv(testdf, csvfile, row.names = FALSE)
  # When this call is converted to a list (i.e. as.list(argList)), the
  # named arguments will be names of the list, and the positional args
  # will be in the corresponsing location for the call, i.e. for write.csv
  # the "file" argument is in position 2, well actually position 3, as the
  # function name will always be the first list element.
  nback <- 1
  for (iframe in nFrame:1) {
    nback <- nback + 1
    thisCall <- sys.call(which=iframe)
    argList <- as.list(thisCall)
    #cat(sprintf("class: %s\n", class(argList[[1]])))
    if(class(argList[[1]]) != "name" && class(argList[[1]]) != "call") next
    if(class(argList[[1]]) == "call") {
      thisCallStr <- deparse(argList[[1]])
    } else {
      thisCallStr <- as.character(argList[[1]])
    }
    
    #cat(sprintf("trying %s\n", thisCallStr))
    # Does the call match the target function call with or without the namespace?
    if(grepl(paste("^", functionName, sep=""), thisCallStr, perl=TRUE) || grepl(paste0("^.*::", functionName), thisCallStr, perl=TRUE)) {
      parentEnv <- parent.frame(n=nback)
      rawArg <- as.list(standardizeCall(sys.call(which=iframe), env=parentEnv))[[argName]]
      if(is.null(rawArg)) {
        if(argPos+1 > length(argList)) {
          return(NULL)
        } else {
          rawArg <- as.list(standardizeCall(sys.call(which=iframe), env=parentEnv))[[argPos+1]]
        }
      }
      #eval(parse(text=foo), envir=parent.frame(n=6))
      #cat(sprintf("class(rawArg): %s\n", class(rawArg)))
      if(class(rawArg) == "character" || class(rawArg) == "call") {
        argValue <- eval(rawArg, envir=parentEnv)
      } else {
        argValue <- eval(parse(text=as.character(rawArg)), envir=parentEnv)
      }
      return(argValue)
    } 
    #else {
    #  cat(sprintf("skipping %s\n", thisCallStr))
    #}
  }
  stop("Internal error: unable to trace %s, cannot find this function in the call stack.")
}

findEnv <- function(name, env = parent.frame()) {
  cat(sprintf("checking env: %s\n", environmentName(env)))
  if (identical(env, emptyenv())) {
    return(as.character(NA))
  } else if (exists(name, envir = env, inherits = FALSE)) {
      return(env$recordrEnv)
  } else {
    # Recursive case
    findEnv(name, parent.env(env))
  }
}

#' Standardise a function call
#'
#' @param call A call
#' @param env Environment in which to look up call value.
#' @note from Hadley Wicham's pryr standarize_call
standardizeCall <- function(call, env = parent.frame()) {
  stopifnot(is.call(call))
  f <- eval(call[[1]], env)
  if (is.primitive(f)) return(call)
  
  match.call(f, call)
}