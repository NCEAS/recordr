# 
# This file contains recordr functions that override the corresponding functions from R and DataONE.
# These functions are overriden so that provenance information can be recorded for the files that
# are read and written.
#
#' @include Constants.R
# Override dataone::getObject function
recordr_getObject <- function(node, pid, ...) {
  # Call the masked function to retrieve the DataONE object
  # This function has been entered because the user called "getObject", which redirects to
  # "recordr_getObject". It is possible for the user to enter this function then, if they
  # didn't actually load the dataone package (i.e. library(dataone)), so check if dataone
  # package is available, and print the appropriate message if not.
  if(suppressWarnings(requireNamespace(dataone))) {
    # Call the original function that we are overriding
    d1o <- dataone::getObject(node, pid, ...)
  } else {
    stop("recordr package is tracing getObject(), but package \"dataone\" is not available.")
  } 
  
  # Get the option that controls whether or not DataONE read operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture reads.
  capture_d1_reads <- getOption("capture_dataone_reads")
  if(is.null(capture_d1_reads)) capture_d1_reads <- TRUE
  
  # Write provenance info for this object to the DataPackage object.
  if (getProvCapture() && capture_d1_reads) {
    setProvCapture(FALSE)
    recordrEnv <- as.environment(".recordr")
    # Record the DataONE resolve service endpoint + pid for the object of the RDF triple
    # Decode the URL that will be added to the resource map
    D1_URL <- URLdecode(sprintf("%s/object/%s", node@endpoint, pid))
    # Record prov:used relationship between the input dataset and the execution
    insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=D1_URL, predicate=provUsed)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=D1_URL, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    recordrEnv$execInputIds <- c(recordrEnv$execInputIds, D1_URL)
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
  return(d1o)
}

## Override the DataONE 'create' method
recordr_create <- function(mnode, pid, file, sysmeta, ...) {
  if(suppressWarnings(requireNamespace(dataone))) {
    # Call the overridden function
    result <- dataone::createObject(mnode, pid, file, sysmeta, ...)
  } else {
    stop("recordr package is tracing getObject(), but package \"dataone\" is not available.")
  } 
  
  # Get the option that controls whether or not DataONE write operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture writes.
  capture_d1_writes <- getOption("capture_dataone_writes")
  if(is.null(capture_d1_writes)) capture_d1_writes <- TRUE
  
  # Record provenance if not disabled 
  if (getProvCapture() && capture_d1_writes) {
    setProvCapture(FALSE)
    recordrEnv <- as.environment(".recordr")
    # Record the DataONE endpoint + pid for the object of the RDF triple
    D1_URL <- URLdecode(sprintf("%s/object/%s", mnode@endpoint, pid))
    # Record prov:wasGeneratedByrelationship between the created dataset and the execution
    insertRelationship(recordrEnv$dataPkg, subjectID=D1_URL, objectIDs=recordrEnv$execMeta@executionId, predicate=provWasGeneratedBy)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=D1_URL, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    recordrEnv$execOutputIds <- c(recordrEnv$execOutputIds, D1_URL)
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
  return(result)
}

# Override the DataONE 'update' method
recordr_updateObject <- function(mnode, pid, file, newpid, sysmeta, ...) {
  if(suppressWarnings(requireNamespace(dataone))) {
    # Call the overridden function
    result <- dataone::updateObject(mnode, pid, file, newpid, sysmeta)
  } else {
    stop("recordr package is tracing dataone::update(), but package dataone is not available.")
  } 
  
  # Get the option that controls whether or not DataONE write operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture writes.
  capture_d1_writes <- getOption("capture_dataone_writes")
  if(is.null(capture_d1_writes)) capture_d1_writes <- TRUE
  
  if (getProvCapture() && capture_d1_writes) {
    setProvCapture(FALSE)
    recordrEnv <- as.environment(".recordr")
    # Record the DataONE endpoint + pid for the object of the RDF triple
    D1_URL <- URLdecode(sprintf("%s/object/%s", mnode@endpoint, pid))
    # Record prov:wasGeneratedByrelationship between the input dataset and the execution
    insertRelationship(recordrEnv$dataPkg, subjectID=D1_URL, objectIDs=recordrEnv$execMeta@executionId, predicate=provWasGeneratedBy)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=D1_URL, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    # Record the execution outputs that will be used to assert 'prov:wasDerivedFrom' relationships
    recordrEnv$execOutputIds <- c(recordrEnv$execOutputIds, D1_URL)
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
  return(result)
}

# Override the 'source' function so that recordr can detect when the user's script sources another script
recordr_source <- function (file, local = FALSE, echo = verbose, print.eval = echo,
                            verbose = getOption("verbose"), prompt.echo = getOption("prompt"),
                            max.deparse.length = 150, chdir = FALSE, encoding = getOption("encoding"),
                            continue.echo = getOption("continue"), skip.echo = 0,
                            keep.source = getOption("keep.source")) {
  if(length(verbose) == 0)
    verbose = FALSE
  
  if(chdir) {
    cwd = getwd()
    on.exit(setwd(cwd))
    setwd(dirname(file))
  }
  
  #cat(sprintf("recordr_source: Sourcing file: %s\n", file))
  
  base::source(file, local, echo, print.eval, verbose, prompt.echo,
               max.deparse.length, chdir, encoding,continue.echo, skip.echo,
               keep.source)
  
  # Record the provenance relationship between the sourcing script and the sourced script
  # as 'sourced script <- wasInfluenceddBy <- sourcing script
  # i.e. insertRelationship
}

# Override the R 'write.csv' method
recordr_write.csv <- function(x, file, ...) {
  # Call the original function that we are overriding
  obj <- utils::write.csv(x, file, ...)
  
  # Get the option that controls whether or not file write operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file writes.
  capture_file_writes <- getOption("capture_file_writes")
  if(is.null(capture_file_writes)) capture_file_writes <- TRUE
  
  # Record the provenance relationship between the user's script and the derived data file
  if (getProvCapture() && capture_file_writes) {
    # Currently connections are not traced
    if(is.element("connection", class(file))) {
      #message(sprintf("Tracing write.csv from a connection is not supported."))
      return(dataRead)
    }
    recordrEnv <- as.environment(".recordr")
    setProvCapture(FALSE)
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    dataFmt <- "text/csv"
    dataObj <- new("DataObject", id=datasetId, file=file, format=dataFmt)
    # Record prov:wasGeneratedBy relationship between the execution and the output dataset
    addData(recordrEnv$dataPkg, dataObj)
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
  return(obj)
}

# Override read.csv
recordr_read.csv <- function(...) {
  dataRead <- utils::read.csv(...)
  # Record the provenance relationship between the user's script and an input data file.
  # If the user didn't specify a data file, i.e. they are reading from a text connection,
  # then exit, as we don't track provenance for text connections. With read.csv, a
  # text connection can be specified by omitting the 'file' argument and specifying the
  # 'text' argument.
  argList <- list(...)
  argListLen <- length(argList)
  # read.csv() args: no "file=", but "text="
  if (!"file" %in% names(argList) && "text" %in% names(argList)) {
    #cat(sprintf("text connection: %s", argList$text))
    return(dataRead)
  } else if ("file" %in% names(argList)) {
    # read.csv() args: "file=" specified
    #cat(sprintf("file: %s\n", argList$file))
    fileArg <- argList$file
  } else if (!"file" %in% names(argList) && !"text" %in% names(argList)) {
    # read.csv() args: no "file=" or "text=", so first arg must be the filename
    #cat(sprintf("file: %s\n", argList[[1]]))
    fileArg <- argList[[1]]
  } else {
    cat(paste0("Error: unknown arguments passed to record_read.csv: ", argList))
  }
  
  # Currently connections are not traced
  if(is.element("connection", class(fileArg))) {
    #message(sprintf("Tracing read.csv from a connection is not supported."))
    return(dataRead)
  }
  
  # Get the option that controls whether or not file read operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file reads.
  capture_file_reads <- getOption("capture_file_reads")
  if(is.null(capture_file_reads)) capture_file_reads <- TRUE
  
  if (getProvCapture() && capture_file_reads) {
    recordrEnv <- as.environment(".recordr")
    setProvCapture(FALSE)
    user <- recordrEnv$execMeta@user
    # TODO: replace this with a user configurable faciltiy to specify how to generate identifiers
    #datasetId <- sprintf("%s_%s", basename(fileArg), UUIDgenerate())
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    dataFmt <- "text/csv"
    dataObj <- new("DataObject", id=datasetId, format=dataFmt, user=user, mnNodeId=recordrEnv$mnNodeId, filename=fileArg)
    # TODO: use file argument when file size is greater than a configuration value
    #dataObj <- new("DataObject", id=datasetId, filename=normalizePath(file), format=dataFmt, user=user, mnNodeId=recordrEnv$mnNodeId)    
    # Record prov:wasGeneratedBy relationship between the execution and the output dataset
    addData(recordrEnv$dataPkg, dataObj)
    # Record prov:wasUsedBy relationship between the input dataset and the execution
    insertRelationship(recordrEnv$dataPkg, subjectID=recordrEnv$execMeta@executionId, objectIDs=datasetId, predicate = provUsed)
    # Record relationship identifying this dataset as a provone:Data
    insertRelationship(recordrEnv$dataPkg, subjectID=datasetId, objectIDs=provONEdata, predicate=rdfType, objectTypes="uri")
    # Record the execution inputs that will be used to assert 'prov:wasDerivedFrom' relationships
    recordrEnv$execInputIds <- c(recordrEnv$execInputIds, datasetId)
    # Save a copy of this input file into the recordr archive
    archivedFilePath <- archiveFile(file=fileArg)
    # Save the file metadata to the database
    filemeta <- new("FileMetadata", file=fileArg, 
                    fileId=datasetId, 
                    executionId=recordrEnv$execMeta@executionId, 
                    access="read", format="text/csv",
                    archivedFilePath=archivedFilePath)
    writeFileMeta(recordrEnv$recordr, filemeta)
    setProvCapture(TRUE)
  }
  return(dataRead)
}

# Override ggplot2::ggsave()
recordr_ggsave <- function(filename, ...) {
  if (suppressWarnings(requireNamespace("ggplot2"))) {
    # Call the original function that we are overriding
    obj <- ggplot2::ggsave(filename, ...)
  } else {
    stop("recordr package is tracing ggplot2::ggsave(), but package ggplot2 is not available.")
  }
  
  # Get the option that controls whether or not file write operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file writes.
  capture_file_writes <- getOption("capture_file_writes")
  if(is.null(capture_file_writes)) capture_file_writes <- TRUE
  
  # Record the provenance relationship between the user's script and the derived data file
    #message("tracing recordr_ggssave")
  if (getProvCapture() && capture_file_writes) {
    recordrEnv <- as.environment(".recordr")
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
    addData(recordrEnv$dataPkg, dataObj)
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
  return(obj)
}

# Override the 'readLines' function
# record the provenance relationship of script -> used -> file 
recordr_readLines <- function(con, ...) {
  # Call the original function that we are overriding
  obj <- base::readLines(con, ...)
  
  # Get the option that controls whether or not file read operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file reads.
  capture_file_reads <- getOption("capture_file_reads")
  if(is.null(capture_file_reads)) capture_file_reads <- TRUE
  
  # TODO: if this is a connection, and not a string containing a filename, then check and
  # see if we have already recorded reading from the connection
  # Record the provenance relationship between the user's script and the derived data file
  
  if (getProvCapture() && capture_file_reads) {
    if(is.element("connection", class(con))) {
      filePath <- summary(con)$description
      #message(sprintf("Tracing readLines from a connection is not supported."))
      return(obj)
    } else {
      filePath <- con
    }
    recordrEnv <- as.environment(".recordr")
    setProvCapture(FALSE)
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    dataFmt <- "application/octet-stream"
    dataObj <- new("DataObject", id=datasetId, format=dataFmt, file=filePath)
    # TODO: use file argument when file size is greater than a configuration value
    # Record prov:used relationship between the execution and the input dataset
    addData(recordrEnv$dataPkg, dataObj)
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
  invisible(obj)
}

# Override the 'writeLines' function
# record the provenance relationship of script <- used <- used
recordr_writeLines <- function(text, con, ...) {
  # Call the original function that we are overriding
  base::writeLines(text, con, ...)
  
  # Get the option that controls whether or not file write operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file writes.
  capture_file_writes <- getOption("capture_file_writes")
  if(is.null(capture_file_writes)) capture_file_writes <- TRUE
  
  # Record the provenance relationship between the user's script and the derived data file
  if (getProvCapture() && capture_file_writes) {
    if(is.element("connection", class(con))) {
      filePath <- summary(con)$description
      #message(sprintf("Tracing writeLines from a connection is not supported."))
      return()
    } else {
      filePath <- con
    }
    recordrEnv <- as.environment(".recordr")
    setProvCapture(FALSE)
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    dataFmt <- "application/octet-stream"
    dataObj <- new("DataObject", id=datasetId, format=dataFmt, file=filePath)
    # TODO: use file argument when file size is greater than a configuration value
    # Record prov:wasGeneratedBy relationship between the execution and the output dataset
    addData(recordrEnv$dataPkg, dataObj)
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
  return()
}


# Override the 'readLines' function
# record the provenance relationship of script -> used -> file 
recordr_scan <- function(file, ...) {
  # Call the original function that we are overriding
  obj <- base::scan(file, ...)
  
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
      #message(sprintf("Tracing scan from a connection is not supported."))
      return(obj)
    } else {
      filePath <- file
    }
    recordrEnv <- as.environment(".recordr")
    setProvCapture(FALSE)
    user <- recordrEnv$execMeta@user
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    dataFmt <- "application/octet-stream"
    dataObj <- new("DataObject", id=datasetId, format=dataFmt, file=filePath)
    # TODO: use file argument when file size is greater than a configuration value
    # Record prov:used relationship between the execution and the input dataset
    addData(recordrEnv$dataPkg, dataObj)
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
  invisible(obj)
}

# Override the 'readPNG' function
# record the provenance relationship of script -> used -> file 
recordr_readPNG <- function (source, ...) {
  
  # Call the original function that we are overriding
  # Call the masked function to retrieve the DataONE object
  # This function has been entered because the user called "getObject", which redirects to
  # "recordr_getObject". It is possible for the user to enter this function then, if they
  # didn't actually load the dataone package (i.e. library(dataone)), so check if dataone
  # package is available, and print the appropriate message if not.
  if(suppressWarnings(requireNamespace(png))) {
    # Call the original function that we are overriding
    obj <- png::readPNG(source, ...)
  } else {
    stop("recordr package is tracing readPNG(), but package \"png\" is not available.\nPlease install package \"png\"")
  } 
  
  # Get the option that controls whether or not file read operations are traced.
  # If this option is not set, NULL is returned. If this is the case, set the default
  # to TRUE, i.e. capture file reads.
  capture_file_reads <- getOption("capture_file_reads")
  if(is.null(capture_file_reads)) capture_file_reads <- TRUE
  
  # Record the provenance relationship between the user's script and the derived data file
  if (getProvCapture() && capture_file_reads) {
    if(is.element("raw", class(source))) {
      filePath <- summary(con)$description
      message(sprintf("Tracing readPNG with source as a raw vector is not supported."))
      return(obj)
    } else {
      filePath <- source
    }
    recordrEnv <- as.environment(".recordr")
    setProvCapture(FALSE)
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    dataFmt <- "application/octet-stream"
    dataObj <- new("DataObject", id=datasetId, format=dataFmt, file=filePath)
    # TODO: use file argument when file size is greater than a configuration value
    # Record prov:used relationship between the execution and the input dataset
    addData(recordrEnv$dataPkg, dataObj)
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
  invisible(obj)
}

# Override the 'writePNG' function
# record the provenance relationship of script <- used <- used
recordr_writePNG <- function(image, target, ...) {
  # Call the original function that we are overriding
  outImage <- png::writePNG(image, target, ...)
  
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
      message(sprintf("Tracing writePNG from a connection is not supported."))
      return()
    } else {
      filePath <- target
    }
    recordrEnv <- as.environment(".recordr")
    setProvCapture(FALSE)
    datasetId <- sprintf("urn:uuid:%s", UUIDgenerate())
    # Create a data package object for the derived dataset
    dataFmt <- "application/octet-stream"
    dataObj <- new("DataObject", id=datasetId, format=dataFmt, file=filePath)
    # TODO: use file argument when file size is greater than a configuration value
    # Record prov:wasGeneratedBy relationship between the execution and the output dataset
    addData(recordrEnv$dataPkg, dataObj)
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
  return(outImage)
}

#' Disable or enable provenance capture temporarily
#' It may be necessary to disable provenance capture temporarily, for example when
#' record() is writting out a housekeeping file.
#' A state variable in the ".recordr" environment is used to
#' temporarily disable provenance capture so that housekeeping tasks
#' will not have provenance information recorded for them.
#' Return the state of provenance capture: TRUE is enalbed, FALSE is disabled
#' @param enable logical variable used to enable or disable provenance capture
#' @return enabled a logical indicating the state of provenance capture: TRUE=enabled, FALSE=disabled
#' @author slaughter
#' @export
setProvCapture <- function(enable) {
  # If the '.recordr' environment hasn't been created, then we are calling this
  # function outside the context of record(), so don't attempt to update the environment'
  if (is.element(".recordr", base::search())) {
    assign("provCaptureEnabled", enable, envir = as.environment(".recordr"))
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
  if (is.element(".recordr", base::search())) {
    if (exists("provCaptureEnabled", where = ".recordr", inherits = FALSE )) {
      enabled <- base::get("provCaptureEnabled", envir = as.environment(".recordr"))
    } else {
      enabled <- FALSE
    }
  } else {
    enabled <- FALSE
  }
  return(enabled)
}

#' Archive a file into the recordr archive directory
#' @param file The file to save in the archive
#' @return The name of the archived file path relative to the recordr home directory
#' @import uuid
#' @note This function is intended to run only during a record() session, i.e. the
#' recordr environment needs to be available.
archiveFile <- function(file) {
  if(!file.exists(file)) {
    message("Cannot copy file %s, it does not exist\n", file)
    return(NULL)
  }
  
  recordrEnv <- as.environment(".recordr") 
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
  # lookup performance is not an issue.
  archiveRelDir <- sprintf("archive/%s", substr(as.character(Sys.time()), 1, 10))
  fullDirPath <- sprintf("%s/%s", recordrEnv$recordr@recordrDir, archiveRelDir)
  if (!file.exists(fullDirPath)) {
    dir.create(fullDirPath, recursive = TRUE)
  }  
  archivedRelFilePath <- sprintf("%s/%s", archiveRelDir, UUIDgenerate())
  fullFilePath <- sprintf("%s/%s", recordrEnv$recordr@recordrDir, archivedRelFilePath)
  # First check if the file has already been archived by searching for a file
  # with the same sha256 checksum. Each archived file must have a unique name
  # as a filename may be an input for many runs on the same day, with the
  # file being updated between each run
  file.copy(file, fullFilePath, overwrite = FALSE, recursive = FALSE,
            copy.mode = TRUE, copy.date = TRUE)
  # TODO: Check if the file was actually copied
  return(archivedRelFilePath)
}
