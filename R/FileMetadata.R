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

#' A class containing information about a file or group of files
#' @details This class is used internally by the recordr package.
#' @rdname FileMetadata-class
#' @aliases FileMetadata-class
#' @slot fileId
#' @slot executionId
#' @slot filePath
#' @slot sha256
#' @slot size
#' @slot user
#' @slot createTime
#' @slot modifyTime
#' @slot access
#' @slot format
#' @slot archivedFilePath
#' @import tools
#' @include Recordr.R
#' @section Methods:
#' \itemize{
#'  \item{\code{\link[=initialize-FileMetadata]{initialize}}}{: Initialize a FileMetadata object}
#'  \item{\code{\link{readFileMeta}}}{: Retrieve saved file metadata for one or more files}
#'  \item{\code{\link{writeFileMeta}}}{: Save metadata for a single file.}
#' }
#' @seealso \code{\link{recordr}}{ package description.}
setClass("FileMetadata", slots = c(fileId = "character",
                                   executionId = "character",
                                   filePath    = "character",
                                   sha256      = "character",
                                   size        = "numeric",
                                   user        = "character",
                                   createTime  = "character",
                                   modifyTime  = "character",
                                   access      = "character",
                                   format      = "character",
                                   archivedFilePath = "character")
         )

#' Initialize a file metadata object.
#' @details This method is used internally by the recordr package.
#' @param .Object a \code{"FileMetdata"} object
#' @param file a \code{"character"}, a file to acquire metadata for
#' @param fileId a \code{"character"}, the unique identifier for this FileMeta object
#' @param sha256 a \code{"character"}, the checksum for the file
#' @param size a \code{"numeric"}, size in bytes of the file
#' @param user a \code{"character"}, the user that owns the file
#' @param createTime a \code{"character"}, the creation time of the file
#' @param modifyTime a \code{"character"}, the modification time of the file
#' @param executionId a \code{"character"}, the executionId associated with this FileMeatadata object
#' @param access \code{"character"}, the access that occurred for this file ("read", "write", "execute")
#' @param format a \code{"character"}, the format type associate with the file, e.g. "text/csv"
#' @param archivedFilePath a \code{"character"}, the file path of the file 
#' @rdname initialize-FileMetadata
#' @aliases initialize-FileMetadata
#' @import uuid
#' @import digest
#' @seealso \code{\link[=FileMetadata-class]{FileMetadata}} { class description}
setMethod("initialize", signature = "FileMetadata", definition = function(.Object, file,
                                                                          fileId=as.character(NA),
                                                                          sha256=as.character(NA),
                                                                          size=as.numeric(0),
                                                                          user=as.character(NA),
                                                                          createTime=as.character(NA),
                                                                          modifyTime=as.character(NA),
                                                                          executionId,
                                                                          access=as.character(NA),
                                                                          format=as.character(NA),
                                                                          archivedFilePath=as.character(NA)) {

  # Get info for this file, unless provided in argument list. 
  # file.info stores dates as POSIXct, so convert them to strings
  # so that they don't get written out as an integer timestamp, i.e. milliseconds since ref date
  # 'file' argument is required and can be a URL, so no file info available and should be provided on command 
  # line, if possible
  if(file.exists(file)) {
    filePath <- normalizePath(file)
    fpInfo <- file.info(filePath)
  } else {
    filePath <- file
  }
  .Object@filePath <- filePath
  
  .Object@sha256 <- sha256
  if(is.na(sha256)) {
    if(file.exists(filePath)) .Object@sha256 <- as.character(digest::digest(object=filePath, algo="sha256", file=TRUE)[[1]])
  }
  
  .Object@size <- size
  if(size == 0) {
    if(file.exists(filePath)) .Object@size <- as.numeric(fpInfo[["size"]])
  } 
  
  .Object@user <- user
  if(is.na(user)) {
    if(file.exists(filePath)) .Object@user <- as.character(fpInfo[["uname"]])
  }
  
  .Object@createTime <- createTime
  if(is.na(createTime)) {
    if(file.exists(filePath)) .Object@createTime <- as.character(fpInfo[["ctime"]])
  }
  
  .Object@modifyTime <- modifyTime
  if(is.na(modifyTime)) {
    if(file.exists(filePath)) .Object@modifyTime <- as.character(fpInfo[["mtime"]])
  }
  
  # Required field, so it must have been specified as an argument
  .Object@executionId <- executionId
  
  .Object@fileId <- fileId
  if(is.na(fileId)) {
    .Object@fileId <- sprintf("urn:uuid:%s", UUIDgenerate())
  } 
  .Object@access <- access
  .Object@format <- format
  .Object@archivedFilePath <- archivedFilePath
  return(.Object)
})
# 
# ##########################
# ## Methods
# ##########################
# 
#' Save metadata for a single file.
#' @description Metadata for a file is written to an RSQLite database.
#' @details This method is used internally by the recordr package.
#' @param recordr A recordr object
#' @param fileMeta A fileMetadata object
#' @param ... (Not yet used)
#' @seealso \code{\link[=FileMetadata-class]{FileMetadata}} { class description}
setGeneric("writeFileMeta", function(recordr, fileMeta, ...) {
  standardGeneric("writeFileMeta")
})

#' @rdname writeFileMeta
setMethod("writeFileMeta", signature("Recordr", "FileMetadata"), function(recordr, fileMeta, ...) {
  
  # Check if the connection to the database is still working
  tmpDBconn<- FALSE
  if (!dbIsValid(recordr@dbConn)) {
    dbConn <- getDBconnection(dbFile=recordr@dbFile)
    if(is.null(dbConn)) {
      stop(sprintf("Error reconnecting to database file %s\n", recordr@dbFile))
    }
  } else {
    dbConn <- recordr@dbConn
  } 
  
  # Get the database connection and chek if the filemeta table exists.
  if (!is.element("filemeta", dbListTables(dbConn))) {
    createStatement <- "CREATE TABLE filemeta
            (fileId     TEXT PRIMARY KEY,
            executionId TEXT not null,
            filePath    TEXT not null,
            sha256      TEXT not null,
            size        INTEGER not null,
            user        TEXT not null,
            modifyTime  TEXT not null,
            createTime  TEXT not null,
            access      TEXT not null,
            format      TEXT,
            archivedFilePath TEXT,
            foreign key(executionId) references execmeta(executionId),
            unique(fileId));"
    
    #cat(sprintf("create: %s\n", createStatement))
    result <- dbSendQuery(conn=dbConn, statement=createStatement)
    dbClearResult(result)
  }
  
  fileSlotNames <- slotNames("FileMetadata")
  # Get slot types
  slotDataTypes <- getSlots("FileMetadata")
  # Get values from all the slots for the file metadata, in the order they were declared in the class definition.
  #slotValues <- unlist(lapply(fileSlotNames, function(x) as.character(slot(fileMeta, x))))
  slotValues <- unlist(lapply(fileSlotNames, function(x) slot(fileMeta, x)))
  slotValuesStr <- NULL
  fileSlotNamesStr <- paste(fileSlotNames, collapse=",")
  # SQLite doesn't like the 'fancy' quotes that R uses for output, so switch to standard quotes
  quoteOption <- getOption("useFancyQuotes")
  options(useFancyQuotes="FALSE")
  for (i in 1:length(fileSlotNames)) {
    slotName <- fileSlotNames[[i]]
    slotDataType <- slotDataTypes[[i]]
    # Surround character values in single quotes for 'insert' statement 
    if(slotDataType=="character") {
      slotValuesStr <- ifelse(is.null(slotValuesStr),  
                              slotValuesStr <- sQuote(slotValues[i]), 
                              slotValuesStr <- paste(slotValuesStr, sQuote(slotValues[i]), sep=","))
    } else if (slotName == "size") {
      # Size is a number, so shouldn't need quotes.
      # However, make sure the size isn't inserted into the db in scientific notation format
      slotValuesStr <- paste(slotValuesStr, format(as.numeric(slotValues[i]), scientific=FALSE), sep=",")
    } else {
      # All other datatypes don't get quotes
      slotValuesStr <- ifelse(is.null(slotValuesStr),  
                              slotValuesStr <- slotValues[i], 
                              slotValuesStr <- paste(slotValuesStr, slotValues[i], sep=","))
    }
  }
  options(useFancyQuotes=quoteOption)
  
  insertStatement <- paste("INSERT INTO filemeta ", "(", fileSlotNamesStr, ")", "VALUES (", slotValuesStr, ")", sep=" ")
  #cat(sprintf("insert: %s\n", insertStatement))
  result <- dbSendQuery(conn=dbConn, statement=insertStatement)
  dbClearResult(result)
  # We can't return this database connection we just opened, as we are not returning the recordr object with a
  # slot that contains the new database connection, so just disconnect. 
  if(tmpDBconn) dbDisconnect(dbConn)
  # TODO: interpret result status and set true or false
  return(TRUE)
})

#' Retrieve saved file metadata for one or more files
#' @description File metadata is retrived from the recordr database table \emph{filemeta}
#' based on search parameters.
#' @details This method is used internally by the recordr package.
#' @param recordr A recordr object
#' @param ... Additional parameters
#' @seealso \code{\link[=FileMetadata-class]{FileMetadata}} { class description}
#' @export
setGeneric("readFileMeta", function(recordr, ...) {
  standardGeneric("readFileMeta")
})

#' @rdname readFileMeta
#' @param fileId The id of the file to search for
#' @param executionId A character value that specifies an execution identifier to search for.
#' @param filePath The path name of the file to search for.
#' @param sha256 The sha256 checksum value for the uncompressed file.
#' @param user The user that ran the execution that created or accessed the file.
#' @param access The type of access for the file. Values include "read", "write", "execute"
#' @param format The format type of the object, e.g. "text/plain"
#' @param orderBy The column to sort the result set by.
#' @param sortOrder The sort type. Values include ("ascending", "descending")
#' @param delete a \code{"logical"}, if TRUE, the selected file entries are deleted (default: FALSE).
#' @return A dataframe containing file metadata objects
setMethod("readFileMeta", signature("Recordr"), function(recordr, 
                                    fileId=as.character(NA),  executionId=as.character(NA), 
                                    filePath=as.character(NA),  sha256=as.character(NA), 
                                    user=as.character(NA),  access=as.character(NA), format=as.character(NA),
                                    orderBy=as.character(NA), sortOrder="ascending", delete=FALSE, ...) {
  
  # Check if the connection to the database is still working
  tmpDBconn<- FALSE
  if (!dbIsValid(recordr@dbConn)) {
    dbConn <- getDBconnection(dbFile=recordr@dbFile)
    if(is.null(dbConn)) {
      stop(sprintf("Error reconnecting to database file %s\n", recordr@dbFile))
    }
  } else {
    dbConn <- recordr@dbConn
  }
  # If the 'execmeta' table doesn't exist yet, then there is no exec metadata for this
  # executionId, so just return a blank data.frame
  if (!is.element("filemeta", dbListTables(dbConn))) {
    return(data.frame())
  }
  
  # Construct a SELECT statement to retrieve the runs that match the specified search criteria.
  select <- "SELECT * from filemeta"
  whereClause <- NULL
  # Is the column that the user specified for ordering correct?
  orderByClause <- ""
  if (!is.na(orderBy)) {
    # Change the requested ordering statement to valid SQLite
    if(tolower(sortOrder) %in% c("descending", "desc")) {
      sortOrder <- "DESC"
    } else {
      sortOrder <- "ASC"
    }
    orderByClause <- sprintf("order by %s %s", orderBy, sortOrder)
  }
  
  if(!is.na(fileId)) {
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and fileId=\'%s\'", whereClause, fileId)
    } else {
      whereClause <- sprintf(" where fileId=\'%s\'", fileId)
    }
  }
  
  if(!is.na(executionId)) {
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and executionId=\'%s\'", whereClause, executionId)
    } else {
      whereClause <- sprintf(" where executionId=\'%s\'", executionId)
    }
  }
  
  if(!is.na(sha256)) {
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and sha256=\'%s\'", whereClause, sha256)
    } else {
      whereClause <- sprintf(" where sha256=\'%s\'", sha256)
    }
  }

  if(!is.na(filePath)) { 
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and filePath like \'%%%s%%\'", whereClause, filePath)
    } else {
      whereClause <- sprintf(" where filePath like \'%%%s%%\'", filePath)
    }
  }

  if(!is.na(user)) { 
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and user like \'%%%s%%\'", whereClause, user)
    } else {
      whereClause <- sprintf(" where user like \'%%%s%%\'", user)
    }
  }
  
  if(!is.na(access)) { 
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and access = \'%s\'", whereClause, access)
    } else {
      whereClause <- sprintf(" where access = \'%s\'", access)
    }
  }
  if(!is.na(format)) { 
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and format = \'%s\'", whereClause, format)
    } else {
      whereClause <- sprintf(" where format = \'%s\'", format)
    }
  }
  # If the user specified 'delete=TRUE', so first fetch the
  
  # matching records, then delete them.
  if (delete) {
    # Don't allow the user to delete all records unless they specify at
    # least one search term, possibly with a wildcard that will match
    # all records.
    if (is.null(whereClause)) {
      message("Deleting all records is not allowed unless at least one search term is supplied.") 
      if(tmpDBconn) dbDisconnect(dbConn)
      return(data.frame())
    } 
  }
  
  # Retrieve records that match search criteria
  selectStatement <- paste(select, whereClause, orderByClause, sep=" ")
  #cat(sprintf("select: %s\n", selectStatement))
  result <- dbSendQuery(conn = dbConn, statement=selectStatement)
  resultdf <- dbFetch(result, n=-1)
  dbClearResult(result)
  
  # Now delete records if requested.
  if(delete) {
    deleteStatement <- paste("DELETE from filemeta ", whereClause, sep=" ")
    result <- dbSendQuery(conn=dbConn, statement=deleteStatement)
    dbClearResult(result)
  }
  
  # We can't return this database connection we just opened, as we are not returning the recordr object with a
  # slot that contains the new database connection, so just disconnect. 
  if(tmpDBconn) dbDisconnect(dbConn)
  return(resultdf)
})
