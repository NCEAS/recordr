#
#   This work was created by participants in the DataONE project, and is
#   jointly copyrighted by participating institutions in DataONE. For
#   more information on DataONE, see our web site at http://dataone.org.
#
#     Copyright 2016
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
#' @rdname ProvRels-class
#' @aliases ProvRels-class
#' @slot executionId a \code{characgter} containing the identifier associated with the file entry
#' @slot subject a \code{character} containing the subject of a provenance relationship
#' @slot predicate a \code{character} containign the predicate of a provenance relationship
#' @slot object a \code{character} containing the object of a provenance relationship
#' @slot subjectType, a \code{character} containing the RDF node type of the the subject, values can be 'uri', 'blank'
#' @slot objectType a \code{character} containign the RDF node type of the object, each value can be 'uri', 'blank', or 'literal'
#' @slot dataTypeURI The RDF data type that specifies the type of the object
#' @include Recordr.R
#' @section Methods:
#' \itemize{
#'  \item{\code{\link[=initialize-ProvRels]{initialize}}}{: Initialize a ProvRels object}
#'  \item{\code{\link{readProvRels}}}{: Retrieve saved provenance relationships.}
#'  \item{\code{\link{writeProvRel}}}{: Save a provenance relationship.}object
#' }
#' @seealso \code{\link{recordr}}{ package description.}
setClass("ProvRels", slots = c(executionId = "character",
                                   subject     = "character",
                                   predicate   = "character",
                                   object      = "character",
                                   subjectType = "character",
                                   objectType  = "character",
                                   dataTypeURI = "character")
                                   
)

#' Initialize a provenance relationship object.
#' @details This method is used internally by the recordr package.
#' @param .Object a \code{"ProvRels"} object
#' @param executionId a \code{"character"}, the execution identifier for an execution
#' @param subject a \code{"character"}, the<BS>subject of the provenance relationship
#' @param predicate a \code{"character"}, the predicate of the provenance relationship
#' @param object a \code{"character"}, the object of the provenance relationship
#' @param subjectType a \code{"character"}, the RDF node type of the subject, i.e. "url", "blank"
#' @param objectType a \code{"character"}, the RDF node type of the object i.e. "url", "blank"
#' @param dataTypeURI a \code{"character"}, the RDF data type of the object, i.e. "xsd:string"
#' @rdname initialize-ProvRels
#' @aliases initialize-ProvRels
#' @import uuid
#' @import digest
#' @seealso \code{\link[=ProvRels-class]{ProvRels}} { class description}
setMethod("initialize", signature = "ProvRels", definition = function(.Object, executionId=as.character(NA),
                                                                          subject=as.character(NA),
                                                                          predicate=as.character(NA),
                                                                          object=as.character(0),
                                                                          subjectType=as.character(NA),
                                                                          objectType=as.character(NA),
                                                                          dataTypeURI=as.character(NA)) {
  
  .Object@executionId <- executionId
  .Object@subject <- subject
  .Object@predicate <- predicate
  .Object@object <- object
  .Object@subjectType <- subjectType
  .Object@objectType <- objectType
  .Object@dataTypeURI <- dataTypeURI
  return(.Object)
})
# 
# ##########################
# ## Methods
# ##########################
# 
#' Save a single provenance relationship.
#' @description Metadata for a provenance relationship is written to the recordr RSQLite database.
#' @details This method is used internally by the recordr package.
#' @param recordr A recordr object
#' @param provRels A ProvRels object.
#' @param ... (Not yet used)
#' @seealso \code{\link[=ProvRels-class]{ProvRels}} { class description}
setGeneric("writeProvRel", function(recordr, provRels, ...) {
  standardGeneric("writeProvRel")
})

#' @rdname writeProvRel
setMethod("writeProvRel", signature("Recordr"), function(recordr, provRels, ...) {
  
  # Check if the connection to the database is still working
  tmpDBconn<- FALSE
  if (!dbIsValid(recordr@dbConn)) {
    dbConn <- getDBconnection(dbFile=recordr@dbFile)
    if(is.null(dbConn)) {
      stop(sprintf("Error reconnecting to database file %s\n", recordr@dbFile))
    }
    tmpDBconn <- TRUE
  } else {
    dbConn <- recordr@dbConn
  } 
  
  # Get the database connection and check if the filemeta table exists.
  if (!is.element("provenance", dbListTables(dbConn))) {
    createStatement <- "CREATE TABLE provenance
    (executionId TEXT not null,
    subject     TEXT not null,
    predicate   TEXT not null,
    object      TEXT not null,
    subjectType TEXT,
    objectType  TEXT,
    dataTypeURI TEXT,
    foreign key(executionId) references execmeta(executionId));"
    
    #cat(sprintf("create: %s\n", createStatement))
    result <- dbSendQuery(conn=dbConn, statement=createStatement)
    dbClearResult(result)
  }
  
  
  
  # Construct the insert statement, getting the column names from the class slot names, and determining
  # the values to insert from the slots. Each value is single quoted.
  provRelsSlotNames <- slotNames("ProvRels")
  # Get slot types
  slotDataTypes <- getSlots("ProvRels")
  # Get values from all the slots for the execution metadata, in the order they were declared in the class definition.
  slotValues <- unlist(lapply(provRelsSlotNames, function(x) as.character(slot(provRels, x))))
  slotValuesStr <- NULL
  provRelsSlotNamesStr <- paste(provRelsSlotNames, collapse=",")
  # SQLite doesn't like the 'fancy' quotes that R uses for output, so switch to standard quotes
  quoteOption <- getOption("useFancyQuotes")
  options(useFancyQuotes="FALSE")
  for (i in 1:length(provRelsSlotNames)) {
    slotName <- provRelsSlotNames[[i]]
    slotDataType <- slotDataTypes[[i]]
    if(slotDataType =="character") {
      # if single quotes already exist in the string, then double them up, which 
      # is the way to escape them in SQLite!
      if (grepl("'", slotValues[i])) {
        thisValue <- gsub("'", "''", slotValues[i])
      } else {
        thisValue <- slotValues[i]
      }
      # If the character value is unassinged, write it out as NULL
      if(is.na(thisValue)) {
        slotValuesStr <- ifelse(is.null(slotValuesStr),  
                                slotValuesStr <- "NULL", 
                                slotValuesStr <- paste(slotValuesStr, "NULL", sep=","))
      } else {
        slotValuesStr <- ifelse(is.null(slotValuesStr),  
                                slotValuesStr <- sQuote(thisValue), 
                                slotValuesStr <- paste(slotValuesStr, sQuote(thisValue), sep=","))
      }
    } else if (slotDataType=="logical") {
      # Logical values are actually stored as integers in SQLite
      ifelse(slotValues[i], thisValue <- 1, thisValue <- 0)
      slotValuesStr <- ifelse(is.null(slotValuesStr),  
                              slotValuesStr <- thisValue, 
                              slotValuesStr <- paste(slotValuesStr, thisValue, sep=","))
    } else {
      # All other datatypes don't get quotes
      slotValuesStr <- ifelse(is.null(slotValuesStr),  
                              slotValuesStr <- slotValues[i], 
                              slotValuesStr <- paste(slotValuesStr, slotValues[i], sep=","))
    }
  }
  insertStatement <- paste("INSERT INTO provenance ", "(", provRelsSlotNamesStr, ")", " VALUES (", slotValuesStr, ")", sep=" ")
  result <- dbSendQuery(conn=dbConn, statement=insertStatement)
  dbClearResult(result)
  
  # We can't return this database connection we just opened, as we are not returning the recordr object with a
  # slot that contains the new database connection, so just disconnect. 
  if(tmpDBconn) dbDisconnect(dbConn)
  return(TRUE)
})

#' Retrieve saved file metadata for one or more files
#' @description File metadata is retrived from the recordr database table \emph{filemeta}
#' based on search parameters.
#' @details This method is used internally by the recordr package.
#' @param recordr A recordr object
#' @param ... Additional parameters
#' @seealso \code{\link[=ProvRels-class]{ProvRels}} { class description}
setGeneric("readProvRels", function(recordr, ...) {
  standardGeneric("readProvRels")
})

#' @rdname readProvRels
#' @param executionId A character value that specifies an execution identifier to search for.
#' @param subject The subject of the provenance relationships to match
#' @param predicate The predicate of the provenance relationships to match
#' @param object The object of the provenance relationships to match
#' @param subjectType A character value containing the subject type of the relationship to match
#' @param objectType A character value containing the object type of the relationship to match
#' @param dataTypeURI A character value containing the data type of the relationship to match
#' @param orderBy The column to sort the result set by.
#' @param sortOrder The sort type. Values include ("ascending", "descending")
#' @param delete a \code{"logical"}, if TRUE, the selected file entries are deleted (default: FALSE).
#' @return A dataframe containing file metadata objects
setMethod("readProvRels", signature("Recordr"), function(recordr,
                                                         executionId=as.character(NA), subject=as.character(NA),  
                                                         predicate=as.character(NA), object=as.character(NA),  
                                                         subjectType=as.character(NA), objectType=as.character(NA),  
                                                         dataTypeURI=as.character(NA),
                                                         orderBy=as.character(NA), sortOrder="ascending", delete=FALSE, ...) {
  
  # Check if the connection to the database is still working
  tmpDBconn<- FALSE
  if (!dbIsValid(recordr@dbConn)) {
    dbConn <- getDBconnection(dbFile=recordr@dbFile)
    if(is.null(dbConn)) {
      stop(sprintf("Error reconnecting to database file %s\n", recordr@dbFile))
    }
    tmpDBconn <- TRUE
  } else {
    dbConn <- recordr@dbConn
  }
  # If the 'provRels' table doesn't exist yet, then there is no provenance relationships for this
  # executionId, so just return a blank data.frame
  if (!is.element("provenance", dbListTables(dbConn))) {
    return(data.frame())
  }
  
  # Construct a SELECT statement to retrieve the runs that match the specified search criteria.
  select <- "SELECT * from provenance "
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
  
  if(!is.na(executionId)) {
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and executionId=\'%s\'", whereClause, executionId)
    } else {
      whereClause <- sprintf(" where executionId=\'%s\'", executionId)
    }
  }
  
  if(!is.na(subject)) {
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and subject=\'%s\'", whereClause, subject)
    } else {
      whereClause <- sprintf(" where subject=\'%s\'", subject)
    }
  }
  
  if(!is.na(predicate)) { 
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and predicate like \'%%%s%%\'", whereClause, predicate)
    } else {
      whereClause <- sprintf(" where predicate like \'%%%s%%\'", predicate)
    }
  }
  
  if(!is.na(object)) { 
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and object like \'%%%s%%\'", whereClause, object)
    } else {
      whereClause <- sprintf(" where object like \'%%%s%%\'", object)
    }
  }
    
  if(!is.na(subjectType)) { 
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and subjectType like \'%%%s%%\'", whereClause, subjectType)
    } else {
      whereClause <- sprintf(" where subjectType like \'%%%s%%\'", subjectType)
    }
  }
    
  if(!is.na(objectType)) { 
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and objectType like \'%%%s%%\'", whereClause, objectType)
    } else {
      whereClause <- sprintf(" where objectType like \'%%%s%%\'", objectType)
    }
  }
    
  if(!is.na(dataTypeURI)) { 
    if(!is.null(whereClause)) {
      whereClause <- sprintf(" %s and dataTypeURI like \'%%%s%%\'", whereClause, dataTypeURI)
    } else {
      whereClause <- sprintf(" where dataTypeURI like \'%%%s%%\'", dataTypeURI)
    }
  }
  
  # If the user specified 'delete=TRUE', first fetch the
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
    deleteStatement <- paste("DELETE from provenance ", whereClause, sep=" ")
    result <- dbSendQuery(conn=dbConn, statement=deleteStatement)
    dbClearResult(result)
  }
  
  # We can't return this database connection we just opened, as we are not returning the recordr object with a
  # slot that contains the new database connection, so just disconnect. 
  if(tmpDBconn) dbDisconnect(dbConn)
  return(resultdf)
})
