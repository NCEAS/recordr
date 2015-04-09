# KNB The identifier of the DataONE Member Node server used as a read only source to retrieve files.
source_member_node_id <- "urn:node:KNB"
# The identifier of the DataONE Member Node server used as a read or write target for files.
target_member_node_id <- "urn:node:testKNB"
# DataONE environment
dataone_env <- "PROD"
# The base URL of the DataONE Coordinating Node server.
coordinating_node_base_url <- "https://cn-stage-2.test.dataone.org/cn/v1/node"
# The default object format identifier when creating system metadata and uploading files to a Member Node. Defaults to application/octet-stream.
format_id <- "application/octet-stream"
# The DataONE Subject DN string of account uploading the file to a Member Node.
submitter <- "CN=Peter Slaughter A10499,O=Google,C=US,DC=cilogon,DC=org"
# The DataONE Subject DN string of account with read, write, and changePermission permissions for the file being uploaded.
rights_holder <- "CN=Peter Slaughter A10499,O=Google,C=US,DC=cilogon,DC=org"
# Allow public read access to uploaded files. Defaults to true.
public_read_allowed <- TRUE
# Allow replication of files to preserve the integrity of the data file over time.
replication_allowed <- TRUE
# The desired number of replicas of each file uploaded to the DataONE network.
number_of_replicas <- 2
# A comma-separated list of Member Node identifiers that are preferred for replica storage.
preferred_replica_node_list <- c("urn:node:mnDemo9", "urn:node:testKNB")
# A comma-separated list of Member Node identifiers that are blocked from replica storage.
blocked_replica_node_list <- NULL
# The researcher's ORCID identifier from http://orcid.org. Identity information found via the ORCID API will populate or override other identity fields as appropriate.
orcid_identifier <- "orcid.org/0000-0002-2192-403X"
# The researcher's DataONE Subject as a Distinguished Name string. If not set, defaults to the Subject DN found in the CILogon X509 certificate at the given certificate path.
subject_dn <- "CN=Peter Slaughter A10499,O=Google,C=US,DC=cilogon,DC=org"
# The absolute file system path to the X509 certificate downloaded from https://cilogon.org. The path includes the file name itself.
certificate_path <- "/tmp/x509up_u501"
# The Friend of a friend 'name' vocabulary term as defined at http://xmlns.com/foaf/spec/, typically the researchers given and family name together.
foaf_name <- "Peter Slaughter"
# The directory used to store per execution provenance information. Defaults to '~/.recordr/runs
provenance_storage_directory <- "~/.recordr"
# When set to true, provenance capture will be triggered when reading from files based on specific read commands in the scripting language. Default: true
capture_file_reads <- TRUE
# When set to true, provenance capture will be triggered when writing to files based on specific write commands in the scripting language. Default: true
capture_file_writes <- TRUE
# When set to true, provenance capture will be triggered when reading from DataONE MNRead.get() API calls. Default: true
capture_dataone_reads <- TRUE
# When set to true, provenance capture will be triggered when writing with DataONE MNStorage.create() or MNStorage.update() API calls. Default: true
capture_dataone_writes <- TRUE
# When set to true, provenance capture will be triggered when encountering YesWorkflow inline comments. Default: true
capture_yesworkflow_comments <- TRUE