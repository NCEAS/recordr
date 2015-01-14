# Define constants from the Prov Ontology (http://www.w3.org/TR/prov-dm)
provNS <- "http://www.w3.org/ns/prov#"
provQualifiedAssociation <- sprintf("%s%s", provNS, "qualifiedAssociation")
provWasDerivedFrom       <- sprintf("%s%s", provNS, "wasDerivedFrom")
provHadPlan              <- sprintf("%s%s", provNS, "hadPlan")
provUsed                 <- sprintf("%s%s", provNS, "used")
provWasGeneratedBy       <- sprintf("%s%s", provNS, "wasGeneratedBy")
provAssociation          <- sprintf("%s%s", provNS, "wasAssociatedWith")
provAgent                <- sprintf("%s%s", provNS, "Agent")

# Define constants from the ProvONE Ontology
provONE_NS               <- "http://purl.org/provone/"
provONEprogram           <- sprintf("%s%s", provONE_NS, "Program")
provONEexecution         <- sprintf("%s%s", provONE_NS, "Execution")
