
# This R script implements the "Load" phase of the data pipeline. 
# It incorporates the **Environment Variable** security method you selected 
# and includes the robust safety checks (idempotency, referential integrity, 
# and transactions) discussed in the architecture sources.

### Prerequisite: Setting up the Environment Variable
#    Before running this code, you must define your password so R can 
#    find it without it being written in the script.
#     1.  In RStudio, run: `usethis::edit_r_environ()`
#     2.  In the file that opens, add this line: `TWQD_PASSWORD=YourActualPasswordHere`
#     3.  Save the file and **restart RSession** (Ctrl+Shift+F10).


  
  ### R Script: TWQD Robust Data Loader
  

# ==============================================================================
# TWQD ROBUST DATA LOADER (database Loading Phase)
# Purpose: Safely upload Activity and Result CSVs to SQL Server
# Features: Atomic Transactions, Idempotency (Duplicate checks), Logging
# ==============================================================================

# 1. SETUP & LIBRARIES ----------------------------------------------------
library(DBI)
library(odbc)
library(tidyverse)
library(readr)

# Define Log File
log_file <- paste0("Log_DataLoad_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".txt")

# Helper Function: Logging
write_log <- function(message) {
  timestamped_msg <- paste0("[", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "] ", message)
  cat(timestamped_msg, "\n")
  write(timestamped_msg, file = log_file, append = TRUE)
}

write_log("INITIATING DATA LOAD PROCESS...")

# 2. DATABASE CONNECTION (SECURE) -----------------------------------------
# Uses Sys.getenv to retrieve password from .Renviron file
tryCatch({
  con <- dbConnect(odbc::odbc(),
                   Driver = "SQL Server",
                   Server = "SEASTAR\\SQLEXP2017",
                   Database = "TWQD",
                   UID = "sa",             # Adjust if using Windows Auth
                   PWD = Sys.getenv("TWQD_PASSWORD")) # Secure retrieval
  write_log("SUCCESS: Connected to TWQD Database.")
}, error = function(e) {
  write_log(paste("FATAL ERROR: Connection failed.", e$message))
  stop("Script aborted due to connection failure.")
})

##-----Alt connection
# Connect to TWQD
con <- dbConnect(odbc::odbc(),"twqd_test",
                 Database = "TWQD")

# 3. LOAD IMPORT FILES ----------------------------------------------------
# Update these filenames to match the specific files generated in the "Factory" step
file_activity <- "Import_Sktrends_Activity_20260110_R.csv" 
file_result   <- "Import_Sktrends_Result_20260110_R.csv"

if(file.exists(file_activity) & file.exists(file_result)) {
  # Read CSVs - forcing all columns to character initially to prevent type mismatches
  # We will coerce specific types for SQL later
  new_activities <- read_csv(file_activity, col_types = cols(.default = "c"))
  new_results    <- read_csv(file_result, col_types = cols(.default = "c"))
  
  write_log(paste("Loaded Input Files. Activities:", nrow(new_activities), 
                  "Results:", nrow(new_results)))
} else {
  stop("FATAL: Input CSV files not found. Check filenames.")
}

# 4. PRE-FLIGHT CHECKS (REFERENTIAL INTEGRITY) ----------------------------
# Source: Project and MonLoc must exist before Activity can be loaded.

# A. Check Project Existence
req_project <- unique(new_activities$ProjectIdentifier)
db_projects <- dbGetQuery(con, "SELECT ProjectIdentifier FROM Project")

if(!all(req_project %in% db_projects$ProjectIdentifier)) {
  write_log("FATAL ERROR: ProjectIdentifier in upload file does not exist in TWQD.")
  dbDisconnect(con)
  stop("Fix Project ID in source data.")
}

# B. Check Monitoring Location Existence
req_sites <- unique(new_activities$MonitoringLocationIdentifier)
db_sites  <- dbGetQuery(con, "SELECT MonitoringLocationIdentifier FROM MonitoringLocation")

missing_sites <- setdiff(req_sites, db_sites$MonitoringLocationIdentifier)

if(length(missing_sites) > 0) {
  write_log(paste("FATAL ERROR: The following Sites are missing in TWQD:", 
                  paste(missing_sites, collapse=", ")))
  dbDisconnect(con)
  stop("Missing Monitoring Locations. Upload these to MonLoc table first.")
}

write_log("PASSED: Pre-flight integrity checks (Project & MonLocs exist).")

# 5. IDEMPOTENCY CHECK (PREVENT DUPLICATES) -------------------------------
# Source: "Force the worker to look at the shelf first."

# Fetch existing Activity IDs from Database
existing_ids <- dbGetQuery(con, "SELECT ActivityIdentifier FROM Activity")

# Filter upload dataset to only NEW records
# anti_join removes rows from 'new_activities' that match 'existing_ids'
unique_activities <- new_activities %>% 
  anti_join(existing_ids, by = "ActivityIdentifier")

# Filter results to match only the unique activities we are about to upload
unique_results <- new_results %>%
  semi_join(unique_activities, by = "ActivityIdentifier")

count_dropped <- nrow(new_activities) - nrow(unique_activities)
write_log(paste("IDEMPOTENCY CHECK:", count_dropped, 
                "Activities already exist and were dropped."))
write_log(paste("READY TO LOAD:", nrow(unique_activities), "Activities and", 
                nrow(unique_results), "Results."))

if(nrow(unique_activities) == 0) {
  write_log("No new data to upload. Script finishing.")
  dbDisconnect(con)
  quit()
}

# 6. TRANSACTIONAL UPLOAD (ATOMIC COMMIT) ---------------------------------
# Source: Activity is Parent, Result is Child. 
# We use dbBegin/dbCommit to ensure ALL data loads, or NONE loads.

# A. Format Data Types for SQL 
# Source: SQL expects specific types.
unique_activities$StartDate <- as.Date(unique_activities$StartDate)
unique_results$ResultMeasureValue <- as.character(unique_results$ResultMeasureValue) # Prevents decimal truncation

dbBegin(con) # Start Transaction

tryCatch({
  # Step 1: Write Activities
  dbWriteTable(con, "Activity", unique_activities, append = TRUE, row.names = FALSE)
  write_log(paste("Staged", nrow(unique_activities), "rows into Activity Table."))
  
  # Step 2: Write Results
  dbWriteTable(con, "Result", unique_results, append = TRUE, row.names = FALSE)
  write_log(paste("Staged", nrow(unique_results), "rows into Result Table."))
  
  # Step 3: Commit (Save)
  dbCommit(con)
  write_log("SUCCESS: Transaction Committed. Data is live in TWQD.")
  
}, error = function(e) {
  # If ANY error occurs above, this block runs
  dbRollback(con) # Undo everything
  write_log(paste("TRANSACTION FAILED & ROLLED BACK. Error:", e$message))
  message("Database was not modified.")
})

# 7. CLEANUP --------------------------------------------------------------
dbDisconnect(con)
write_log("Connection Closed. Process Complete.")

### Key Features Explained
#
#   1.  **Environment Variables:** Line 29 uses `Sys.getenv("TWQD_PASSWORD")`. 
#       This ensures that if you share this script file with a colleague, 
#        you are not handing them your database password.
#   2.  **Referential Integrity (Pre-Flight):** Lines 53-73 verify that the 
#        "Parents" (Project and MonLoc) exist before we try to load the 
#        "Children" (Activity). This prevents obscure SQL Key Violation errors 
#        that are hard to debug.
#   3.  **Idempotency (The "Warehouse Check"):** Lines 77-83 query the database
#        for *existing* IDs and remove them from your upload set. This allows 
#        you to run this script multiple times safely. If you run it twice on 
#        the same day, the second run will simply say "0 records to load" rather
#        than crashing with "Primary Key Violation."
#   4.  **Transaction Management:** Lines 103-125 wrap the write operations. If 
#        the **Activity** load succeeds but the **Result** load fails (e.g., due
#        to a data type error), `dbRollback(con)` cancels the Activity load. 
#        This prevents "Orphaned Activities" (events with no results) from 
#        polluting your database.




