# ==============================================================================
# R Script: Transform Raw Trends Data to TWQD Format
# ------------------------------------------------------------------------------
# Purpose: Converts wide-format water quality data provided by Clallam County 
#          Streamkeepers into WQX-compliant 'Activity' and 'Result' tables.
#
# Sources Used: 
#   - SK_Trends_data2015-2024.txt [1]
#   - TrendsMonLocs [2]
#   - project-trends [3]
# ==============================================================================

# 1. SETUP & LIBRARIES ---------------------------------------------------------
#if (!require("tidyverse")) install.packages("tidyverse")
library(tidyverse)
library(lubridate)

# TWQD table values that constant for SK PIC trends
ORG_ID <- "ClallamCoDCD"
PROJECT_ID <- "ClallamTrendsPIC"
TIME_ZONE  <- "PST"


# --- INPUT FILES ---
# Raw Streamkeepers PIC Trends data source [1]
RAW_DATA_FILE <- "Raw_SKTrends_2015_2024.txt"
# Reference table to map Site Names to IDs
REF_MONLOCS   <- "trendsMonLocs.csv"   # source [2]

# --- OUTPUT FILE NAMING ---
# Format: [Action]_[Source]_[Destination]_[Date]_[Tool]
# Example: Import_Sktrends_Activity_20250127_R.csv
# Note ****** Import in this context is ready to be uploaded/imported into TWQD

run_date <- format(Sys.Date(), "%Y%m%d")

file_out_activity <- paste0("04_data_ready4TWQD/Import_Sktrends_Activity_", run_date, "_R.csv")
file_out_result   <- paste0("04_data_ready4TWQD/Import_Sktrends_Result_", run_date, "_R.csv")

print(paste("Outputs defined:", file_out_activity, "and", file_out_result))


# 2. DEFINE MAPPINGS -----------------------------------------------------------
# Create a lookup table to map Raw Headers to WQX Characteristics and Units
# Based on column headers found in Source [1]
char_map <- tribble(
  ~RawHeader,                             ~CharacteristicName,     ~ResultMeasureUnitCode,
  "Temperature, water",                   "Temperature, water",    "deg C",
  "pH",                                   "pH",                    "None",
  "Dissolved Oxygen",                     "Dissolved oxygen (DO)", "mg/l",
  "Specific Conductivity (at 25 deg C)",  "Specific conductance",  "mS/cm",
  "Salinity",                             "Salinity",              "ppt",
  "Turbidity",                            "Turbidity",             "FNU",
  "Fecal Coliform",                       "Fecal Coliform",        "cfu/100ml",
  "Nitrate as N",                         "Nitrate",               "mg/l",
  "Nitrite as N",                         "Nitrite",               "mg/l",
  "Ammonia (NH3) as Nitrogen (N)",        "Ammonia-nitrogen",      "mg/l",
  "Total Persulfate Nitrogen",            "Nitrogen",              "mg/l",
  "Total Persulfate Phosphorus",          "Phosphorus",            "mg/l",
  "Silicate as Si",                       "Silicate",              "mg/l",
  "Flow",                                 "Flow",                  "cfs",
  "Stream/River Stage",                   "Gage height",           "ft"
)  # ResultMeasureUnitCode inferred from RawHeader and knowing these are stream water samples
#  # except: Turbidity FNU from Joel Green Streamkeepers.

# 3. LOAD DATA INTO R STUDIO-----------------------------------------------------------------
# Reading text file [1]. Assuming tab-delimited based on source structure.
# You may need to adjust the path "SK_Trends_data2015-2024.txt"
raw_data <- read_tsv("01_Input_raw/Raw_SKTrends_2015_2024.txt", show_col_types = FALSE)

# Reading MonLocs to get IDs
mon_locs <- read_csv("03_TWQD_lookup/TrendsMonLocs.csv", show_col_types = FALSE) %>%
  select(MonitoringLocationName, MonitoringLocationIdentifier)

# 4. PREPARE MAIN DATA FRAME ---------------------------------------------------
# Join raw data with MonLoc IDs and format dates
processed_data <- raw_data %>%
  # Join to get the official ID (e.g., maps "Agnew Creek/Ditch 0.3" to "Ag Cr 0.3")
  left_join(mon_locs, by = c("Primary Name" = "MonitoringLocationName")) %>%
  mutate(
    # Parse date from "11/24/2015" format [1]
    StartDateObj = mdy(Arrival_Date),
    StartDateString = format(StartDateObj, "%Y-%m-%d"),
    
    # Generate Activity ID: [MonLoc]-[StartDate as YYYYMMDD]-[Type]
    ActivityIdentifier = paste0(
      MonitoringLocationIdentifier, "-",
      format(StartDateObj, "%Y%m%d"), 
      "-SR" # SR for Sample-Routine
    ),
    
    # Assign Constants
    OrganizationIdentifier = ORG_ID,
    ProjectIdentifier = PROJECT_ID,
    ActivityTypeCode = "Sample-Routine",
    ActivityMediaName = "Water",
    StartTime = "12:00:00",
    SubmitData = 1
  ) %>%
  # Filter out rows where Location mapping failed (if any)
  filter(!is.na(MonitoringLocationIdentifier))

# 5. GENERATE ACTIVITIES TABLE -------------------------------------------------
# Select distinct events (one row per location per date)
activities_table <- processed_data %>%
  select(
    ActivityIdentifier,
    ActivityTypeCode,
    ActivityMediaName,
    OrganizationIdentifier,
    StartDate = StartDateString,
    StartTime,
    ProjectIdentifier,
    MonitoringLocationIdentifier,
    SubmitData
  ) %>%
  distinct() # Remove duplicates

# Check row count
message(paste("Generated", nrow(activities_table), "Activities."))

# 6. GENERATE RESULTS TABLE ----------------------------------------------------
# Transform "Wide" to "Long" using pivot_longer

results_table <- processed_data %>%
  # 1. Select ID columns and the measurement columns defined in our map
  select(ActivityIdentifier, OrganizationIdentifier, any_of(char_map$RawHeader)) %>%
  
  # 2. FIX: Force all measurement columns to Character type to avoid pivot errors
  # This handles columns that R read as text due to symbols like "-" or "<"
  mutate(across(any_of(char_map$RawHeader), as.character)) %>%
  
  # 3. Pivot: Turn column headers into a "Characteristic" column
  pivot_longer(
    cols = any_of(char_map$RawHeader),
    names_to = "RawHeader",
    values_to = "ResultMeasureValue_Raw" # Temp name to distinguish from final
  ) %>%
  
  # 4. Join with the mapping table to get WQX Names and Units
  inner_join(char_map, by = "RawHeader") %>%
  
  # 5. Clean Data: Convert values back to numeric
  # This converts "-" or text to NA (which we filter out)
  mutate(
    ResultMeasureValue = as.numeric(ResultMeasureValue_Raw)
  ) %>%
  
  # 6. Remove rows with no valid numeric data (NA)
  filter(!is.na(ResultMeasureValue)) %>%
  
  # 7. Add Result Constants
  mutate(
    ResultStatusName = "Final",
    ResultValueTypeName = "Actual",
    SubmitData = 1
  ) %>%
  
  # 8. Select final columns in correct order for TWQD
  select(
    ActivityIdentifier,
    OrganizationIdentifier,
    CharacteristicName,
    ResultMeasureValue,
    ResultMeasureUnitCode,
    ResultStatusName,
    ResultValueTypeName,
    SubmitData
  )


# Check row count
message(paste("Generated", nrow(results_table), "Results."))

# 7. EXPORT TO CSV -------------------------------------------------------------
#run_date <- format(Sys.Date(), "%Y%m%d")
#write_csv(activities_table, "Import_Activities_R.csv", na = "")
#write_csv(results_table, "Import_Results_R.csv", na = "")

# --- Export ---
write_csv(activities_table, file_out_activity, na = "")
write_csv(results_table, file_out_result, na = "")


message("Files 'Import_Activities_R.csv' and 'Import_Results_R.csv' saved.")



