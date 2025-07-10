
# About this code ---------------------------------------------------------
### SK-PIC-v5.R
# This script is a compilation of code bits mostly found via internet searching
# or co-developed with TXG technical assistance providers
# The script was envisioned, assembled, and de-bugged by Robert Knapp for the 
# Jamestown S'Klallam Tribe and others.
# the code is intended to read a .csv file output from the Clallam Streamkeepers
# water quality database for all the Clallam County Pollution Identification and Correction (PIC)
# trends monitoring sites. Sites are visited by volunteer citizen scientist 4 or 12 times per year.
# https://www.clallamcountywa.gov/416/Pollution-Identification-and-Correction-
# The code loads, wrangles, performs some (but likely not all) QC checks,
# summarizes, and produces plots and tables of the data. 
# Table and plots may be used for the JST TAR in the future.
# This is a work in progress and could use more commenting. 
# Please feel free to use, edit and borrow from. 

###### Version Notes
###   v5.0 Robert cleaned up a bit added a bit of comments submitted to UW Student project
###   v5.1 Robert added package notation to many of commands/functions as a learning tool, add version notes.
###   2025-07-09 connected to Git and Github


# Load libraries ----------------------------------------

# Load required libraries
library(tidyverse)
library(lubridate)
library(ggplot2)
library(gt)
library(webshot2)


# Set working Directory ---------------------------------------------------
#Optional set working directory by browsing. Could be hard coded.
# Prompts user to set navigate to the working directory of choice-- csv datasets must also
# be in the working directory.
#Robert's directory is # C:/Users/Robert/Documents/R/workspace/TAR/SK

# may work with both windows (tested) and macOS (not tested)
if (interactive()) {
  if (.Platform$OS.type == "windows") {
    outDir <- tcltk::tk_choose.dir(getwd(), "Choose a suitable folder")
  } else {
    outDir <- tcltk::tk_choose.dir(getwd(), "Choose a suitable folder")
  }
  if (!is.null(outDir)) {
    base::setwd(outDir)
  } else {
    message("No directory selected. Working directory not changed.")
  }
}



# Load and Wrangle Data ---------------------------------------------------


# Read the CSV file
# File Name will need to be changed to point to new csv when additional data is provided. 
# The code is designed to accommodate new data and new years (tested with 2024 data) without additional modification but your mileage may vary.
df <- readr::read_csv("utl_crosstab_PIC_data_summary 2015 - 2024.csv", col_types = cols(Arrival_Date = col_date(format = "%m/%d/%Y")))
# "C:\Users\rknapp\OneDrive - Jamestown S'Klallam Tribe\Documents 1\R\workspace\TAR\SK\utl_crosstab_PIC_data_summary 2015 - 2024.csv"


# Clean column names - remove spaces and special characters
names(df) <- base::gsub("[[:space:]]|[[:punct:]]", "_", names(df))

print(names(df))


# Convert data types and clean the data
df_clean <- df %>%
  #separate stream name from site (river mile) and extract year from arrival date
  dplyr::mutate(column_split = Primary_Name) %>% 
  tidyr::separate(column_split, into = c("stream","site"), sep = "(?<=\\D)(?=\\d)", extra = "merge") %>% 
  dplyr::relocate( stream, site, .before = Arrival_Date) %>% 
  dplyr::mutate(Year=year(Arrival_Date), .after = Arrival_Date) %>% 
  # Convert date
  dplyr::mutate(
    # Convert numeric columns, replacing empty strings with NA
    Ammonia__NH3__as_Nitrogen__N_ = as.numeric(Ammonia__NH3__as_Nitrogen__N_),
    Barometric_pressure = as.numeric(Barometric_pressure),
    Dissolved_Oxygen = as.numeric(Dissolved_Oxygen),
    Dissolved_Oxygen_Percent_Saturation = as.numeric(Dissolved_Oxygen_Percent_Saturation),
    Fecal_Coliform = as.numeric(Fecal_Coliform),
    Flow = as.numeric(Flow),
    Nitrate_as_N = as.numeric(Nitrate_as_N),
    Nitrite_as_N = as.numeric(Nitrite_as_N),
    pH = as.numeric(pH),
    Phosphate_as_P = as.numeric(Phosphate_as_P),
    Salinity = as.numeric(Salinity),
    Silicate_as_Si = as.numeric(Silicate_as_Si),
    Specific_Conductivity__at_25_deg_C_ = as.numeric(Specific_Conductivity__at_25_deg_C_),
    Stream_River_Stage = as.numeric(Stream_River_Stage),
    Temperature__water = as.numeric(Temperature__water),
    Total_Persulfate_Nitrogen = as.numeric(Total_Persulfate_Nitrogen),
    Total_Persulfate_Phosphorus = as.numeric(Total_Persulfate_Phosphorus),
    Turbidity = as.numeric(Turbidity),
    
    # Convert PIC tier to factor
    PIC_tier = factor(PIC_tier),
    
    # Ensure Primary Name is character
    Primary_Name = as.character(Primary_Name)
  ) %>%
  # Remove duplicate records
  dplyr::distinct() %>%
  # Sort by Primary Name and Date
  dplyr::arrange(Primary_Name, Arrival_Date)


# QC Checks ---------------------------------------------------------------


# Add some basic data quality checks
data_summary <- df_clean %>%
  dplyr::summarise(
    total_records = dplyr::n(),
    date_range_start = base::min(Arrival_Date, na.rm = TRUE),
    date_range_end = base::max(Arrival_Date, na.rm = TRUE),
    missing_dates = base::sum(is.na(Arrival_Date)),
    missing_primary_names = base::sum(is.na(Primary_Name)),
    unique_locations = dplyr::n_distinct(Primary_Name)
  )

# Print summary
print(data_summary)

# Check for any remaining potential issues
issues <- df_clean %>%
  dplyr::filter(
    pH < 0 | pH > 14 |  # Invalid pH values
      Temperature__water < -5 | Temperature__water > 40 |  # Extreme temperatures
      Dissolved_Oxygen < 0 | Dissolved_Oxygen > 20  # Extreme DO values
  )

if(nrow(issues) > 0) {
  print("Potential data quality issues found:")
  print(issues)
}

# Optional: Write cleaned data to new CSV
# readr::write_csv(df_clean, "SK-onlyPIC-data-wFC-2015-2024-cleaned.csv")


# Optional: Read cleaned data to dataframe
#df_clean <- readr::read_csv("C:/Users/Robert/Documents/R/workspace/TAR/SK-onlyPIC-data-wFC-2015-2024-cleaned.csv")


# Create a summary of missing values
missing_summary <- df_clean %>%
  dplyr::summarise(dplyr::across(tidyselect::everything(), ~sum(is.na(.)))) %>%
  t() %>% #this seems to transpose but what package does this call?
  base::as.data.frame() %>%
  `colnames<-`(c("missing_count"))

# Print missing value summary
base::print("Missing values summary:")
print(missing_summary)

# Ensure ggplot2 is installed and loaded
if (!require(ggplot2)) {
  install.packages("ggplot2")
  library(ggplot2)
}

# Optional: Create basic plots for key parameters
if(require(ggplot2)) {
  # Print structure of df_clean for debugging
  print(head(df_clean))
  print(names(df_clean))
  
  # Handle missing values
  #df_clean <- na.omit(df_clean)
  
  # Temperature over time
  ggplot(df_clean, aes(x = Arrival_Date, y = Temperature__water)) +
    geom_point(alpha = 0.5) +
    geom_smooth(method = "loess") +
    theme_minimal() +
    labs(title = "Water Temperature Over Time",
         x = "Date",
         y = "Temperature (°C)")  # Closing parenthesis here
}

# DO vs Temperature
ggplot(df_clean, aes(x = Temperature__water, y = Dissolved_Oxygen)) +
  geom_point(alpha = 0.5) +
  geom_smooth(method = "loess") +
  theme_minimal() +
  labs(title = "Dissolved Oxygen vs Temperature",
       x = "Temperature (°C)",
       y = "Dissolved Oxygen (mg/L)")  # Closing parenthesis here

# Fecal Coliform ----------------------------------------------------------

# Calculate geometric mean of fecal coliform by stream, site, PIC tier and year
fecal_summary <- df_clean %>%
  dplyr::group_by(stream, site, PIC_tier, Year) %>%
  dplyr::summarise(
    geomean_fecal = if(n() > 1) exp(mean(log(Fecal_Coliform), na.rm = TRUE)) else NA,
    n_samples = n(),
    .groups = 'drop'
  ) %>%
  tidyr::pivot_wider(
    id_cols = c(stream, site, PIC_tier),
    names_from = Year,
    values_from = c(geomean_fecal, n_samples),
    names_glue = "{.value}_{Year}"
  ) %>%
  dplyr::arrange(stream, site) %>% 
  dplyr::filter(PIC_tier!="is.na")

# Get years for header construction
years <- base::sort(base::unique(df_clean$Year))
cols_to_round <- base::paste0("geomean_fecal_", years)

# Create dynamic column labels
geomean_labels <- stats::setNames(
  rep("Geomean", length(years)),
  base::paste0("geomean_fecal_", years)
)
n_samples_labels <- stats::setNames( #don't know how stats package is loaded.
  rep("n", length(years)),
  base::paste0("n_samples_", years)
)

# Create dynamic spanner specifications
spanner_specs <- base::lapply(years, function(year) {
  list(
    label = as.character(year),
    columns = c(
      paste0("geomean_fecal_", year),
      paste0("n_samples_", year)
    )
  )
})

# Create table using gt
tab_1 <- fecal_summary %>%
  dplyr::arrange(PIC_tier, stream, site) %>%
  gt::gt() %>%
  gt::tab_header(
    title = "Geometric Mean of Fecal Coliform by Location and Year"
  ) %>%
  gt::cols_label(
    stream = "Stream",
    site = "Site",
    PIC_tier = "PIC Tier"
  ) %>%
  gt::fmt_number(
    columns = starts_with("geomean_"),
    decimals = 1
  ) %>%
  sub_missing(
    columns = starts_with("geomean_"),
    missing_text = ""
  ) %>%
  # Style geomean columns background
  data_color(
    columns = matches("geomean_fecal_\\d{4}$"),
    fn = function(x) {
      case_when(
        !is.na(as.numeric(x)) & as.numeric(x) >= 100 ~ "red",
        !is.na(as.numeric(x)) & as.numeric(x) >= 50 & as.numeric(x) < 100 ~ "#FFB668",
        TRUE ~ "white"
      )
    }
  ) %>%
  # Style geomean columns background
  data_color(
    columns = matches("n_samples_\\d{4}$"),
    fn = function(x) {
      case_when(
        !is.na(as.numeric(x)) & as.numeric(x) < 2 ~ "gray",
        !is.na(as.numeric(x)) & as.numeric(x) > 12 ~ "#f333aa",
        TRUE ~ "white"
      )
    }
  ) %>%
  
  # create footnotes
  tab_footnote(
    footnote = "Red color indicates geomean greater than or equal to 100 CFU/100 ml.",
    locations = cells_column_labels(
      columns =  matches("geomean_fecal_\\d{4}$")
    )
  ) %>%
  tab_footnote(
    footnote = "Orange color indicates geomean greater than or equal to 50 and less than 100 CFU/100 ml.",
    locations = cells_column_labels(
      columns =  matches("geomean_fecal_\\d{4}$")
    )
  ) %>%
  tab_footnote(
    footnote = "NA indicates no samples collected. Grey color indicates samples count of 1. Geomean values are not shown when based on less than 2 samples.",
    locations = cells_column_labels(
      columns = matches("n_samples_\\d{4}$")
    )
  ) %>%
  tab_footnote(
    footnote = "Pink color indicates samples count greater than 12.",
    locations = cells_column_labels(
      columns = matches("n_samples_\\d{4}$")
    )
  ) %>% 
  tab_source_note(
    source_note = "Date collected and provided by Clallam County Streamkeepers. See also: https://www.clallamcountywa.gov/416/Pollution-Identification-and-Correction-"
  ) %>% 
  tab_source_note(
    source_note = "CFU = Colony Forming Units. The geometric mean is defined as the nth root (where n is the count of numbers) of the product of the numbers (CFU)."
  ) %>% 
  
  
  # Add dynamic spanners
  {
    local({
      tbl <- .
      for(spec in spanner_specs) {
        tbl <- tab_spanner(
          tbl,
          label = spec$label,
          columns = spec$columns
        )
      }
      tbl
    })
  } %>%
  # Add dynamic column labels
  cols_label(
    !!!c(geomean_labels, n_samples_labels)
  ) %>%
  cols_align(
    align = "center",
    columns = -c(stream, site, PIC_tier)
  ) %>%
  
  tab_style(
    style = cell_borders(
      sides = "right",
      color = "grey",
      weight = px(1)
    ),
    locations = cells_body(
      columns = matches("n_samples_\\d{4}$")
    )
  ) %>%
  tab_style(
    style = cell_borders(
      sides = "right",
      color = "grey",
      weight = px(1)
    ),
    locations = cells_body(
      columns = matches("PIC_tier")
    )
  )
#tab_1 |> gtsave(filename = "tab_1.html", inline_css = FALSE)
#tab_1 |> gtsave("tab_1.png", expand = 10) not printing full table??
#tab_1


# Create box plot for fecal_coliform grouped by stream and year with log scale on y-axis
# Does not do what I thought it would- need more experience plotting
FC_2023 <- fecal_summary %>% 
  select(stream, geomean_fecal_2023)
ggplot(FC_2023, aes(x = stream, y = geomean_fecal_2023, fill = stream)) +
  geom_boxplot() +
  scale_y_log10() +
  labs(title = "Box Plot of Fecal Coliform by Stream and Year (Log Scale)",
       x = "Year",
       y = "Fecal Coliform (Log Scale)") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))


# Create box plot for fecal_coliform grouped by stream and year with log scale on y-axis
ggplot(df_clean, aes(x = factor(Year), y = Fecal_Coliform, fill = stream)) +
  geom_boxplot() +
  scale_y_log10() +
  labs(title = "Box Plot of Fecal Coliform by Stream and Year (Log Scale)",
       x = "Year",
       y = "Fecal Coliform (Log Scale)") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))



# end of code 2025-05-1- where can we go next- better plots and code to export the plots and tables