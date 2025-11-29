# Global dependencies, configuration, and cached data setup for the app
library(shiny)
library(DT)
library(shinyWidgets)

#main method
library(DBI)
library(RMariaDB)
library(bit64)  # For integer64 support

#extracting from JSON
library(dplyr)
library(jsonlite)
library(listviewer)


#applying extraction function
library(purrr)

#aggregating text
library(tidyverse)
library(stringr)

#chatGPT api
library(httr)


library(googlesheets4)

#timer
library(tictoc)

# plotting layout
library(patchwork)
library(ggExtra)
library(cowplot)
library(plotly)
library(RColorBrewer)
library(ggiraph)

# Optional side-panel geoms for distributions
ggside_available <- requireNamespace("ggside", quietly = TRUE)


#initialisation

# Load environment variables from .env file
env_file <- file.path(getwd(), ".env")
if (file.exists(env_file)) {
  env_vars <- readLines(env_file, warn = FALSE)
  for (line in env_vars) {
    line <- trimws(line)
    # Skip empty lines and comments
    if (nchar(line) == 0 || startsWith(line, "#")) next
    # Parse KEY=VALUE format
    if (grepl("=", line)) {
      parts <- strsplit(line, "=", fixed = TRUE)[[1]]
      if (length(parts) >= 2) {
        key <- trimws(parts[1])
        value <- trimws(paste(parts[-1], collapse = "="))
        # Set environment variable using proper syntax
        do.call(Sys.setenv, setNames(list(value), key))
      }
    }
  }
  message("✓ Loaded environment variables from .env file")
} else {
  message("Note: .env file not found - qualitative analysis may fail without OPENAI_API_KEY")
}

# App configuration and cache setup
# Get today's date in the format used in filenames
today_string <- format(Sys.Date(), "%Y-%m-%d")

#location of cache
cache_path <- "cache"

content_tree_file <- file.path(cache_path, paste0("content_tree_", today_string, ".rds"))
lesson_text_file <- file.path(cache_path, paste0("lesson_text_app-", today_string, ".rds"))
analysis_cache_file <- file.path(cache_path, "analysis_cache.rds")
qualitative_cache_file <- file.path(cache_path, "qualitative_cache.rds")


# Helper function to ensure Cloud SQL proxy is running
ensure_db_proxy <- function() {
  # Check if we can connect to the database port
  can_connect <- tryCatch({
    con <- socketConnection(host = "127.0.0.1", port = 2244, open = "r+", timeout = 1)
    close(con)
    TRUE
  }, error = function(e) FALSE)

  if (!can_connect) {
    message("Cloud SQL proxy not detected. Starting it now...")

    # Path to the proxy script
    proxy_script <- file.path(getwd(), "learnable-staging-db")

    if (!file.exists(proxy_script)) {
      stop(
        "\n\n========================================\n",
        "CLOUD SQL PROXY SCRIPT NOT FOUND\n",
        "========================================\n\n",
        "Could not find the proxy script at: ", proxy_script, "\n\n",
        "Please ensure 'learnable-staging-db' exists in the project directory.\n",
        "========================================\n",
        call. = FALSE
      )
    }

    # Make the script executable
    system2("chmod", args = c("+x", proxy_script), stdout = FALSE, stderr = FALSE)

    # Start the proxy in the background
    system2(proxy_script, wait = FALSE, stdout = FALSE, stderr = FALSE)

    # Wait a moment for the proxy to start
    message("Waiting for Cloud SQL proxy to start...")
    Sys.sleep(3)

    # Verify it's now running
    can_connect_now <- tryCatch({
      con <- socketConnection(host = "127.0.0.1", port = 2244, open = "r+", timeout = 2)
      close(con)
      TRUE
    }, error = function(e) FALSE)

    if (!can_connect_now) {
      stop(
        "\n\n========================================\n",
        "FAILED TO START CLOUD SQL PROXY\n",
        "========================================\n\n",
        "The proxy script was executed but the connection still failed.\n\n",
        "Please try running manually:\n",
        "  ./learnable-staging-db\n\n",
        "And check for any error messages.\n",
        "========================================\n",
        call. = FALSE
      )
    }

    message("✓ Cloud SQL proxy started successfully!")
  } else {
    message("✓ Cloud SQL proxy is already running")
  }
}

#function that retrieves the content structure from the Learnable database
get_content_tree <- function() {
  # Ensure Cloud SQL proxy is running
  ensure_db_proxy()

  # Connect to the MariaDB (MySQL-compatible) database
  connection <- tryCatch({
    dbConnect(
      RMariaDB::MariaDB(),
      dbname   = 'learnable',
      host     = '127.0.0.1',
      port     = 2244,
      user     = 'dbeaver',
      password = 'shizkqhsh-18-791uwhsjw-891'
    )
  }, error = function(e) {
    stop(
      "\n\n========================================\n",
      "DATABASE CONNECTION FAILED\n",
      "========================================\n\n",
      "Could not connect to the database.\n\n",
      "The Cloud SQL proxy appears to be running, but the database connection failed.\n\n",
      "Original error: ", conditionMessage(e), "\n",
      "========================================\n",
      call. = FALSE
    )
  })

  # Query content hierarchy; filter to Module-level nodes to match app scope
  query <- paste0("\n    SELECT\n      l.id                 AS lesson_id,\n      tn.position          AS level_1_pos,\n      tn.structure_title   AS level_1_title,\n      tn2.position         AS level_2_pos,\n      tn2.structure_title  AS level_2_title,\n      tn3.position         AS level_3_pos,\n      tn3.structure_title  AS level_3_title,\n      tn4.position         AS level_4_pos,\n      tn4.structure_title  AS level_4_title\n    FROM collections c\n    JOIN tree_nodes tn   ON c.root_tree_node_id = tn.id\n    JOIN tree_nodes tn2  ON tn2.parent_id = tn.id\n    JOIN tree_nodes tn3  ON tn3.parent_id = tn2.id\n    JOIN tree_nodes tn4  ON tn4.parent_id = tn3.id\n    JOIN lesson_tree_node ltn ON tn4.id = ltn.tree_node_id\n    JOIN lessons l       ON l.id = ltn.lesson_id\n    where tn2.structure_title like 'Module%'\n    ;\n  ")

  content_index <- dbGetQuery(connection, query)
  dbDisconnect(connection)
  return(content_index)
}


#function that takes content tree and transforms it
process_content_tree <- function(content_tree) {
  content_tree %>%
    # Map level titles/positions to familiar names used downstream
    transmute(
      course    = level_1_title,
      module    = level_2_title,
      chapter   = level_3_title,
      lesson    = level_4_title,
      c_pos     = level_1_pos,
      m_pos     = level_2_pos,
      ch_pos    = level_3_pos,
      l_pos     = level_4_pos,
      lesson_id = as.character(lesson_id)
    ) %>%
    # Optional filters to mimic previous behaviour
    filter(!str_detect(course, "Reference")) %>%
    filter(!str_detect(lesson, "^Introduction to Module")) %>%
    mutate(lesson_id = as.character(lesson_id)) %>%
    mutate(
      subject = str_extract(course, "Chemistry|Physics|Biology"),
      title_lesson_id = paste0(lesson, " (", lesson_id, ")")
    ) %>%
    arrange(subject, course, module, ch_pos, l_pos) %>%
    select(course, module, chapter, lesson, lesson_id, title_lesson_id) %>%
    distinct()
}





# Ensure cache directory exists
if (!dir.exists(cache_path)) {
  dir.create(cache_path)
}

# Load from cache if it exists, otherwise run query and save to cache
if (file.exists(content_tree_file)) {
  message("Loading content tree from cache.")
  content_tree <- readRDS(content_tree_file) %>%
    process_content_tree()

} else {
  message("No cached content tree found. Attempting to fetch from database...")
  content_tree <- tryCatch({
    result <- get_content_tree()
    message("✓ Successfully fetched content tree from database. Saving to cache...")
    saveRDS(result, content_tree_file)
    process_content_tree(result)
  }, error = function(e) {
    warning(
      "\n\n========================================\n",
      "FAILED TO LOAD CONTENT TREE\n",
      "========================================\n\n",
      "Could not fetch content tree from database.\n",
      "Error: ", conditionMessage(e), "\n\n",
      "The app will start but you won't be able to select lessons.\n",
      "To fix this, ensure the database proxy is running:\n",
      "  ./learnable-staging-db\n\n",
      "Then restart the app.\n",
      "========================================\n"
    )
    # Return empty tibble so app can start
    tibble::tibble(
      course = character(),
      module = character(),
      chapter = character(),
      lesson = character(),
      lesson_id = character(),
      title_lesson_id = character()
    )
  })
}

content_tree_display <- content_tree %>%
  select(course, module, chapter, title_lesson_id) %>%
  distinct()



# On startup: load cached lesson text or initialise empty one
cached_lesson_text <- if (file.exists(lesson_text_file)) {
  readRDS(lesson_text_file)
} else {
  tibble(lesson_id = character(), lesson = character(), text = character())
}

# Analysis cache: section-level analysis rows with freshness timestamp
analysis_cache <- if (file.exists(analysis_cache_file)) {
  readRDS(analysis_cache_file)
} else {
  tibble::tibble()
}

# Qualitative analysis cache: lesson-level qualitative results with freshness timestamp
qualitative_cache <- if (file.exists(qualitative_cache_file)) {
  readRDS(qualitative_cache_file)
} else {
  tibble::tibble(lesson_id = character(), qualitative_json = character())
}


# Initialise variable
lesson_text <- NULL

# Load if available
if (file.exists(lesson_text_file)) {
  message("Loading lesson_text from cache.")
  lesson_text <- readRDS(lesson_text_file)
} else {
  message("No cached lesson_text file found.")
}

get_content_df <- function(filtered_content_index) {
  # Expect filtered_content_index to contain lesson_id
  ids <- unique(filtered_content_index$lesson_id)
  if (length(ids) == 0) {
    return(tibble(lesson_id = character(), content = character()))
  }

  # Ensure Cloud SQL proxy is running
  ensure_db_proxy()

  connection <- tryCatch({
    dbConnect(
      RMariaDB::MariaDB(),
      dbname   = 'learnable',
      host     = '127.0.0.1',
      port     = 2244,
      user     = 'dbeaver',
      password = 'shizkqhsh-18-791uwhsjw-891'
    )
  }, error = function(e) {
    stop(
      "\n\n========================================\n",
      "DATABASE CONNECTION FAILED\n",
      "========================================\n\n",
      "Could not connect to the database.\n\n",
      "The Cloud SQL proxy appears to be running, but the database connection failed.\n\n",
      "Original error: ", conditionMessage(e), "\n",
      "========================================\n",
      call. = FALSE
    )
  })

  # Safely quote IDs
  ids_sql <- paste(DBI::dbQuoteString(connection, ids), collapse = ",")

  query <- paste0("
    SELECT
      CAST(l.id AS CHAR) AS lesson_id,
      d.content           AS content_json
    FROM lessons l
    JOIN docs d ON l.doc_id = d.id
    WHERE l.id IN (", ids_sql, ");
  ")

  df <- dbGetQuery(connection, query) %>%
    as_tibble() %>%
    rename(content = content_json)

  dbDisconnect(connection)
  df
}

get_content_text_df <- function(content_df) {
  # content_df: tibble(lesson_id, content)
  if (nrow(content_df) == 0) {
    return(tibble(lesson_id = character(), text = character()))
  }
  
  extract_all_text <- function(json_str) {
    if (is.na(json_str) || is.null(json_str) || json_str == "") return("")
    x <- tryCatch(jsonlite::fromJSON(json_str, simplifyVector = FALSE), error = function(e) NULL)
    if (is.null(x)) return("")
    
    acc <- character()
    
    walker <- function(node) {
      if (is.null(node)) return()
      # Collect text fields
      if (!is.null(node$text) && is.character(node$text)) {
        acc <<- c(acc, node$text)
      }
      # Recurse children commonly under 'content' or 'children'
      for (child_key in c("content", "children")) {
        if (!is.null(node[[child_key]])) {
          for (child in node[[child_key]]) walker(child)
        }
      }
      # Also scan all list elements in case structure varies
      if (is.list(node)) {
        for (el in node) {
          if (is.list(el)) walker(el)
        }
      }
    }
    
    walker(x)
    paste(acc, collapse = " ")
  }
  
  content_df %>%
    mutate(text = purrr::map_chr(content, extract_all_text)) %>%
    select(lesson_id, text)
}



get_updated_lesson_text <- function(selected_lesson_ids, cached_lesson_text, content_tree) {
  # Which IDs are missing?
  missing_ids <- setdiff(selected_lesson_ids, cached_lesson_text$lesson_id)
  
  # If nothing is missing, return relevant cached rows
  if (length(missing_ids) == 0) {
    return(cached_lesson_text %>% filter(lesson_id %in% selected_lesson_ids))
  }
  
  # Otherwise, fetch the missing rows
  message("Fetching missing lesson text for IDs: ", paste(missing_ids, collapse = ", "))
  
  # Step 1: Get metadata
  filtered_content_index <- content_tree %>%
    filter(lesson_id %in% missing_ids)
  
  # Step 2: Get section-level content
  content_df <- get_content_df(filtered_content_index)
  
  # Step 3: Convert JSON content into text
  section_text <- get_content_text_df(content_df)
  
  # Step 4: Format lesson-level data
  new_lesson_text <- section_text %>%
    aggregate(text ~ lesson_id, data = ., paste, collapse=" ") %>%
    merge(filtered_content_index %>% select(lesson_id, lesson), by = "lesson_id") %>%
    select(lesson_id, lesson, text) %>%
    distinct()
  
  # Step 5: Update cache
  updated_cache <- bind_rows(cached_lesson_text, new_lesson_text) %>%
    distinct(lesson_id, .keep_all = TRUE)
  
  saveRDS(updated_cache, lesson_text_file)
  message("Updated cached lesson text saved.")
  
  # Return only requested lessons
  updated_cache %>% filter(lesson_id %in% selected_lesson_ids)
}

