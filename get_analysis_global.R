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


#initialisation

# Get today's date in the format used in filenames
today_string <- format(Sys.Date(), "%Y-%m-%d")

#location of cache
cache_path <- "cache"

content_tree_file <- file.path(cache_path, paste0("content_tree_", today_string, ".rds"))
lesson_text_file <- file.path(cache_path, paste0("lesson_text_app-", today_string, ".rds"))


#function that retrieves the content structure from the Learnable database
get_content_tree <- function() {
  # Connect to the MariaDB (MySQL-compatible) database
  connection <- dbConnect(
    RMariaDB::MariaDB(),
    dbname   = 'learnable',
    host     = '127.0.0.1',
    port     = 2244,
    user     = 'dbeaver',
    password = 'shizkqhsh-18-791uwhsjw-891'
  )
  
  query <- paste0("
    SELECT
      l.id                 AS lesson_id,
      tn.position          AS level_1_pos,
      tn.structure_title   AS level_1_title,
      tn2.position         AS level_2_pos,
      tn2.structure_title  AS level_2_title,
      tn3.position         AS level_3_pos,
      tn3.structure_title  AS level_3_title,
      tn4.position         AS level_4_pos,
      tn4.structure_title  AS level_4_title
    FROM collections c
    JOIN tree_nodes tn   ON c.root_tree_node_id = tn.id
    JOIN tree_nodes tn2  ON tn2.parent_id = tn.id
    JOIN tree_nodes tn3  ON tn3.parent_id = tn2.id
    JOIN tree_nodes tn4  ON tn4.parent_id = tn3.id
    JOIN lesson_tree_node ltn ON tn4.id = ltn.tree_node_id
    JOIN lessons l       ON l.id = ltn.lesson_id
    ;
  ")
  
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
  content_tree <- get_content_tree() 
  message("Getting content tree from database and saving to cache.")
  saveRDS(content_tree, content_tree_file)
  content_tree <- content_tree %>%
    process_content_tree()
  
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

