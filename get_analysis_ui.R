# Define UI for application that draws a histogram
ui <- fluidPage(
  tags$h2("getAnalysis"),
  fluidRow(
    column(
      width = 5,
      shinyWidgets::treeInput(
        inputId = "lessonTree",
        label = "Select lessons:",
        choices = create_tree(content_tree_display),
        #selected = "X",
        returnValue = "text",
        closeDepth = 0
      )
    ),
    column(
      width = 7,
      DTOutput("summaryTable")
    )
  )
)

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
  
  connection <- dbConnect(
    RMariaDB::MariaDB(),
    dbname   = 'learnable',
    host     = '127.0.0.1',
    port     = 2244,
    user     = 'dbeaver',
    password = 'shizkqhsh-18-791uwhsjw-891'
  )
  
  # Safely quote IDs
  ids_sql <- paste(DBI::dbQuoteString(connection, ids), collapse = ",")
  
  query <- paste0("
    SELECT
      CAST(l.id AS CHAR) AS lesson_id,
      l.content           AS content_json
    FROM lessons l
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





