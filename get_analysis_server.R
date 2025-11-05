# Shiny server: selections, content fetch, section metrics, and outputs
source("get_analysis_pipeline.R")
server <- function(input, output, session) {
  
  # Holds DB rows (lesson_id, content JSON) fetched on demand
  pulled_content_df <- reactiveVal(NULL)
  
  # Summarise the tree selection into unique Course/Module/Chapter/Lesson rows
  selected_summary <- reactive({
    
    selected <- input$lessonTree
    
    if (is.null(selected) || length(selected) == 0) {
      return(tibble(Name = character(), Type = character(), Lesson_ID = character()))
    }
    
    
    # Lessons
    lesson_df <- content_tree %>%
      filter(title_lesson_id %in% selected) %>%
      transmute(Name = title_lesson_id, Type = "Lesson", Lesson_ID = as.character(lesson_id))
    
    # Chapter
    chapter_df <- content_tree %>%
      group_by(chapter) %>%
      summarise(
        module = first(module),
        lesson_list = list(title_lesson_id),
        .groups = "drop"
      ) %>%
      rowwise() %>%
      mutate(fully_selected = all(unlist(lesson_list) %in% selected)) %>%
      filter(fully_selected) %>%
      transmute(Name = chapter, Type = "Chapter", Lesson_ID = NA_character_)
    
    # Module
    module_df <- content_tree %>%
      group_by(module) %>%
      summarise(
        course = first(course),
        lesson_list = list(title_lesson_id),
        .groups = "drop"
      ) %>%
      rowwise() %>%
      mutate(fully_selected = all(unlist(lesson_list) %in% selected)) %>%
      filter(fully_selected) %>%
      transmute(Name = module, Type = "Module", Lesson_ID = NA_character_)
    
    # Course
    course_df <- content_tree %>%
      group_by(course) %>%
      summarise(
        lesson_list = list(title_lesson_id),
        .groups = "drop"
      ) %>%
      rowwise() %>%
      mutate(fully_selected = all(unlist(lesson_list) %in% selected)) %>%
      filter(fully_selected) %>%
      transmute(Name = course, Type = "Course", Lesson_ID = NA_character_)
    
    
    bind_rows(lesson_df, chapter_df, module_df, course_df) %>%
      distinct(Name, Type, Lesson_ID)
  })
  
  # On click, pull JSON content for the selected lesson IDs from DB
  observeEvent(input$pullContent, {
    ids <- selected_lesson_ids()
    if (is.null(ids) || length(ids) == 0) {
      pulled_content_df(NULL)
      return()
    }
    
    connection <- DBI::dbConnect(
      RMariaDB::MariaDB(),
      dbname   = 'learnable',
      host     = '127.0.0.1',
      port     = 2244,
      user     = 'dbeaver',
      password = 'shizkqhsh-18-791uwhsjw-891'
    )
    on.exit(DBI::dbDisconnect(connection), add = TRUE)
    
    ids_sql <- paste(DBI::dbQuoteString(connection, ids), collapse = ",")
    query <- paste0(
      "SELECT\n",
      "  CAST(l.id AS CHAR) AS lesson_id,\n",
      "  d.content           AS content\n",
      "FROM lessons l\n",
      "JOIN docs d ON l.doc_id = d.id\n",
      "WHERE l.id IN (", ids_sql, ");"
    )
    
    df <- DBI::dbGetQuery(connection, query) %>%
      tibble::as_tibble()
    
    pulled_content_df(df)
  })
  
  # Preview of pulled JSON per lesson
  output$pulledContentTable <- renderDT({
    df <- pulled_content_df()
    if (is.null(df) || nrow(df) == 0) {
      return(datatable(data.frame()))
    }
    content_chr <- as.character(df$content)
    preview <- ifelse(nchar(content_chr) > 200, paste0(substr(content_chr, 1, 200), "…"), content_chr)
    datatable(
      tibble::tibble(lesson_id = df$lesson_id, content_preview = preview),
      rownames = FALSE,
      options = list(pageLength = 10)
    )
  })
  
  # Section metrics table via the refactored pipeline
  section_metrics_refactored <- reactive({
    build_section_metrics_table(pulled_content_df())
  })
  
  # Expose latest table for inspection in RStudio: View(section_metrics_last)
  observe({
    df <- section_metrics_refactored()
    if (!is.null(df) && nrow(df) > 0) {
      assign("section_metrics_last", df, envir = .GlobalEnv)
    }
  })

  # Expose latest raw Tiptap JSON per lesson for inspection in RStudio
  # Access as: View(lessons_tiptap_raw_last)
  observe({
    raw_df <- pulled_content_df()
    if (!is.null(raw_df) && nrow(raw_df) > 0) {
      assign("lessons_tiptap_raw_last", raw_df, envir = .GlobalEnv)
    }
  })

  output$sectionMetricsTable <- renderDT({
    df <- section_metrics_refactored()
    if (is.null(df) || nrow(df) == 0) {
      return(datatable(data.frame()))
    }
    datatable(df, rownames = FALSE, options = list(pageLength = 10, scrollX = TRUE))
  })

  # Aggregated per-lesson metrics (sums only; exclude non-countable metrics)
  lesson_metrics <- reactive({
    df <- section_metrics_refactored()
    if (is.null(df) || nrow(df) == 0) return(tibble::tibble())
    out <- df %>%
      dplyr::group_by(lesson_id) %>%
      dplyr::summarise(
        plaintext = stringr::str_trunc(stringr::str_squish(paste(plaintext, collapse = " ")), 100),
        words_total = sum(words_total, na.rm = TRUE),
        words_nomath = sum(words_nomath, na.rm = TRUE),
        chars_total = sum(chars_total, na.rm = TRUE),
        sentences_nomath = sum(sentences_nomath, na.rm = TRUE),
        blocks_total = sum(blocks_total, na.rm = TRUE),
        words_sum = sum(words_sum, na.rm = TRUE),
        headings_count = sum(headings_count, na.rm = TRUE),
        images_count = sum(images_count, na.rm = TRUE),
        tables_count = sum(tables_count, na.rm = TRUE),
        lists_count = sum(lists_count, na.rm = TRUE),
        latex_total = sum(latex_total, na.rm = TRUE),
        latex_inline = sum(latex_inline, na.rm = TRUE),
        latex_display = sum(latex_display, na.rm = TRUE),
        latex_display_multi = sum(latex_display_multi, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::left_join(
        content_tree %>% dplyr::select(lesson_id, lesson) %>% dplyr::distinct(),
        by = "lesson_id"
      ) %>%
      dplyr::relocate(lesson, .after = lesson_id) %>%
      dplyr::arrange(lesson_id)
    out
  })

  output$lessonMetricsTable <- renderDT({
    df <- lesson_metrics()
    if (is.null(df) || nrow(df) == 0) {
      return(datatable(data.frame()))
    }
    datatable(df, rownames = FALSE, options = list(pageLength = 10, scrollX = TRUE))
  })
  
  # Optional: save table snapshot to data/ when button used
  observeEvent(input$saveSectionMetrics, {
    df <- section_metrics_refactored()
    if (is.null(df) || nrow(df) == 0) {
      showNotification("Nothing to save: table is empty.", type = "warning")
      return()
    }
    dir.create("data", showWarnings = FALSE)
    ts <- format(Sys.time(), "%Y-%m-%d_%H%M%S")
    path <- file.path("data", paste0("section_metrics_", ts, ".rds"))
    saveRDS(df, path)
    showNotification(paste0("Saved section metrics to ", path), type = "message")
  })
  
  # Selection summary table: hides redundant child rows
  output$summaryTable <- renderDT({
    df <- selected_summary()
    
    # If a parent is selected, remove its children
    hide_redundant <- function(df) {
      # Priority order
      df <- df %>%
        mutate(type_order = case_when(
          Type == "Course" ~ 1,
          Type == "Module" ~ 2,
          Type == "Chapter" ~ 3,
          Type == "Lesson" ~ 4
        )) %>%
        arrange(type_order)
      
      # Start with parent types, remove children if parent exists
      keep <- rep(TRUE, nrow(df))
      
      for (i in seq_len(nrow(df))) {
        if (df$Type[i] == "Course") {
          keep <- keep & !(df$Type == "Module" & df$Name %in% content_tree$module[content_tree$course == df$Name[i]])
          keep <- keep & !(df$Type == "Chapter" & df$Name %in% content_tree$chapter[content_tree$course == df$Name[i]])
          keep <- keep & !(df$Type == "Lesson" & df$Name %in% content_tree$title_lesson_id[content_tree$course == df$Name[i]])
        }
        if (df$Type[i] == "Module") {
          keep <- keep & !(df$Type == "Chapter" & df$Name %in% content_tree$chapter[content_tree$module == df$Name[i]])
          keep <- keep & !(df$Type == "Lesson" & df$Name %in% content_tree$title_lesson_id[content_tree$module == df$Name[i]])
        }
        if (df$Type[i] == "Chapter") {
          keep <- keep & !(df$Type == "Lesson" & df$Name %in% content_tree$title_lesson_id[content_tree$chapter == df$Name[i]])
        }
      }
      
      df[keep, c("Name", "Type")]
    }
    
    datatable(hide_redundant(df), rownames = FALSE)
  })
  
  # Helper to collapse selected tree rows into unique lesson IDs
  selected_lesson_ids <- reactive({
    selected_summary() %>%
      filter(Type == "Lesson") %>%
      pull(Lesson_ID) %>%
      unique()
  })
  
  # Simple text view of selected lesson IDs
  output$selectedLessonIds <- renderText({
    ids <- selected_lesson_ids()
    if (length(ids) == 0) {
      "None selected"
    } else {
      paste(ids, collapse = ", ")
    }
  })
  
  # Legacy cache updater for lesson text (kept for compatibility)
  lesson_text_df <- reactive({
    get_updated_lesson_text(
      selected_lesson_ids = selected_lesson_ids(),
      cached_lesson_text = cached_lesson_text,
      content_tree = content_tree
    )
  })
  
}