# Shiny server: selections, content fetch, section metrics, and outputs
source("get_analysis_pipeline.R")
server <- function(input, output, session) {
  
  # Holds DB rows (lesson_id, content JSON) fetched on demand
  pulled_content_df <- reactiveVal(NULL)
  
  # Accumulators and progress for incremental analysis
  section_metrics_acc <- reactiveVal(tibble::tibble())
  lesson_metrics_acc  <- reactiveVal(tibble::tibble())
  # Non-reactive state for sequential processing (safe to access in later callbacks)
  .state <- new.env(parent = emptyenv())
  .state$ids <- character(0)
  .state$idx <- 0L
  .state$total <- 0L
  .state$sec_acc <- tibble::tibble()
  .state$les_acc <- tibble::tibble()
  .state$analysis_cache <- analysis_cache
  # Reactive progress for UI display
  progress_info <- reactiveVal(list(done = 0L, total = 0L))


  # Helper: bin a numeric vector and flag the bin containing a selected value
  bin_counts_df <- function(x, bins = 30, sel_val = NA_real_) {
    x <- x[is.finite(x)]
    if (!length(x)) return(tibble::tibble(bin_mid = numeric(), bin_start = numeric(), bin_end = numeric(), count = integer(), highlight = logical()))
    rng <- range(x, na.rm = TRUE)
    if (isTRUE(all.equal(rng[1], rng[2]))) {
      # Single-valued; create a tiny range
      rng <- c(rng[1] - 0.5, rng[2] + 0.5)
    }
    brks <- seq(rng[1], rng[2], length.out = bins + 1L)
    if (length(unique(brks)) < length(brks)) {
      brks <- pretty(rng, n = bins)
    }
    cut_x <- cut(x, breaks = brks, include.lowest = TRUE, right = TRUE)
    df <- tibble::tibble(bin = cut_x) |>
      dplyr::count(bin, name = "count") |>
      dplyr::mutate(
        bin_start = as.numeric(sub("^\\((.*),.*\\]$", "\\1", as.character(bin))),
        bin_end   = as.numeric(sub("^\\(.*,(.*)\\]$", "\\1", as.character(bin))),
        bin_mid   = (bin_start + bin_end) / 2
      ) |>
      dplyr::select(bin_mid, bin_start, bin_end, count)
    if (is.finite(sel_val)) {
      df$highlight <- sel_val >= df$bin_start & sel_val <= df$bin_end
    } else {
      df$highlight <- FALSE
    }
    df
  }
  
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
  
  # Process one lesson at a time using the event loop (non-blocking)
  process_next_lesson <- function() {
    # if done, stop
    if (.state$idx >= .state$total) return(invisible(NULL))
    idx <- .state$idx + 1L
    lid <- .state$ids[[idx]]

    # Check analysis cache (fresh within 24 hours)
    now_ts <- Sys.time()
    fresh_cutoff <- now_ts - as.difftime(1, units = "days")
    cached <- NULL
    if (!is.null(.state$analysis_cache) && nrow(.state$analysis_cache)) {
      cand <- .state$analysis_cache %>% dplyr::filter(lesson_id == lid)
      if (nrow(cand)) {
        if ("analyzed_at" %in% names(cand)) {
          cand_fresh <- cand %>% dplyr::filter(analyzed_at >= fresh_cutoff)
          if (nrow(cand_fresh)) cached <- cand_fresh
        }
      }
    }
    if (is.null(cached)) {
      # Pull single lesson only when cache is missing/stale
      conn <- DBI::dbConnect(
        RMariaDB::MariaDB(),
        dbname   = 'learnable',
        host     = '127.0.0.1',
        port     = 2244,
        user     = 'dbeaver',
        password = 'shizkqhsh-18-791uwhsjw-891'
      )
      on.exit(DBI::dbDisconnect(conn), add = TRUE)
      q <- paste0(
        "SELECT CAST(l.id AS CHAR) AS lesson_id, d.content AS content ",
        "FROM lessons l JOIN docs d ON l.doc_id = d.id ",
        "WHERE l.id = ", DBI::dbQuoteString(conn, lid), ";"
      )
      one_df <- DBI::dbGetQuery(conn, q) %>% tibble::as_tibble()
      sec_tbl <- build_section_metrics_table(one_df)
      if (nrow(sec_tbl)) sec_tbl$analyzed_at <- now_ts
      # update cache in memory and on disk
      .state$analysis_cache <- dplyr::bind_rows(.state$analysis_cache, sec_tbl) %>% dplyr::distinct()
      saveRDS(.state$analysis_cache, analysis_cache_file)
    } else {
      sec_tbl <- cached
    }
    .state$sec_acc <- dplyr::bind_rows(.state$sec_acc, sec_tbl)
    # push to reactive store for UI consumers
    section_metrics_acc(.state$sec_acc)

    # Recompute per-lesson aggregation from accumulated sections
    les_acc <- .state$sec_acc %>%
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
    .state$les_acc <- les_acc
    lesson_metrics_acc(.state$les_acc)

    # Expose last raw only when we pulled from DB
    if (exists("one_df")) pulled_content_df(one_df)
      .state$idx <- idx
      progress_info(list(done = .state$idx, total = .state$total))

      # Schedule next iteration to allow UI to flush
      if (.state$idx < .state$total) {
        later::later(process_next_lesson, delay = 0)
      }
  }

  observeEvent(input$pullContent, {
    ids <- selected_lesson_ids()
    # Reset accumulators and progress and ids
    section_metrics_acc(tibble::tibble())
    lesson_metrics_acc(tibble::tibble())
    .state$ids <- ids
    .state$total <- length(ids)
    .state$idx <- 0L
    .state$sec_acc <- tibble::tibble()
    .state$les_acc <- tibble::tibble()
    progress_info(list(done = 0L, total = .state$total))
    # Kick off processing (non-blocking)
    if (.state$total > 0L) later::later(process_next_lesson, delay = 0)
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

  # Prefer accumulated results for progressive update; else empty
  section_metrics_display <- reactive({
    acc <- section_metrics_acc()
    if (!is.null(acc) && nrow(acc) > 0) return(acc)
    tibble::tibble()
  })

  output$sectionMetricsTable <- renderDT({
    df <- section_metrics_display()
    if (is.null(df) || nrow(df) == 0) {
      return(datatable(data.frame()))
    }
    datatable(
      df,
      rownames = FALSE,
      options = list(pageLength = 10, scrollX = TRUE),
      selection = 'single'
    )
  })

  # Proxies to control table selection
  section_proxy <- dataTableProxy("sectionMetricsTable")
  lesson_proxy  <- dataTableProxy("lessonMetricsTable")

  # Click on section plot to select corresponding row
  observeEvent(input$sectionPlot_click, {
    # Safe string helper to avoid closure coercion errors in diagnostics
    sc <- function(x) {
      tryCatch({
        if (is.null(x)) return("NULL")
        if (is.atomic(x) || is.list(x) || is.factor(x)) return(toString(x))
        if (is.language(x)) return(paste0(deparse(x)[1], " …"))
        as.character(x)
      }, error = function(e) paste0("<", class(x)[1], ">"))
    }
    full_df <- section_metrics_display()
    req(!is.null(full_df), nrow(full_df) > 0, input$sectionMetric)
    # Filter exactly as the plot: keep only sections with positive words_total
    df <- full_df %>% dplyr::filter(is.finite(words_total) & words_total > 0)
    req(nrow(df) > 0)
    if (!("section_index" %in% names(df)) || all(is.na(df$section_index))) df$section_index <- seq_len(nrow(df))
    df$sec_label <- paste0(df$lesson_id, "-", df$section_index)
    # Log raw click event for diagnostics
    { cx <- input$sectionPlot_click$x; cy <- input$sectionPlot_click$y; message(paste0("Section raw click → x: ", sc(cx), ", y: ", sc(cy))) }
    # Try by label first, then nearPoints in plotted space, then numeric fallback
    hit <- NA_integer_
    if (is.character(input$sectionPlot_click$y)) {
      ylab <- input$sectionPlot_click$y
      if (!is.na(ylab) && ylab %in% df$sec_label) hit <- which(df$sec_label == ylab)[1]
    }
    if (!is.finite(hit) || is.na(hit)) {
      df$.ord <- seq_len(nrow(df))
      df$y_index <- as.integer(factor(df$.ord, levels = rev(df$.ord)))
      np <- try(nearPoints(df, input$sectionPlot_click, xvar = m, yvar = "y_index", maxpoints = 1, threshold = 40, addDist = FALSE), silent = TRUE)
      if (!inherits(np, "try-error") && is.data.frame(np) && nrow(np) == 1) {
        hit <- which(df$lesson_id == np$lesson_id & df$section_index == np$section_index)[1]
      }
    }
    if (!is.finite(hit) || is.na(hit)) {
      df$.ord <- seq_len(nrow(df))
      nlev <- nrow(df)
      ynum <- suppressWarnings(as.numeric(input$sectionPlot_click$y))
      if (is.finite(ynum)) {
        idx_in_levels <- as.integer(round(ynum))
        if (idx_in_levels >= 1 && idx_in_levels <= nlev) {
          clicked_ord <- rev(df$.ord)[idx_in_levels]
          cand <- which(df$.ord == clicked_ord)
          if (length(cand) >= 1) hit <- cand[1]
        }
      }
    }
    req(is.finite(hit), !is.na(hit), hit >= 1, hit <= nrow(df))
    # Console diagnostics of what was clicked
    m <- input$sectionMetric
    clicked_x <- input$sectionPlot_click$x
    clicked_row <- df[hit, , drop = FALSE]
    clicked_val <- if (!is.null(m) && m %in% names(df)) clicked_row[[m]][1] else NA
    message(paste0(
      "Section click → label: ", sc(ylab),
      " | lesson_id: ", sc(clicked_row$lesson_id[1]),
      " | section_index: ", sc(clicked_row$section_index[1]),
      " | x: ", sc(if (is.numeric(clicked_x)) round(clicked_x, 3) else clicked_x),
      " | ", sc(m), ": ", sc(clicked_val)
    ))
    req(length(hit) == 1)
    # Map back to row index in full (unfiltered) table
    key <- list(lesson_id = df$lesson_id[hit], section_index = df$section_index[hit])
    full_hit <- which(full_df$lesson_id == key$lesson_id &
                      (if ("section_index" %in% names(full_df)) full_df$section_index else NA_integer_) == key$section_index)
    if (length(full_hit)) selectRows(section_proxy, full_hit[1])
  })

  # Click on lesson plot to select corresponding lesson row
  observeEvent(input$lessonPlot_click, {
    sc <- function(x) {
      tryCatch({
        if (is.null(x)) return("NULL")
        if (is.atomic(x) || is.list(x) || is.factor(x)) return(toString(x))
        if (is.language(x)) return(paste0(deparse(x)[1], " …"))
        as.character(x)
      }, error = function(e) paste0("<", class(x)[1], ">"))
    }
    df <- lesson_metrics()
    req(!is.null(df), nrow(df) > 0, input$lessonMetric)
    # Log raw click event for diagnostics
    { cx <- input$lessonPlot_click$x; cy <- input$lessonPlot_click$y; message(paste0("Lesson raw click → x: ", sc(cx), ", y: ", sc(cy))) }
    # Try label first, then nearPoints, then numeric fallback
    hit <- NA_integer_
    if (is.character(input$lessonPlot_click$y)) {
      ylab <- input$lessonPlot_click$y
      if (!is.na(ylab) && ylab %in% df$lesson_id) hit <- which(df$lesson_id == ylab)[1]
    }
    if (!is.finite(hit) || is.na(hit)) {
      df$.ord <- seq_len(nrow(df))
      df$y_index <- as.integer(factor(df$.ord, levels = rev(df$.ord)))
      np <- try(nearPoints(df, input$lessonPlot_click, xvar = m, yvar = "y_index", maxpoints = 1, threshold = 40, addDist = FALSE), silent = TRUE)
      if (!inherits(np, "try-error") && is.data.frame(np) && nrow(np) == 1) {
        hit <- which(df$lesson_id == np$lesson_id)[1]
      }
    }
    if (!is.finite(hit) || is.na(hit)) {
      df$.ord <- seq_len(nrow(df))
      nlev <- nrow(df)
      ynum <- suppressWarnings(as.numeric(input$lessonPlot_click$y))
      if (is.finite(ynum)) {
        idx_in_levels <- as.integer(round(ynum))
        if (idx_in_levels >= 1 && idx_in_levels <= nlev) {
          clicked_ord <- rev(df$.ord)[idx_in_levels]
          cand <- which(df$.ord == clicked_ord)
          if (length(cand) >= 1) hit <- cand[1]
        }
      }
    }
    req(is.finite(hit), !is.na(hit), hit >= 1, hit <= nrow(df))
    # Console diagnostics of what was clicked
    m <- input$lessonMetric
    clicked_x <- input$lessonPlot_click$x
    clicked_row <- df[hit, , drop = FALSE]
    clicked_val <- if (!is.null(m) && m %in% names(df)) clicked_row[[m]][1] else NA
    message(paste0(
      "Lesson click → lesson_id: ", sc(ylab),
      " | x: ", sc(if (is.numeric(clicked_x)) round(clicked_x, 3) else clicked_x),
      " | ", sc(m), ": ", sc(clicked_val)
    ))
    if (length(hit)) selectRows(lesson_proxy, hit)
  })

  # Section-level metric choices and distribution plot with monochrome gradient by lesson index
  observe({
    df <- section_metrics_display()
    if (is.null(df) || nrow(df) == 0) return()
    # Candidate numeric metrics present in section table
    candidates <- c(
      "words_total","words_nomath","chars_total","sentences_nomath",
      "blocks_total","words_sum","headings_count","images_count",
      "tables_count","lists_count","latex_total","latex_inline",
      "latex_display","latex_display_multi"
    )
    ok <- intersect(candidates, names(df))
    if (length(ok) == 0) return()
    updateSelectInput(session, "sectionMetric", choices = ok, selected = ok[[1]])
  })

  output$sectionMetricPlot <- renderPlot({
    full_df <- section_metrics_display()
    req(!is.null(full_df), nrow(full_df) > 0, input$sectionMetric)
    m <- input$sectionMetric
    req(m %in% names(full_df))
    # Determine selected key from the full table (which includes empty sections)
    sel <- input$sectionMetricsTable_rows_selected
    sel_key <- NULL
    if (!is.null(sel) && length(sel) == 1 && sel >= 1 && sel <= nrow(full_df)) {
      sel_key <- list(lesson_id = full_df$lesson_id[sel], section_index = if ("section_index" %in% names(full_df)) full_df$section_index[sel] else NA_integer_)
    }
    # Filter out sections with zero word count
    df <- full_df %>% dplyr::filter(is.finite(words_total) & words_total > 0)
    req(nrow(df) > 0)
    # order by current (filtered) table order
    df$.ord <- seq_len(nrow(df))
    # section label: [lesson_id]-[section_index]
    if (!("section_index" %in% names(df)) || all(is.na(df$section_index))) df$section_index <- seq_len(nrow(df))
    df$sec_label <- paste0(df$lesson_id, "-", df$section_index)
    # Highlight if selected row remains after filtering
    df$highlight <- FALSE
    if (!is.null(sel_key)) {
      hit <- which(df$lesson_id == sel_key$lesson_id & df$section_index == sel_key$section_index)
      if (length(hit) == 1) df$highlight[hit] <- TRUE
    }
    # Compute common x-range to align with the box plot
    x_max <- suppressWarnings(max(df[[m]], na.rm = TRUE))
    if (!is.finite(x_max) || x_max <= 0) x_max <- 1
    # Base bars only; distribution rendered in sectionMetricDist
    sec_labels <- rev(df$sec_label)
    # Compute left margin to align with distribution panel
    if (!("section_index" %in% names(df)) || all(is.na(df$section_index))) df$section_index <- seq_len(nrow(df))
    sec_label_chars <- nchar(paste0(df$lesson_id, "-", df$section_index))
    l_margin <- 8 + 7 * max(sec_label_chars, na.rm = TRUE) - 35  # add tick/padding offset
    p <- ggplot2::ggplot(df, ggplot2::aes(x = .data[[m]], y = factor(.ord, levels = rev(.ord), labels = sec_labels), fill = highlight)) +
      ggplot2::geom_col(color = "white") +
      ggplot2::scale_fill_manual(values = c(`TRUE` = "#d62728", `FALSE` = "#7aa6c2"), guide = "none") +
      ggplot2::labs(x = m, y = "section") +
      ggplot2::coord_cartesian(xlim = c(0, x_max)) +
      ggplot2::theme_minimal(base_size = 12) +
      ggplot2::theme(plot.margin = ggplot2::margin(t = 5, r = 5, b = 5, l = l_margin, unit = "pt"))
    p
  })

  # Section distribution (always shown above bars)
  output$sectionMetricDist <- renderPlot({
    full_df <- section_metrics_display()
    req(!is.null(full_df), nrow(full_df) > 0, input$sectionMetric)
    m <- input$sectionMetric
    req(m %in% names(full_df))
    df <- full_df %>% dplyr::filter(is.finite(words_total) & words_total > 0)
    req(nrow(df) > 0)
    x_max <- suppressWarnings(max(df[[m]], na.rm = TRUE))
    if (!is.finite(x_max) || x_max <= 0) x_max <- 1
    # Approximate left margin to align with bar plot y labels
    if (!("section_index" %in% names(df)) || all(is.na(df$section_index))) df$section_index <- seq_len(nrow(df))
    sec_label_chars <- nchar(paste0(df$lesson_id, "-", df$section_index))
    l_margin <- 8 + 7 * max(sec_label_chars, na.rm = TRUE) + 15
    # Manual alignment tweak for distribution panel (increase to move right)
    dist_left_tweak <- 4
    l_margin <- min(l_margin + dist_left_tweak, 260)
    if (!is.null(input$sectionDist) && identical(input$sectionDist, "Boxplot")) {
      if (sum(is.finite(df[[m]])) < 2) {
        ggplot2::ggplot(df, ggplot2::aes(x = .data[[m]], y = 1)) +
          ggplot2::geom_blank() +
          ggplot2::labs(x = m, y = NULL) +
          ggplot2::theme_minimal(base_size = 12)
      } else {
        ggplot2::ggplot(df, ggplot2::aes(x = .data[[m]], y = 1)) +
          ggplot2::geom_boxplot(fill = "#999999", color = "#555555") +
          ggplot2::labs(x = m, y = NULL) +
          ggplot2::coord_cartesian(xlim = c(0, x_max)) +
          ggplot2::theme_minimal(base_size = 12) +
          ggplot2::theme(plot.margin = ggplot2::margin(t = 5, r = 5, b = 5, l = l_margin, unit = "pt")) +
          ggplot2::theme(axis.text.y = ggplot2::element_blank(), axis.ticks.y = ggplot2::element_blank())
      }
    } else {
      ggplot2::ggplot(df, ggplot2::aes(x = .data[[m]])) +
        ggplot2::geom_histogram(bins = 20, fill = "#999999", color = "#555555") +
        ggplot2::labs(x = m, y = NULL) +
        ggplot2::coord_cartesian(xlim = c(0, x_max)) +
        ggplot2::theme_minimal(base_size = 12) +
        ggplot2::theme(plot.margin = ggplot2::margin(t = 5, r = 5, b = 5, l = l_margin, unit = "pt")) +
        ggplot2::theme(axis.text.y = ggplot2::element_blank(), axis.ticks.y = ggplot2::element_blank())
    }
  })

  output$sectionMetricBox <- renderPlot({
    # Use the same filtered df and x-range as the bar plot
    full_df <- section_metrics_display()
    req(!is.null(full_df), nrow(full_df) > 0, input$sectionMetric)
    m <- input$sectionMetric
    req(m %in% names(full_df))
    df <- full_df %>% dplyr::filter(
      (is.finite(words_total) & words_total > 0) |
      (is.finite(words_nomath) & words_nomath > 0) |
      (is.finite(chars_total) & chars_total > 0) |
      (is.finite(sentences_nomath) & sentences_nomath > 0)
    )
    req(nrow(df) > 0)
    x_max <- suppressWarnings(max(df[[m]], na.rm = TRUE))
    if (!is.finite(x_max) || x_max <= 0) x_max <- 1
    # Approximate left margin so the box aligns with the bar plot panel
    if (!("section_index" %in% names(df)) || all(is.na(df$section_index))) df$section_index <- seq_len(nrow(df))
    sec_label_chars <- nchar(paste0(df$lesson_id, "-", df$section_index))
    l_margin <- 8 + 7 * max(sec_label_chars, na.rm = TRUE)  # pts
    ggplot2::ggplot(df, ggplot2::aes(x = .data[[m]], y = 1)) +
      ggplot2::geom_boxplot(fill = "#999999", color = "#555555") +
      ggplot2::labs(x = m, y = NULL) +
      ggplot2::coord_cartesian(xlim = c(0, x_max)) +
      ggplot2::theme_minimal(base_size = 12) +
      ggplot2::theme(plot.margin = ggplot2::margin(t = 5, r = 5, b = 5, l = l_margin, unit = "pt")) +
      ggplot2::theme(axis.text.y = ggplot2::element_blank(), axis.ticks.y = ggplot2::element_blank())
  })

  # Aggregated per-lesson metrics (sums only; exclude non-countable metrics)
  lesson_metrics <- reactive({
    acc <- lesson_metrics_acc()
    if (!is.null(acc) && nrow(acc) > 0) return(acc)
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
    datatable(
      df,
      rownames = FALSE,
      options = list(pageLength = 10, scrollX = TRUE),
      selection = 'single'
    )
  })

  # Metric selection and distribution plot for Lesson Analysis
  numeric_metric_names <- c(
    "words_total","words_nomath","chars_total","sentences_nomath",
    "blocks_total","words_sum","headings_count","images_count",
    "tables_count","lists_count","latex_total","latex_inline",
    "latex_display","latex_display_multi"
  )

  observe({
    df <- lesson_metrics()
    if (is.null(df) || nrow(df) == 0) return()
    ok <- intersect(numeric_metric_names, names(df))
    if (length(ok) == 0) return()
    updateSelectInput(session, "lessonMetric", choices = ok, selected = ok[[1]])
  })

  output$lessonMetricPlot <- renderPlot({
    df <- lesson_metrics()
    req(!is.null(df), nrow(df) > 0, input$lessonMetric)
    m <- input$lessonMetric
    req(m %in% names(df))
    df$.ord <- seq_len(nrow(df))
    sel <- input$lessonMetricsTable_rows_selected
    df$highlight <- FALSE
    if (!is.null(sel) && length(sel) == 1 && sel >= 1 && sel <= nrow(df)) df$highlight[sel] <- TRUE
    lesson_labels <- rev(df$lesson_id)
    x_max <- suppressWarnings(max(df[[m]], na.rm = TRUE))
    if (!is.finite(x_max) || x_max <= 0) x_max <- 1
    # Base bars only; distribution rendered in lessonMetricDist
    # Compute left margin to align with distribution panel
    lesson_label_chars <- nchar(lesson_labels)
    l_margin <- 8 + 7 * max(lesson_label_chars, na.rm = TRUE) - 35
    l_margin <- max(0, min(l_margin, 220))
    p <- ggplot2::ggplot(df, ggplot2::aes(x = .data[[m]], y = factor(.ord, levels = rev(.ord), labels = lesson_labels), fill = highlight)) +
      ggplot2::geom_col(color = "white") +
      ggplot2::scale_fill_manual(values = c(`TRUE` = "#d62728", `FALSE` = "#4C78A8"), guide = "none") +
      ggplot2::labs(x = m, y = "lesson_id") +
      ggplot2::coord_cartesian(xlim = c(0, x_max)) +
      ggplot2::theme_minimal(base_size = 12) +
      ggplot2::theme(plot.margin = ggplot2::margin(t = 5, r = 5, b = 5, l = l_margin, unit = "pt"))
    p
  })

  # Lesson distribution (always shown above bars)
  output$lessonMetricDist <- renderPlot({
    df <- lesson_metrics()
    req(!is.null(df), nrow(df) > 0, input$lessonMetric)
    m <- input$lessonMetric
    req(m %in% names(df))
    x_max <- suppressWarnings(max(df[[m]], na.rm = TRUE))
    if (!is.finite(x_max) || x_max <= 0) x_max <- 1
    # Approximate left margin based on lesson_id label widths in bar plot
    lesson_label_chars <- nchar(df$lesson_id)
    l_margin <- 8 + 7 * max(lesson_label_chars, na.rm = TRUE) - 35
    # Manual alignment tweak for distribution panel (increase to move right)
    dist_left_tweak <- 38
    l_margin <- max(0, min(l_margin + dist_left_tweak, 260))
    if (!is.null(input$lessonDist) && identical(input$lessonDist, "Boxplot")) {
      if (sum(is.finite(df[[m]])) < 2) {
        ggplot2::ggplot(df, ggplot2::aes(x = .data[[m]], y = 1)) +
          ggplot2::geom_blank() +
          ggplot2::labs(x = m, y = NULL) +
          ggplot2::theme_minimal(base_size = 12)
      } else {
        ggplot2::ggplot(df, ggplot2::aes(x = .data[[m]], y = 1)) +
          ggplot2::geom_boxplot(fill = "#999999", color = "#555555") +
          ggplot2::labs(x = m, y = NULL) +
          ggplot2::coord_cartesian(xlim = c(0, x_max)) +
          ggplot2::theme_minimal(base_size = 12) +
          ggplot2::theme(plot.margin = ggplot2::margin(t = 5, r = 5, b = 5, l = l_margin, unit = "pt")) +
          ggplot2::theme(axis.text.y = ggplot2::element_blank(), axis.ticks.y = ggplot2::element_blank())
      }
    } else {
      ggplot2::ggplot(df, ggplot2::aes(x = .data[[m]])) +
        ggplot2::geom_histogram(bins = 20, fill = "#999999", color = "#555555") +
        ggplot2::labs(x = m, y = NULL) +
        ggplot2::coord_cartesian(xlim = c(0, x_max)) +
        ggplot2::theme_minimal(base_size = 12) +
        ggplot2::theme(plot.margin = ggplot2::margin(t = 5, r = 5, b = 5, l = l_margin, unit = "pt")) +
        ggplot2::theme(axis.text.y = ggplot2::element_blank(), axis.ticks.y = ggplot2::element_blank())
    }
  })

  # Lesson histogram (shown above bars when Distribution == Histogram)
  output$lessonMetricHist <- renderPlot({
    req(!is.null(input$lessonDist), identical(input$lessonDist, "Histogram"))
    df <- lesson_metrics()
    req(!is.null(df), nrow(df) > 0, input$lessonMetric)
    m <- input$lessonMetric
    req(m %in% names(df))
    x_max <- suppressWarnings(max(df[[m]], na.rm = TRUE))
    if (!is.finite(x_max) || x_max <= 0) x_max <- 1
    ggplot2::ggplot(df, ggplot2::aes(x = .data[[m]])) +
      ggplot2::geom_histogram(bins = 20, fill = "#999999", color = "#555555") +
      ggplot2::labs(x = m, y = NULL) +
      ggplot2::coord_cartesian(xlim = c(0, x_max)) +
      ggplot2::theme_minimal(base_size = 12) +
      ggplot2::theme(axis.text.y = ggplot2::element_blank(), axis.ticks.y = ggplot2::element_blank())
  })

  output$lessonMetricBox <- renderPlot({
    df <- lesson_metrics()
    req(!is.null(df), nrow(df) > 0, input$lessonMetric)
    m <- input$lessonMetric
    req(m %in% names(df))
    x_max <- suppressWarnings(max(df[[m]], na.rm = TRUE))
    if (!is.finite(x_max) || x_max <= 0) x_max <- 1
    # Approximate left margin from lesson_id label widths
    lesson_label_chars <- nchar(df$lesson_id)
    l_margin <- 8 + 7 * max(lesson_label_chars, na.rm = TRUE)  # pts
    ggplot2::ggplot(df, ggplot2::aes(x = .data[[m]], y = 1)) +
      ggplot2::geom_boxplot(fill = "#999999", color = "#555555") +
      ggplot2::labs(x = m, y = NULL) +
      ggplot2::coord_cartesian(xlim = c(0, x_max)) +
      ggplot2::theme_minimal(base_size = 12) +
      ggplot2::theme(plot.margin = ggplot2::margin(t = 5, r = 5, b = 5, l = l_margin, unit = "pt")) +
      ggplot2::theme(axis.text.y = ggplot2::element_blank(), axis.ticks.y = ggplot2::element_blank())
  })

  # --- Dynamic Level n analysis tabs (appended to mainTabs) ---
  if (is.null(.state$level_tabs_added)) .state$level_tabs_added <- FALSE
  session$onFlushed(function() { return() }, once = FALSE)

  # Progress counter next to the Analyse button
  output$analysisProgress <- renderText({
    info <- progress_info()
    total <- (info$total %||% 0L)
    done  <- (info$done %||% 0L)
    if (total <= 0L) "" else paste0(done, " / ", total)
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