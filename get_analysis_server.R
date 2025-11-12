# Shiny server: selections, content fetch, section metrics, and outputs
source("get_analysis_pipeline.R")
source("R/helpers_shared_x.R")
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


  # Compose a distribution (top) and bars (bottom) plot with shared x-scale and aligned left axis
  # highlight_labels: optional character vector of labels to highlight in the bar chart
  make_dist_bars <- function(df, metric, y_title, labels, dist_type = c("Histogram","Boxplot"), rel_heights = c(1, 3), highlight_labels = NULL) {
    dist_type <- match.arg(dist_type)
    # Always aggregate by labels first so each bar is a single piece and the distribution reflects totals
    label_levels <- unique(labels)
    df <- tibble::tibble(.label = labels, .value = df[[metric]]) %>%
      dplyr::group_by(.label) %>%
      dplyr::summarise(.value = sum(.value, na.rm = TRUE), .groups = "drop") %>%
      dplyr::mutate(.ord = match(.label, label_levels)) %>%
      dplyr::arrange(.ord) %>%
      dplyr::mutate(.highlight = if (!is.null(highlight_labels)) .label %in% highlight_labels else FALSE)
    x_max <- suppressWarnings(max(df$.value, na.rm = TRUE)); if (!is.finite(x_max) || x_max <= 0) x_max <- 1

    # Bars (bottom)
    # Precompute y factor with labels to avoid NSE warnings and enable label-based clicks
    df$y_factor <- factor(df$.label, levels = label_levels)
    p_bars <- ggplot2::ggplot(
      df, ggplot2::aes(x = .data[[".value"]], y = .data[["y_factor"]], fill = .data[[".highlight"]])
    ) +
      ggplot2::geom_col(color = "white") +
      ggplot2::scale_fill_manual(values = c(`FALSE` = "#4C78A8", `TRUE` = "#d62728"), guide = "none") +
      ggplot2::labs(x = metric, y = y_title) +
      ggplot2::coord_cartesian(xlim = c(0, x_max)) +
      ggplot2::theme_minimal(base_size = 12)

    # Distribution (top)
    if (identical(dist_type, "Boxplot")) {
      p_dist <- ggplot2::ggplot(df, ggplot2::aes(x = .data[[".value"]], y = 1)) +
        ggplot2::geom_boxplot(fill = "#999999", color = "#555555") +
        ggplot2::labs(x = metric, y = NULL) +
        ggplot2::coord_cartesian(xlim = c(0, x_max)) +
        ggplot2::theme_minimal(base_size = 12) +
        ggplot2::theme(axis.text.y = ggplot2::element_blank(), axis.ticks.y = ggplot2::element_blank())
    } else {
      p_dist <- ggplot2::ggplot(df, ggplot2::aes(x = .data[[".value"]])) +
        ggplot2::geom_histogram(bins = 20, fill = "#999999", color = "#555555") +
        ggplot2::labs(x = metric, y = NULL) +
        ggplot2::coord_cartesian(xlim = c(0, x_max)) +
        ggplot2::theme_minimal(base_size = 12) +
        ggplot2::theme(axis.text.y = ggplot2::element_blank(), axis.ticks.y = ggplot2::element_blank())
    }

    aligned <- cowplot::align_plots(p_dist, p_bars, align = "v", axis = "l")
    cowplot::plot_grid(aligned[[1]], aligned[[2]], ncol = 1, rel_heights = rel_heights)
  }

  # Bars-only helper in the same label order
  make_bars_only <- function(df, metric, y_title, labels, highlight_labels = NULL) {
    label_levels <- unique(labels)
    df <- tibble::tibble(.label = labels, .value = df[[metric]]) %>%
      dplyr::group_by(.label) %>%
      dplyr::summarise(.value = sum(.value, na.rm = TRUE), .groups = "drop") %>%
      dplyr::mutate(.ord = match(.label, label_levels)) %>%
      dplyr::arrange(.ord) %>%
      dplyr::mutate(.highlight = if (!is.null(highlight_labels)) .label %in% highlight_labels else FALSE)
    x_max <- suppressWarnings(max(df$.value, na.rm = TRUE)); if (!is.finite(x_max) || x_max <= 0) x_max <- 1
    df$y_factor <- factor(df$.label, levels = label_levels)
    ggplot2::ggplot(
      df, ggplot2::aes(x = .data[[".value"]], y = .data[["y_factor"]], fill = .data[[".highlight"]])
    ) +
      ggplot2::geom_col(color = "white") +
      ggplot2::scale_fill_manual(values = c(`FALSE` = "#4C78A8", `TRUE` = "#d62728"), guide = "none") +
      ggplot2::labs(x = metric, y = y_title) +
      ggplot2::coord_cartesian(xlim = c(0, x_max)) +
      ggplot2::theme_minimal(base_size = 12)
  }


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
    # Backward compatibility: ensure heading-by-level columns exist on section table (cached rows may lack them)
    ensure_cols <- c("headings_h1","headings_h2","headings_h3","headings_h6","nodes_native","nodes_custom")
    missing <- setdiff(ensure_cols, names(sec_tbl))
    if (length(missing)) {
      for (nm in missing) sec_tbl[[nm]] <- 0L
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
        headings_h1 = sum(headings_h1, na.rm = TRUE),
        headings_h2 = sum(headings_h2, na.rm = TRUE),
        headings_h3 = sum(headings_h3, na.rm = TRUE),
        
        headings_h6 = sum(headings_h6, na.rm = TRUE),
        nodes_native = sum(nodes_native, na.rm = TRUE),
        nodes_custom = sum(nodes_custom, na.rm = TRUE),
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

  # Plot-driven filters
  section_filter <- reactiveVal(NULL)
  lesson_filter  <- reactiveVal(NULL)

  output$sectionMetricsTable <- renderDT({
    df <- section_metrics_display()
    if (is.null(df) || nrow(df) == 0) {
      return(datatable(data.frame()))
    }
    filt <- section_filter()
    if (!is.null(filt)) {
      m <- isolate(input$sectionMetric)
      if (!is.null(m) && m %in% names(df)) {
        if (!("section_index" %in% names(df)) || all(is.na(df$section_index))) df$section_index <- seq_len(nrow(df))
        df <- df %>% dplyr::left_join(content_tree %>% dplyr::select(lesson_id, lesson) %>% dplyr::distinct(), by = "lesson_id") %>%
          dplyr::mutate(sec_label = paste0(lesson, "-", section_index))
        if (identical(filt$type, "labels")) {
          df <- df %>% dplyr::filter(sec_label %in% filt$values)
        } else if (identical(filt$type, "range") && length(filt$values) == 2) {
          rng <- sort(as.numeric(filt$values))
          df <- df %>% dplyr::filter(is.finite(.data[[m]]) & .data[[m]] >= rng[1] & .data[[m]] <= rng[2])
        }
      }
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

  # Selection/filter state driven by Plotly interactions
  section_filter <- reactiveVal(NULL)  # list(type="labels"| "range", values=c(...))
  lesson_filter  <- reactiveVal(NULL)

  # Helper to safely read plotly event_data without warnings when not registered yet
  safe_event_data <- function(event, source_id) {
    suppressWarnings(
      tryCatch(plotly::event_data(event, source = source_id), error = function(e) NULL)
    )
  }
  # ---- NEW: Section charts using shared scale + ggiraph ----
  section_plot_data <- reactive({
    full_df <- section_metrics_display()
    req(!is.null(full_df), nrow(full_df) > 0, input$sectionMetric)
    m <- input$sectionMetric
    df <- full_df %>% dplyr::filter(is.finite(words_total) & words_total > 0)
    req(nrow(df) > 0)
    if (!("section_index" %in% names(df)) || all(is.na(df$section_index))) df$section_index <- seq_len(nrow(df))
    df <- df %>%
      dplyr::left_join(content_tree %>% dplyr::select(lesson_id, lesson) %>% dplyr::distinct(), by = "lesson_id") %>%
      dplyr::mutate(sec_label = paste0(lesson, "-", section_index)) %>%
      dplyr::group_by(sec_label) %>%
      dplyr::summarise(value = suppressWarnings(as.numeric(sum(.data[[m]], na.rm = TRUE))), .groups = "drop") %>%
      dplyr::distinct(sec_label, .keep_all = TRUE) %>%
      dplyr::mutate(value = as.numeric(value)) %>%
      dplyr::filter(is.finite(value))
    list(df = df, metric_name = m)
  })

  section_shared_x <- reactive({
    pd <- section_plot_data()
    compute_shared_x(datasets = list(pd$df, pd$df), x_cols = c("value","value"), n_breaks = 6)
  })

  section_highlights <- reactive({
    pd <- section_plot_data(); df <- pd$df
    sel <- input$sectionMetricsTable_rows_selected
    if (!is.null(sel) && length(sel) == 1 && sel >= 1 && sel <= nrow(df)) df$sec_label[sel] else character(0)
  })

  output$sectionTop_gx <- ggiraph::renderGirafe({
    pd <- section_plot_data()
    if (!is.null(input$sectionDist) && identical(input$sectionDist, "Boxplot")) {
      make_top_box_interactive(data = pd$df, x_col = "value", shared = section_shared_x(), left_pad_pt = 120,
                               add_quartile_bands = TRUE, add_outliers = TRUE)
    } else {
      make_top_hist_interactive(data = pd$df, x_col = "value", shared = section_shared_x(), bins = 20, left_pad_pt = 120)
    }
  })

  output$sectionBars_gx <- ggiraph::renderGirafe({
    pd <- section_plot_data()
    make_bottom_bars_interactive(data = pd$df, x_col = "value", label_col = "sec_label",
                                 shared = section_shared_x(), left_pad_pt = 120,
                                 highlight_labels = section_highlights())
  })

  observeEvent(input$sectionBars_gx_selected, {
    labs <- input$sectionBars_gx_selected
    req(length(labs) >= 1)
    pd <- section_plot_data()
    idx <- match(labs[1], pd$df$sec_label)
    if (is.finite(idx)) selectRows(section_proxy, idx)
  })

  # ---- NEW: Section Plotly combined chart (shared x, click selects) ----
  output$sectionPlotly <- plotly::renderPlotly({
    pd <- section_plot_data()
    df <- pd$df
    req(nrow(df) > 0)
    xr <- range(c(0, df$value), na.rm = TRUE)
    if (!is.finite(xr[2]) || xr[2] <= 0) xr[2] <- 1
    xr[2] <- xr[2] * 1.05
    dist_type <- if (!is.null(input$sectionDist) && identical(input$sectionDist, "Boxplot")) "box" else "hist"
    if (identical(dist_type, "box")) {
      top <- plotly::plot_ly(x = ~df$value, type = "box", boxpoints = "outliers", orientation = "h",
                              selectedpoints = NA, fillcolor = "#D1C8F1",
                              line = list(color = "black", width = 0.5)) %>%
        plotly::layout(yaxis = list(visible = FALSE), xaxis = list(range = c(0, xr[2])), showlegend = FALSE)
    } else {
      top <- plotly::plot_ly(x = ~df$value, type = "histogram", nbinsx = 20, selectedpoints = NA,
                              marker = list(color = "#D1C8F1", line = list(color = "black", width = 0.5))) %>%
        plotly::layout(yaxis = list(visible = FALSE), xaxis = list(range = c(0, xr[2])), showlegend = FALSE)
    }
    # Preserve label order
    df$sec_label <- factor(df$sec_label, levels = unique(df$sec_label))
    bars <- plotly::plot_ly(df, x = ~value, y = ~sec_label, type = "bar", orientation = "h",
                             marker = list(color = "#D1C8F1", line = list(color = "black", width = 0.5))) %>%
      plotly::layout(yaxis = list(title = "", categoryorder = "array", categoryarray = levels(df$sec_label)),
                     xaxis = list(range = c(0, xr[2])), showlegend = FALSE)
    plt <- plotly::subplot(top, bars, nrows = 2, shareX = TRUE, heights = c(0.35, 0.65), margin = 0.02) %>%
      plotly::layout(margin = list(l = 120, r = 20, t = 10, b = 10), dragmode = "select") %>%
      plotly::config(
        displaylogo = FALSE,
        scrollZoom = FALSE,
        modeBarButtonsToRemove = c(
          'zoom2d','pan2d','lasso2d','zoomIn2d','zoomOut2d',
          'hoverClosestCartesian','hoverCompareCartesian','toggleSpikelines'
        )
      )
    plt <- plt %>% plotly::event_register('plotly_selected') %>% plotly::event_register('plotly_click')
    plt$x$source <- 'sectionBars'
    plt
  })

  observe({
    ev <- safe_event_data("plotly_click", "sectionBars")
    sel <- safe_event_data("plotly_selected", "sectionBars")
    pd <- section_plot_data(); df <- pd$df
    if (!is.null(sel) && NROW(sel)) {
      labs <- unique(as.character(sel$y))
      section_filter(list(type = "labels", values = labs))
      return()
    }
    if (!is.null(ev) && !is.null(ev$y)) {
      section_filter(list(type = "labels", values = as.character(ev$y)))
      lab <- as.character(ev$y)[1]
      idx <- match(lab, as.character(df$sec_label))
      if (is.finite(idx)) selectRows(section_proxy, idx)
    }
  })

  # ---- NEW: Lesson charts using shared scale + ggiraph ----
  lesson_plot_data <- reactive({
    # Aggregate from section metrics to current level to ensure correctness above lesson
    base <- section_metrics_display()
    req(!is.null(base), nrow(base) > 0, input$lessonMetric)
    m <- input$lessonMetric
    level <- input$aggLevel %||% "lesson"
    base[[m]] <- suppressWarnings(as.numeric(base[[m]]))
    base <- base[is.finite(base[[m]]), , drop = FALSE]
    if (identical(level, "lesson")) {
      df_small <- base %>%
        dplyr::group_by(lesson_id) %>%
        dplyr::summarise(value = sum(.data[[m]], na.rm = TRUE), .groups = "drop") %>%
        dplyr::left_join(content_tree %>% dplyr::select(lesson_id, lesson) %>% dplyr::distinct(), by = "lesson_id") %>%
        dplyr::transmute(node = dplyr::coalesce(lesson, lesson_id), value = as.numeric(value)) %>%
        dplyr::distinct(node, .keep_all = TRUE)
    } else {
      if (!(level %in% names(content_tree))) return(list(df = tibble::tibble(node = character(), value = numeric()), metric_name = m))
      df_small <- base %>%
        dplyr::left_join(content_tree %>% dplyr::select(lesson_id, !!rlang::sym(level)) %>% dplyr::distinct(), by = "lesson_id") %>%
        dplyr::group_by(node = .data[[level]]) %>%
        dplyr::summarise(value = sum(.data[[m]], na.rm = TRUE), .groups = "drop") %>%
        dplyr::distinct(node, .keep_all = TRUE) %>%
        dplyr::mutate(value = as.numeric(value))
    }
    df_small <- df_small %>% dplyr::filter(is.finite(value))
    list(df = df_small, metric_name = m)
  })

  lesson_shared_x <- reactive({
    pd <- lesson_plot_data()
    compute_shared_x(datasets = list(pd$df, pd$df), x_cols = c("value","value"), n_breaks = 6)
  })

  lesson_highlights <- reactive({
    pd <- lesson_plot_data()
    sel <- input$lessonMetricsTable_rows_selected
    if (!is.null(sel) && length(sel) == 1 && sel >= 1 && sel <= nrow(pd$df)) pd$df$node[sel] else character(0)
  })

  output$lessonTop_gx <- ggiraph::renderGirafe({
    pd <- lesson_plot_data()
    if (!is.null(input$lessonDist) && identical(input$lessonDist, "Boxplot")) {
      make_top_box_interactive(data = pd$df, x_col = "value", shared = lesson_shared_x(), left_pad_pt = 120,
                               add_quartile_bands = TRUE, add_outliers = TRUE)
    } else {
      make_top_hist_interactive(data = pd$df, x_col = "value", shared = lesson_shared_x(), bins = 20, left_pad_pt = 120)
    }
  })

  output$lessonBars_gx <- ggiraph::renderGirafe({
    pd <- lesson_plot_data()
    make_bottom_bars_interactive(data = pd$df, x_col = "value", label_col = "node",
                                 shared = lesson_shared_x(), left_pad_pt = 120,
                                 highlight_labels = lesson_highlights())
  })

  observeEvent(input$lessonBars_gx_selected, {
    labs <- input$lessonBars_gx_selected
    req(length(labs) >= 1)
    pd <- lesson_plot_data()
    idx <- match(labs[1], pd$df$node)
    if (is.finite(idx)) selectRows(lesson_proxy, idx)
  })

  # ---- NEW: Lesson Plotly combined chart (shared x, click selects) ----
  output$lessonPlotly <- plotly::renderPlotly({
    pd <- lesson_plot_data()
    df <- pd$df
    req(nrow(df) > 0)
    xr <- range(c(0, df$value), na.rm = TRUE)
    dist_type <- if (!is.null(input$lessonDist) && identical(input$lessonDist, "Boxplot")) "box" else "hist"
    if (identical(dist_type, "box")) {
      top <- plotly::plot_ly(x = ~df$value, type = "box", boxpoints = "outliers", orientation = "h",
                              selectedpoints = NA, fillcolor = "#D1C8F1",
                              line = list(color = "black", width = 0.5)) %>%
        plotly::layout(yaxis = list(visible = FALSE), xaxis = list(range = c(0, xr[2])), showlegend = FALSE)
    } else {
      top <- plotly::plot_ly(x = ~df$value, type = "histogram", nbinsx = 20, selectedpoints = NA,
                              marker = list(color = "#D1C8F1", line = list(color = "black", width = 0.5))) %>%
        plotly::layout(yaxis = list(visible = FALSE), xaxis = list(range = c(0, xr[2])), showlegend = FALSE)
    }
    df$node <- factor(df$node, levels = unique(df$node))
    bars <- plotly::plot_ly(df, x = ~value, y = ~node, type = "bar", orientation = "h",
                             marker = list(color = "#D1C8F1", line = list(color = "black", width = 0.5))) %>%
      plotly::layout(yaxis = list(title = "", categoryorder = "array", categoryarray = levels(df$node)),
                     xaxis = list(range = c(0, xr[2])), showlegend = FALSE)
    plt <- plotly::subplot(top, bars, nrows = 2, shareX = TRUE, heights = c(0.35, 0.65), margin = 0.02) %>%
      plotly::layout(margin = list(l = 120, r = 20, t = 10, b = 10), dragmode = "select") %>%
      plotly::config(
        displaylogo = FALSE,
        scrollZoom = FALSE,
        modeBarButtonsToRemove = c(
          'zoom2d','pan2d','lasso2d','zoomIn2d','zoomOut2d',
          'hoverClosestCartesian','hoverCompareCartesian','toggleSpikelines'
        )
      )
    plt <- plt %>% plotly::event_register('plotly_selected') %>% plotly::event_register('plotly_click')
    plt$x$source <- 'lessonBars'
    plt
  })

  # Populate benchmark metrics choices
  observe({
    df <- agg_metrics()
    if (is.null(df) || nrow(df) == 0) return()
    ok <- intersect(numeric_metric_names, names(df))
    if (length(ok) == 0) return()
    updatePickerInput(session, "benchmarkMetrics", choices = ok, selected = ok[1])
  })

  # Helper to compute group df for an arbitrary metric at current aggLevel
  get_group_values <- function(m) {
    base <- section_metrics_display()
    if (is.null(base) || nrow(base) == 0) return(tibble::tibble(node = character(), value = numeric()))
    lvl <- input$aggLevel %||% "lesson"
    base[[m]] <- suppressWarnings(as.numeric(base[[m]]))
    base <- base[is.finite(base[[m]]), , drop = FALSE]
    if (identical(lvl, "lesson")) {
      out <- base %>%
        dplyr::group_by(lesson_id) %>%
        dplyr::summarise(value = sum(.data[[m]], na.rm = TRUE), .groups = "drop") %>%
        dplyr::left_join(content_tree %>% dplyr::select(lesson_id, lesson) %>% dplyr::distinct(), by = "lesson_id") %>%
        dplyr::transmute(node = dplyr::coalesce(lesson, lesson_id), value = as.numeric(value)) %>%
        dplyr::distinct(node, .keep_all = TRUE)
    } else if (lvl %in% names(content_tree)) {
      out <- base %>%
        dplyr::left_join(content_tree %>% dplyr::select(lesson_id, !!rlang::sym(lvl)) %>% dplyr::distinct(), by = "lesson_id") %>%
        dplyr::group_by(node = .data[[lvl]]) %>%
        dplyr::summarise(value = sum(.data[[m]], na.rm = TRUE), .groups = "drop") %>%
        dplyr::distinct(node, .keep_all = TRUE) %>%
        dplyr::mutate(value = as.numeric(value))
    } else {
      out <- tibble::tibble(node = character(), value = numeric())
    }
    out
  }

  # Helper: build number line per rules for a metric
  build_benchmark_plot <- function(df, metric_name) {
    # Determine selected labels from plot selection or table selection
    sel_ev  <- safe_event_data('plotly_selected', 'lessonBars')
    click_ev <- safe_event_data('plotly_click', 'lessonBars')
    sel_labels <- character(0)
    if (!is.null(sel_ev) && NROW(sel_ev)) {
      sel_labels <- unique(as.character(sel_ev$y))
    } else if (!is.null(click_ev) && !is.null(click_ev$y)) {
      sel_labels <- as.character(click_ev$y[1])
    } else {
      # fallback to table selection
      row <- input$lessonMetricsTable_rows_selected
      if (!is.null(row) && length(row) == 1 && row >= 1 && row <= nrow(df)) sel_labels <- df$node[row]
    }
    # Build number line per rules
    xr <- range(c(0, df$value), na.rm = TRUE); if (!is.finite(xr[2]) || xr[2] <= 0) xr[2] <- 1; xr[2] <- xr[2] * 1.05
    vals <- df$value[is.finite(df$value)]
    n <- length(vals)
    q <- if (n >= 2) stats::quantile(vals, probs = c(.25,.5,.75), type = 7, names = FALSE) else rep(NA_real_, 3)
    iqr <- if (all(is.finite(q))) (q[3] - q[1]) else NA_real_
    whisk_lo <- if (is.finite(iqr)) max(min(vals, na.rm = TRUE), q[1] - 1.5 * iqr) else NA_real_
    whisk_hi <- if (is.finite(iqr)) min(max(vals, na.rm = TRUE), q[3] + 1.5 * iqr) else NA_real_
    selected_df <- df %>% dplyr::filter(node %in% sel_labels) %>% dplyr::mutate(y = 0)
    peers_df    <- df %>% dplyr::filter(!(node %in% sel_labels)) %>% dplyr::mutate(y = 0)
    show_mean <- FALSE

    p <- plotly::plot_ly()
    # Rules
    if (n < 10) {
      # show all dots + median line (+ optional mean)
      if (nrow(peers_df)) {
        p <- p %>% plotly::add_trace(data = peers_df, x = ~value, y = ~y, type = 'scatter', mode = 'markers',
                                     marker = list(color = '#D1C8F1', size = 10, line = list(color = 'black', width = 0.2)),
                                     hoverinfo = 'text',
                                     text = ~paste0(peers_df$node, ': ', signif(peers_df$value, 5)))
      }
      if (nrow(selected_df)) {
        p <- p %>% plotly::add_trace(data = selected_df, x = ~value, y = ~y, type = 'scatter', mode = 'markers',
                                     marker = list(color = '#FFE648', size = 12, line = list(color = 'black', width = 0.6)),
                                     hoverinfo = 'text',
                                     text = ~paste0(selected_df$node, ' (selected): ', signif(selected_df$value, 5)))
      }
      shapes <- list()
      if (is.finite(q[2])) {
        shapes <- c(shapes, list(list(type = 'line', x0 = q[2], x1 = q[2], y0 = -0.2, y1 = 0.2, line = list(color = 'black', width = 1.5))))
      }
      if (isTRUE(show_mean)) {
        mu <- mean(vals, na.rm = TRUE)
        shapes <- c(shapes, list(list(type = 'line', x0 = mu, x1 = mu, y0 = -0.2, y1 = 0.2, line = list(color = 'grey40', width = 1, dash = 'dot'))))
      }
      p <- p %>% plotly::layout(shapes = shapes)
    } else if (n < 20) {
      # show box only (Q1-median-Q3), tail dots only
      shapes <- list()
      if (all(is.finite(q))) {
        shapes <- c(shapes, list(
          list(type = 'rect', x0 = q[1], x1 = q[3], y0 = -0.15, y1 = 0.15, line = list(color = 'black', width = 1), fillcolor = '#EEEEEE', opacity = 0.5),
          list(type = 'line', x0 = q[2], x1 = q[2], y0 = -0.2, y1 = 0.2, line = list(color = 'black', width = 1.5))
        ))
      }
      tails <- peers_df %>% dplyr::filter((is.finite(q[1]) & value < q[1]) | (is.finite(q[3]) & value > q[3]))
      if (nrow(tails)) {
        p <- p %>% plotly::add_trace(data = tails, x = ~value, y = ~y, type = 'scatter', mode = 'markers',
                                     marker = list(color = '#D1C8F1', size = 10, line = list(color = 'black', width = 0.2)),
                                     hoverinfo = 'text',
                                     text = ~paste0(tails$node, ': ', signif(tails$value, 5)))
      }
      if (nrow(selected_df)) {
        p <- p %>% plotly::add_trace(data = selected_df, x = ~value, y = ~y, type = 'scatter', mode = 'markers',
                                     marker = list(color = '#FFE648', size = 12, line = list(color = 'black', width = 0.6)),
                                     hoverinfo = 'text',
                                     text = ~paste0(selected_df$node, ' (selected): ', signif(selected_df$value, 5)))
      }
      p <- p %>% plotly::layout(shapes = shapes)
    } else {
      # n >= 20 : box + whiskers with outliers
      p <- p %>% plotly::add_trace(x = ~vals, y = ~rep(0, length(vals)), type = 'box', orientation = 'h',
                                   boxpoints = 'outliers', hoverinfo = 'skip', showlegend = FALSE)
      if (nrow(selected_df)) {
        p <- p %>% plotly::add_trace(data = selected_df, x = ~value, y = ~y, type = 'scatter', mode = 'markers',
                                     marker = list(color = '#FFE648', size = 12, line = list(color = 'black', width = 0.6)),
                                     hoverinfo = 'text',
                                     text = ~paste0(selected_df$node, ' (selected): ', signif(selected_df$value, 5)))
      }
    }
    p %>% plotly::layout(yaxis = list(visible = FALSE), xaxis = list(range = c(0, xr[2]), title = metric_name),
                         margin = list(l = 40, r = 20, t = 10, b = 40), showlegend = FALSE)
  }

  # Dynamic container for multiple benchmark plots
  output$benchmarkContainer <- renderUI({
    mets <- input$benchmarkMetrics
    if (is.null(mets) || length(mets) == 0) return(tags$div("Select one or more metrics above"))
    tagList(lapply(mets, function(m) {
      id <- paste0("benchmarkPlot_", m)
      fluidRow(
        column(width = 3, tags$div(style = "text-align:right; padding-right:8px; font-weight:600;", m)),
        column(width = 9, plotly::plotlyOutput(id, height = "240px"))
      )
    }))
  })

  observe({
    mets <- input$benchmarkMetrics
    if (is.null(mets) || length(mets) == 0) return()
    for (m in mets) {
      local({
        metric <- m
        output[[paste0("benchmarkPlot_", metric)]] <- plotly::renderPlotly({
          df <- get_group_values(metric)
          req(nrow(df) > 0)
          build_benchmark_plot(df, metric)
        })
      })
    }
  })

  # ---- Metric analysis: scatterplot matrix for selected metrics ----
  observe({
    df <- agg_metrics()
    if (is.null(df) || nrow(df) == 0) return()
    ok <- intersect(numeric_metric_names, names(df))
    if (length(ok) == 0) return()
    # Preselect up to 4 metrics if none selected
    default_sel <- head(ok, 4)
    updatePickerInput(session, "metricMatrixMetrics", choices = ok, selected = input$metricMatrixMetrics %||% default_sel)
  })

  output$metricMatrixPlot <- plotly::renderPlotly({
    mets <- input$metricMatrixMetrics
    req(!is.null(mets), length(mets) >= 2)
    # Build wide data of selected metrics for current level
    dfs <- lapply(mets, function(m) {
      d <- get_group_values(m)
      names(d) <- c("node", m)
      d
    })
    wide <- Reduce(function(a, b) dplyr::full_join(a, b, by = "node"), dfs)
    # Determine selection
    sel_ev  <- safe_event_data('plotly_selected', 'lessonBars')
    click_ev <- safe_event_data('plotly_click', 'lessonBars')
    sel_labels <- character(0)
    if (!is.null(sel_ev) && NROW(sel_ev)) {
      sel_labels <- unique(as.character(sel_ev$y))
    } else if (!is.null(click_ev) && !is.null(click_ev$y)) {
      sel_labels <- as.character(click_ev$y[1])
    } else {
      row <- input$lessonMetricsTable_rows_selected
      if (!is.null(row) && length(row) == 1 && row >= 1 && row <= nrow(wide)) sel_labels <- wide$node[row]
    }
    wide$.selected <- wide$node %in% sel_labels
    k <- length(mets)
    plots <- list()
    for (i in seq_len(k)) {
      for (j in seq_len(k)) {
        xmet <- mets[j]; ymet <- mets[i]
        # Create scatter or diagonal annotation
        if (i == j) {
          p <- plotly::plot_ly() %>%
            plotly::add_annotations(x = 0.5, y = 0.5, text = xmet, showarrow = FALSE, xref = "paper", yref = "paper", font = list(size = 12)) %>%
            plotly::layout(xaxis = list(visible = FALSE), yaxis = list(visible = FALSE))
        } else {
          peers <- wide %>% dplyr::filter(is.finite(.data[[xmet]]) & is.finite(.data[[ymet]]) & !.selected)
          sels  <- wide %>% dplyr::filter(is.finite(.data[[xmet]]) & is.finite(.data[[ymet]]) & .selected)
          p <- plotly::plot_ly()
          if (nrow(peers)) {
            p <- p %>% plotly::add_trace(data = peers, x = peers[[xmet]], y = peers[[ymet]], type = 'scatter', mode = 'markers',
                                         marker = list(color = '#D1C8F1', size = 7, line = list(color = 'black', width = 0.2)),
                                         hoverinfo = 'text',
                                         text = ~paste0(peers$node, '<br>', xmet, ': ', signif(peers[[xmet]], 5), '<br>', ymet, ': ', signif(peers[[ymet]], 5)))
          }
          if (nrow(sels)) {
            p <- p %>% plotly::add_trace(data = sels, x = sels[[xmet]], y = sels[[ymet]], type = 'scatter', mode = 'markers',
                                         marker = list(color = '#FFE648', size = 9, line = list(color = 'black', width = 0.6)),
                                         hoverinfo = 'text',
                                         text = ~paste0(sels$node, ' (selected)<br>', xmet, ': ', signif(sels[[xmet]], 5), '<br>', ymet, ': ', signif(sels[[ymet]], 5)))
          }
          p <- p %>% plotly::layout(xaxis = list(title = if (i == k) xmet else "", showgrid = FALSE),
                                    yaxis = list(title = if (j == 1) ymet else "", showgrid = FALSE))
        }
        plots[[length(plots) + 1L]] <- p
      }
    }
    h <- 220 * k
    subplot <- plotly::subplot(plots, nrows = k, shareX = FALSE, shareY = FALSE, margin = 0.02) %>%
      plotly::layout(margin = list(l = 60, r = 20, t = 10, b = 40), showlegend = FALSE)
    subplot$sizingPolicy$defaultHeight <- h
    subplot
  })

  observe({
    ev <- safe_event_data("plotly_click", "lessonBars")
    sel <- safe_event_data("plotly_selected", "lessonBars")
    pd <- lesson_plot_data(); df <- pd$df
    if (!is.null(sel) && NROW(sel)) {
      labs <- unique(as.character(sel$y))
      lesson_filter(list(type = "labels", values = labs))
      return()
    }
    if (!is.null(ev) && !is.null(ev$y)) {
      lesson_filter(list(type = "labels", values = as.character(ev$y)))
      lab <- as.character(ev$y)[1]
      idx <- match(lab, as.character(df$node))
      if (is.finite(idx)) selectRows(lesson_proxy, idx)
    }
  })

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
    m <- input$sectionMetric
    # Filter exactly as the plot: keep only sections with positive words_total
    df <- full_df %>% dplyr::filter(is.finite(words_total) & words_total > 0)
    req(nrow(df) > 0)
    if (!("section_index" %in% names(df)) || all(is.na(df$section_index))) df$section_index <- seq_len(nrow(df))
    df <- df %>% dplyr::left_join(content_tree %>% dplyr::select(lesson_id, lesson) %>% dplyr::distinct(), by = "lesson_id")
    df$sec_label <- paste0(df$lesson, "-", df$section_index)
    # Build the same aggregated frame used for plotting, preserving incoming label order
    label_levels <- unique(df$sec_label)
    plot_df <- df %>%
      dplyr::group_by(sec_label) %>%
      dplyr::summarise(value = sum(.data[[m]], na.rm = TRUE), .groups = "drop") %>%
      dplyr::mutate(.ord = match(sec_label, label_levels)) %>%
      dplyr::arrange(.ord)

    # Log raw click event for diagnostics
    { cx <- input$sectionPlot_click$x; cy <- input$sectionPlot_click$y; message(paste0("Section raw click → x: ", sc(cx), ", y: ", sc(cy))) }

    # Resolve clicked label
    ylab <- NULL
    if (is.character(input$sectionPlot_click$y)) {
      ylab <- input$sectionPlot_click$y
    } else {
      # Map numeric y to nearest row index (discrete y uses 1..N in plot order from bottom to top)
      ynum <- suppressWarnings(as.numeric(input$sectionPlot_click$y))
      if (is.finite(ynum)) {
        idx <- as.integer(round(ynum))
        idx <- max(1L, min(idx, nrow(plot_df)))
        ylab <- plot_df$sec_label[idx]
      }
    }
    req(!is.null(ylab), is.character(ylab), length(ylab) == 1, !is.na(ylab))

    # Map label back to a concrete section row; then to full table index
    hit <- which(df$sec_label == ylab)[1]
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
    df <- agg_metrics()
    req(!is.null(df), nrow(df) > 0, input$lessonMetric)
    m <- input$lessonMetric
    # Build the same aggregated frame used for plotting, preserving incoming label order
    label_levels <- unique(df$node)
    plot_df <- df %>%
      dplyr::group_by(node) %>%
      dplyr::summarise(value = sum(.data[[m]], na.rm = TRUE), .groups = "drop") %>%
      dplyr::mutate(.ord = match(node, label_levels)) %>%
      dplyr::arrange(.ord)

    # Log raw click event for diagnostics
    { cx <- input$lessonPlot_click$x; cy <- input$lessonPlot_click$y; message(paste0("Lesson raw click → x: ", sc(cx), ", y: ", sc(cy))) }

    # Resolve clicked label
    ylab <- NULL
    if (is.character(input$lessonPlot_click$y)) {
      ylab <- input$lessonPlot_click$y
    } else {
      # Map numeric y to nearest row index (discrete y uses 1..N in plot order from bottom to top)
      ynum <- suppressWarnings(as.numeric(input$lessonPlot_click$y))
      if (is.finite(ynum)) {
        idx <- as.integer(round(ynum))
        idx <- max(1L, min(idx, nrow(plot_df)))
        ylab <- plot_df$node[idx]
      }
    }
    req(!is.null(ylab), is.character(ylab), length(ylab) == 1, !is.na(ylab))

    hit <- which(df$node == ylab)[1]
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
      "blocks_total","words_sum","headings_count","headings_h1","headings_h2","headings_h3","headings_h6","images_count",
      "tables_count","lists_count","latex_total","latex_inline",
      "latex_display","latex_display_multi","nodes_native","nodes_custom"
    )
    ok <- intersect(candidates, names(df))
    if (length(ok) == 0) return()
    updateSelectInput(session, "sectionMetric", choices = ok, selected = ok[[1]])
  })

  # Legacy ggplot renderers retained earlier are now unused; safe to keep or remove.
  # (Keeping minimal footprint by not rendering them.)
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
    # Build labels using lesson title instead of id: [lesson]-[section_index]
    if (!("section_index" %in% names(df)) || all(is.na(df$section_index))) df$section_index <- seq_len(nrow(df))
    df <- df %>% dplyr::left_join(content_tree %>% dplyr::select(lesson_id, lesson) %>% dplyr::distinct(), by = "lesson_id")
    df$sec_label <- paste0(df$lesson, "-", df$section_index)
    # Highlight if selected row remains after filtering
    df$highlight <- FALSE
    if (!is.null(sel_key)) {
      hit <- which(df$lesson_id == sel_key$lesson_id & df$section_index == sel_key$section_index)
      if (length(hit) == 1) df$highlight[hit] <- TRUE
    }
    highlight_labels <- df$sec_label[df$highlight]
    make_bars_only(df = df, metric = m, y_title = "section", labels = df$sec_label, highlight_labels = highlight_labels)
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
    # Ensure heading-by-level columns exist for new metric support even with older cached rows
    ensure_cols <- c("headings_h1","headings_h2","headings_h3","headings_h6","nodes_native","nodes_custom")
    missing <- setdiff(ensure_cols, names(df))
    if (length(missing)) {
      for (nm in missing) df[[nm]] <- 0L
    }
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
        headings_h1 = sum(headings_h1, na.rm = TRUE),
        headings_h2 = sum(headings_h2, na.rm = TRUE),
        headings_h3 = sum(headings_h3, na.rm = TRUE),
        
        headings_h6 = sum(headings_h6, na.rm = TRUE),
        nodes_native = sum(nodes_native, na.rm = TRUE),
        nodes_custom = sum(nodes_custom, na.rm = TRUE),
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
    df <- agg_metrics()
    if (is.null(df) || nrow(df) == 0) {
      return(datatable(data.frame()))
    }
    filt <- lesson_filter()
    if (!is.null(filt)) {
      m <- isolate(input$lessonMetric)
      if (!is.null(m) && m %in% names(df)) {
        if (identical(filt$type, "labels")) {
          df <- df %>% dplyr::filter(node %in% filt$values)
        } else if (identical(filt$type, "range") && length(filt$values) == 2) {
          rng <- sort(as.numeric(filt$values))
          df <- df %>% dplyr::filter(is.finite(.data[[m]]) & .data[[m]] >= rng[1] & .data[[m]] <= rng[2])
        }
      }
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
    "blocks_total","words_sum","headings_count","headings_h1","headings_h2","headings_h3","headings_h6","images_count",
    "tables_count","lists_count","latex_total","latex_inline",
    "latex_display","latex_display_multi","nodes_native","nodes_custom"
  )

  observe({
    df <- agg_metrics()
    if (is.null(df) || nrow(df) == 0) return()
    ok <- intersect(numeric_metric_names, names(df))
    if (length(ok) == 0) return()
    updateSelectInput(session, "lessonMetric", choices = ok, selected = ok[[1]])
  })

  # Populate aggregate level choices from content_tree (Level 0-3)
  observe({
    level_order <- c("course","module","chapter","lesson")
    present <- intersect(level_order, names(content_tree))
    if (length(present) == 0) return()
    level_index <- match(present, level_order) - 1L
    labels <- paste0("Level ", level_index, " - ", stringr::str_to_title(present))
    choices <- present; names(choices) <- labels
    default <- if ("lesson" %in% present) "lesson" else present[[length(present)]]
    updateSelectInput(session, "aggLevel", choices = choices, selected = default)
  })

  # Aggregated metrics by selected level
  agg_metrics <- reactive({
    level <- input$aggLevel %||% "lesson"
    les <- lesson_metrics()
    if (is.null(les) || nrow(les) == 0) return(tibble::tibble())
    # Ensure heading-by-level cols exist
    ensure_cols <- c("headings_h1","headings_h2","headings_h3","headings_h6","nodes_native","nodes_custom")
    for (nm in setdiff(ensure_cols, names(les))) les[[nm]] <- 0L
    if (identical(level, "lesson")) {
      # Use lesson title for labels instead of lesson_id; fall back to id if title absent
      if ("lesson" %in% names(les)) {
        out <- les %>% dplyr::mutate(node = .data[["lesson"]]) %>% dplyr::relocate(node, .after = lesson_id)
      } else {
        out <- les %>%
          dplyr::left_join(content_tree %>% dplyr::select(lesson_id, lesson) %>% dplyr::distinct(), by = "lesson_id") %>%
          dplyr::mutate(node = .data[["lesson"]]) %>%
          dplyr::relocate(node, .after = lesson_id)
      }
      return(out %>% dplyr::distinct(node, .keep_all = TRUE))
    }
    if (!(level %in% names(content_tree))) return(tibble::tibble())
    join <- content_tree %>% dplyr::select(lesson_id, !!rlang::sym(level)) %>% dplyr::distinct()
    out <- les %>%
      dplyr::left_join(join, by = "lesson_id") %>%
      dplyr::group_by(.data[[level]]) %>%
      dplyr::summarise(
        plaintext = stringr::str_trunc(stringr::str_squish(paste(plaintext, collapse = " ")), 100),
        words_total = sum(words_total, na.rm = TRUE),
        words_nomath = sum(words_nomath, na.rm = TRUE),
        chars_total = sum(chars_total, na.rm = TRUE),
        sentences_nomath = sum(sentences_nomath, na.rm = TRUE),
        blocks_total = sum(blocks_total, na.rm = TRUE),
        words_sum = sum(words_sum, na.rm = TRUE),
        headings_count = sum(headings_count, na.rm = TRUE),
        headings_h1 = sum(headings_h1, na.rm = TRUE),
        headings_h2 = sum(headings_h2, na.rm = TRUE),
        headings_h3 = sum(headings_h3, na.rm = TRUE),
        headings_h6 = sum(headings_h6, na.rm = TRUE),
        nodes_native = sum(nodes_native, na.rm = TRUE),
        nodes_custom = sum(nodes_custom, na.rm = TRUE),
        images_count = sum(images_count, na.rm = TRUE),
        tables_count = sum(tables_count, na.rm = TRUE),
        lists_count = sum(lists_count, na.rm = TRUE),
        latex_total = sum(latex_total, na.rm = TRUE),
        latex_inline = sum(latex_inline, na.rm = TRUE),
        latex_display = sum(latex_display, na.rm = TRUE),
        latex_display_multi = sum(latex_display_multi, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      dplyr::rename(node = !!rlang::sym(level)) %>%
      # Order by parent then by node (alphabetical fallback)
      {
        parent_col <- dplyr::case_when(
          level == "lesson"  ~ "chapter",
          level == "chapter" ~ "module",
          level == "module"  ~ "course",
          TRUE ~ NA_character_
        )
        if (!is.na(parent_col) && parent_col %in% names(content_tree)) {
          dplyr::left_join(., content_tree %>% dplyr::select(lesson_id, !!rlang::sym(level), !!rlang::sym(parent_col)) %>% dplyr::distinct(),
                           by = stats::setNames(level, "node")) %>%
            dplyr::rename(parent = !!rlang::sym(parent_col)) %>%
            dplyr::arrange(parent, node) %>%
            dplyr::select(-parent)
        } else {
          dplyr::arrange(., node)
        }
      }
    out %>% dplyr::distinct(node, .keep_all = TRUE)
  })

  output$lessonMetricPlot <- renderPlot({
    df <- agg_metrics()
    req(!is.null(df), nrow(df) > 0, input$lessonMetric)
    m <- input$lessonMetric
    req(m %in% names(df))
    labels <- df$node
    ylab <- (input$aggLevel %||% "lesson")
    # Highlight currently selected row in the lesson table
    sel <- input$lessonMetricsTable_rows_selected
    highlight_labels <- if (!is.null(sel) && length(sel) == 1 && sel >= 1 && sel <= nrow(df)) labels[sel] else character(0)
    make_bars_only(df = df, metric = m, y_title = ylab, labels = labels, highlight_labels = highlight_labels)
  })

  # Lesson distribution (always shown above bars)
  output$lessonMetricDist <- renderPlot({
    df <- agg_metrics()
    req(!is.null(df), nrow(df) > 0, input$lessonMetric)
    m <- input$lessonMetric
    req(m %in% names(df))
    x_max <- suppressWarnings(max(df[[m]], na.rm = TRUE))
    if (!is.finite(x_max) || x_max <= 0) x_max <- 1
    # Approximate left margin based on lesson_id label widths in bar plot
    lesson_label_chars <- nchar(df$node)
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