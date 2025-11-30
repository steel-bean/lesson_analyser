# Shiny server: selections, content fetch, section metrics, and outputs
source("get_analysis_pipeline.R")
source("R/helpers_shared_x.R")
server <- function(input, output, session) {

  # Configuration: Qualitative analysis cache freshness threshold
  # Set to 7 days for production, or lower for testing (e.g., 0.01 for ~15 minutes)
  QUALITATIVE_FRESHNESS_DAYS <- 7  # Change to 0.01 for testing, 7 for production

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
  .state$qualitative_enabled <- FALSE

  # Backward compatibility: ensure qa_model column exists in cached data
  if (nrow(qualitative_cache) > 0 && !"qa_model" %in% names(qualitative_cache)) {
    message("Backward compatibility: Adding qa_model column to cached qualitative data")
    # Extract model from each JSON entry
    qualitative_cache$qa_model <- sapply(qualitative_cache$qualitative_json, function(qjson) {
      if (is.na(qjson) || qjson == "") return(NA_character_)
      x <- tryCatch(jsonlite::fromJSON(qjson, simplifyVector = FALSE), error = function(e) NULL)
      if (is.null(x)) return(NA_character_)
      x$model %||% NA_character_
    })
  }

  # Clean up any duplicate qa_model columns from previous schema changes
  if (nrow(qualitative_cache) > 0) {
    col_names <- names(qualitative_cache)
    if (any(grepl("^qa_model\\.\\.\\.\\d+$", col_names))) {
      message("Cleaning up duplicate qa_model columns in cache")
      qa_model_cols <- grepl("^qa_model", col_names)
      first_qa_model_idx <- which(qa_model_cols)[1]
      cols_to_keep <- !qa_model_cols | seq_along(col_names) == first_qa_model_idx
      qualitative_cache <- qualitative_cache[, cols_to_keep]
    }
  }

  .state$qual_results <- qualitative_cache
  # Reactive progress for UI display
  progress_info <- reactiveVal(list(done = 0L, total = 0L))

  # Helper: extract rubric scores (supports both rubric_scores and rubric.*$score shapes)
  extract_qa_scores <- function(qjson) {
    cols <- list(
      qa_template_fit = NA_real_,
      qa_template_structure_compliance = NA_real_,
      qa_cognitive_integrity = NA_real_,
      qa_template_boundary_discipline = NA_real_,
      qa_progressive_model_principle = NA_real_,
      qa_lesson_economy_and_cognitive_load = NA_real_,
      qa_priority_index = NA_real_
    )
    if (is.null(qjson) || is.na(qjson) || identical(qjson, "")) return(as.data.frame(cols))
    x <- tryCatch(jsonlite::fromJSON(qjson, simplifyVector = FALSE), error = function(e) NULL)
    if (is.null(x)) return(as.data.frame(cols))

    # Return early if no analysis (error case)
    if (is.null(x$analysis)) return(as.data.frame(cols))
    a <- x$analysis

    if (!is.null(a$rubric_scores)) {
      rs <- a$rubric_scores
      cols$qa_template_fit <- suppressWarnings(as.numeric(rs$template_fit %||% NA))
      cols$qa_template_structure_compliance <- suppressWarnings(as.numeric(rs$template_structure_compliance %||% NA))
      cols$qa_cognitive_integrity <- suppressWarnings(as.numeric(rs$cognitive_integrity %||% NA))
      cols$qa_template_boundary_discipline <- suppressWarnings(as.numeric(rs$template_boundary_discipline %||% NA))
      cols$qa_progressive_model_principle <- suppressWarnings(as.numeric(rs$progressive_model_principle %||% NA))
      cols$qa_lesson_economy_and_cognitive_load <- suppressWarnings(as.numeric(rs$lesson_economy_and_cognitive_load %||% NA))
    } else if (!is.null(a$rubric)) {
      r <- a$rubric
      score_of <- function(node) suppressWarnings(as.numeric((node$score %||% NA)))
      cols$qa_template_fit <- score_of(r$template_fit)
      cols$qa_template_structure_compliance <- score_of(r$template_structure_compliance)
      cols$qa_cognitive_integrity <- score_of(r$cognitive_integrity)
      cols$qa_template_boundary_discipline <- score_of(r$template_boundary_discipline)
      cols$qa_progressive_model_principle <- score_of(r$progressive_model_principle)
      cols$qa_lesson_economy_and_cognitive_load <- score_of(r$lesson_economy_and_cognitive_load)
    }
    if (!is.null(a$priority_index)) {
      # Debug: check the structure of priority_index
      message("DEBUG extract_qa_scores: priority_index structure: ", jsonlite::toJSON(a$priority_index, auto_unbox = TRUE))
      cols$qa_priority_index <- suppressWarnings(as.numeric(a$priority_index$priority_index %||% NA))
      message("DEBUG extract_qa_scores: extracted qa_priority_index value: ", cols$qa_priority_index)
    } else {
      message("DEBUG extract_qa_scores: priority_index is NULL in analysis")
    }
    as.data.frame(cols)
  }


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

    # Check analysis cache (fresh within configured threshold)
    now_ts <- Sys.time()
    fresh_cutoff <- now_ts - as.difftime(QUALITATIVE_FRESHNESS_DAYS, units = "days")
    cached <- NULL
    use_stale <- !is.null(.state$use_stale_qualitative) && .state$use_stale_qualitative

    if (!is.null(.state$analysis_cache) && nrow(.state$analysis_cache)) {
      cand <- .state$analysis_cache %>% dplyr::filter(lesson_id == lid)
      if (nrow(cand)) {
        if ("analyzed_at" %in% names(cand)) {
          # If user explicitly chose to use stale data, accept any cached data
          # Otherwise, only accept data within the freshness window
          if (use_stale) {
            cand_fresh <- cand %>% dplyr::filter(!is.na(analyzed_at))
            if (nrow(cand_fresh)) cached <- cand_fresh
          } else {
            cand_fresh <- cand %>% dplyr::filter(analyzed_at >= fresh_cutoff)
            if (nrow(cand_fresh)) cached <- cand_fresh
          }
        }
      }
    }
    # Need to fetch content for qualitative analysis even if quantitative is cached
    one_df <- NULL
    if (is.null(cached)) {
      # Pull single lesson only when cache is missing/stale
      ensure_db_proxy()
      conn <- tryCatch({
        DBI::dbConnect(
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

    # Qualitative analysis (full lesson text) if enabled
    # This runs independently of quantitative cache status
    if (isTRUE(.state$qualitative_enabled)) {
      lesson_title <- tryCatch({
        content_tree %>% dplyr::filter(lesson_id == lid) %>% dplyr::distinct(lesson) %>% dplyr::pull(lesson) %>% .[1] %||% NULL
      }, error = function(e) NULL)

      # Check if we already have a fresh SUCCESSFUL qualitative analysis for this lesson
      # Only successful analyses should be cached - failed ones should be retried
      # Get the LATEST successful analysis (regardless of model) within the 7-day window
      existing_qual <- .state$qual_results %>%
        dplyr::filter(lesson_id == lid)

      has_fresh_qual <- FALSE
      # Check if user requested fresh analysis (force_fresh flag) or to use stale cache
      force_fresh <- !is.null(.state$force_fresh_qualitative) && .state$force_fresh_qualitative
      use_stale <- !is.null(.state$use_stale_qualitative) && .state$use_stale_qualitative

      if (!force_fresh && nrow(existing_qual) > 0 && "qa_analyzed_at" %in% names(existing_qual) && "qa_status" %in% names(existing_qual)) {
        # If user explicitly chose to use stale data, accept any successful analysis
        # Otherwise, only accept analyses within the freshness window
        if (use_stale) {
          fresh_successful <- existing_qual %>%
            dplyr::filter(
              qa_status == "success",
              !is.na(qa_analyzed_at)
            )
        } else {
          fresh_successful <- existing_qual %>%
            dplyr::filter(
              qa_status == "success",
              !is.na(qa_analyzed_at),
              qa_analyzed_at >= fresh_cutoff
            )
        }

        if (nrow(fresh_successful) > 0) {
          # Get the most recent successful analysis
          latest_analysis <- fresh_successful %>%
            dplyr::arrange(dplyr::desc(qa_analyzed_at)) %>%
            dplyr::slice(1)

          has_fresh_qual <- TRUE
          qa_timestamp <- latest_analysis$qa_analyzed_at[[1]]
          qa_model <- if ("qa_model" %in% names(latest_analysis)) latest_analysis$qa_model[[1]] else "unknown"
          age_text <- if (use_stale) " (using stale cache)" else ""
          message("  Using cached qualitative analysis for lesson ", lid, " (",
                  round(as.numeric(difftime(now_ts, qa_timestamp, units = "days")), 1), " days old, model: ", qa_model, ")", age_text)
        } else {
          # Check if there are any analyses at all (even failed ones)
          if (nrow(existing_qual) > 0) {
            message("  Found ", nrow(existing_qual), " existing analysis/analyses for lesson ", lid, " but none are successful - running new analysis")
          }
        }
      } else if (force_fresh) {
        message("  Force fresh qualitative analysis requested for lesson ", lid, " - skipping cache")
      }

      if (!has_fresh_qual) {
        # Fetch content if we haven't already
        if (is.null(one_df)) {
          ensure_db_proxy()
          conn <- tryCatch({
            DBI::dbConnect(
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
          on.exit(DBI::dbDisconnect(conn), add = TRUE)
          q <- paste0(
            "SELECT CAST(l.id AS CHAR) AS lesson_id, d.content AS content ",
            "FROM lessons l JOIN docs d ON l.doc_id = d.id ",
            "WHERE l.id = ", DBI::dbQuoteString(conn, lid), ";"
          )
          one_df <- DBI::dbGetQuery(conn, q) %>% tibble::as_tibble()
        }

        lesson_text <- tryCatch({
          get_content_text(one_df$content[[1]])$content_text %||% ""
        }, error = function(e) {
          message("ERROR extracting lesson text: ", e$message)
          ""
        })

        if (nchar(lesson_text) >= 100) {
          # Get selected model from state (captured at button click time)
          selected_model <- .state$selected_model
          if (is.null(selected_model) || selected_model == "") {
            selected_model <- "gpt-5.1"
          }

          # Try to analyze, but continue on error
          qres <- tryCatch({
          analyze_lesson_qualitative(
            lesson_text = lesson_text,
            lesson_id = lid,
            lesson_title = lesson_title,
            model = selected_model
          )
        }, error = function(e) {
          # Return error result
          list(
            lesson_id = lid,
            lesson_title = lesson_title,
            model = selected_model,
            timestamp = Sys.time(),
            elapsed_time = 0,
            tokens = list(prompt = 0, completion = 0, total = 0),
            analysis = NULL,
            validation_passed = FALSE,
            iterations = 0,
            error = as.character(e$message),
            status = "failed"
          )
        })

        # Save result to JSON file (even if failed)
        tryCatch({
          save_analysis_result(qres, output_dir = "qualitative_analysis_results")
        }, error = function(e) {
          warning("Failed to save qualitative analysis result: ", e$message)
        })

        qjson <- jsonlite::toJSON(qres, auto_unbox = TRUE, null = "null")
        scores_df <- extract_qa_scores(qjson)

        # Add status column
        status <- if (!is.null(qres$error)) "failed" else if (isTRUE(qres$validation_passed)) "success" else "validation_failed"

        # Store qualitative results with timestamp and model
        # Keep ALL historical analyses for longitudinal studies and model comparisons
        qual_row <- tibble::tibble(
          lesson_id = lid,
          qualitative_json = as.character(qjson),
          qa_status = status,
          qa_error = qres$error %||% NA_character_,
          qa_analyzed_at = now_ts,
          qa_model = selected_model
        ) %>%
          dplyr::bind_cols(scores_df)

        # Add new result WITHOUT removing old ones - preserve all historical analyses
        .state$qual_results <- dplyr::bind_rows(
          .state$qual_results,
          qual_row
        )

        # Remove any duplicate qa_model columns (can happen after schema changes)
        col_names <- names(.state$qual_results)
        if (any(grepl("^qa_model\\.\\.\\.\\d+$", col_names))) {
          # Keep only the first qa_model column
          qa_model_cols <- grepl("^qa_model", col_names)
          first_qa_model_idx <- which(qa_model_cols)[1]
          cols_to_keep <- !qa_model_cols | seq_along(col_names) == first_qa_model_idx
          .state$qual_results <- .state$qual_results[, cols_to_keep]
        }

        # Save qualitative cache to disk
        saveRDS(.state$qual_results, qualitative_cache_file)
        } else {
          message("DEBUG: Lesson text too short (", nchar(lesson_text), " chars) - skipping qualitative analysis for lesson ", lid)
        }
      }
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
        cc_stacks_total = sum(cc_stacks_total, na.rm = TRUE),
        cc_stacks_single = sum(cc_stacks_single, na.rm = TRUE),
        cc_stacks_multi = sum(cc_stacks_multi, na.rm = TRUE),
        cc_questions_total = sum(cc_questions_total, na.rm = TRUE),
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

    # Join qualitative results (lesson-level data)
    # Get only the LATEST analysis per lesson for display
    # (All historical analyses are preserved in .state$qual_results for data warehouse export)
    if (nrow(.state$qual_results) > 0) {
      latest_qual <- .state$qual_results %>%
        dplyr::group_by(lesson_id) %>%
        dplyr::arrange(dplyr::desc(qa_analyzed_at)) %>%
        dplyr::slice(1) %>%
        dplyr::ungroup()

      les_acc <- les_acc %>%
        dplyr::left_join(latest_qual, by = "lesson_id")
    }
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

  # Helper function to start analysis processing
  start_analysis_processing <- function(ids, selected_model, force_fresh = FALSE, use_stale = FALSE) {
    section_metrics_acc(tibble::tibble())
    lesson_metrics_acc(tibble::tibble())
    .state$selected_model <- if (!is.null(selected_model) && nchar(selected_model) > 0) selected_model else NULL
    .state$qualitative_enabled <- !is.null(.state$selected_model)
    .state$force_fresh_qualitative <- force_fresh
    .state$use_stale_qualitative <- use_stale
    .state$ids <- ids
    .state$total <- length(ids)
    .state$idx <- 0L
    .state$sec_acc <- tibble::tibble()
    .state$les_acc <- tibble::tibble()
    progress_info(list(done = 0L, total = .state$total))
    if (.state$total > 0L) later::later(process_next_lesson, delay = 0)
  }

  observeEvent(input$pullContent, {
    ids <- selected_lesson_ids()
    selected_model <- input$qualitativeModel

    # Check for stale analyses (quantitative and/or qualitative)
    now_ts <- Sys.time()
    stale_cutoff <- now_ts - as.difftime(QUALITATIVE_FRESHNESS_DAYS, units = "days")

    stale_quant_count <- 0
    stale_qual_count <- 0

    # Check quantitative cache (only for selected lessons)
    if (!is.null(.state$analysis_cache) && nrow(.state$analysis_cache) > 0) {
      selected_ids <- as.character(ids)

      # Get the most recent analysis timestamp for each selected lesson
      lesson_latest <- .state$analysis_cache %>%
        dplyr::filter(as.character(lesson_id) %in% selected_ids) %>%
        dplyr::group_by(lesson_id) %>%
        dplyr::summarise(
          latest_analyzed_at = max(analyzed_at, na.rm = TRUE),
          .groups = "drop"
        )

      # Count how many selected lessons have stale latest analyses
      if (nrow(lesson_latest) > 0) {
        stale_quant_count <- lesson_latest %>%
          dplyr::filter(
            !is.na(latest_analyzed_at),
            is.finite(latest_analyzed_at),
            latest_analyzed_at < stale_cutoff
          ) %>%
          nrow()
      }
    }

    # Check qualitative cache if qualitative analysis is enabled (only for selected lessons)
    if (!is.null(selected_model) && nchar(selected_model) > 0 && !is.null(.state$qual_results) && nrow(.state$qual_results) > 0) {
      selected_ids <- as.character(ids)

      # Qualitative results should have one row per lesson, but let's get the latest per lesson to be safe
      lesson_qual_latest <- .state$qual_results %>%
        dplyr::filter(as.character(lesson_id) %in% selected_ids) %>%
        dplyr::group_by(lesson_id) %>%
        dplyr::arrange(dplyr::desc(qa_analyzed_at)) %>%
        dplyr::slice(1) %>%
        dplyr::ungroup()

      # Count how many selected lessons have stale successful qualitative analyses
      if (nrow(lesson_qual_latest) > 0) {
        stale_qual_count <- lesson_qual_latest %>%
          dplyr::filter(
            qa_status == "success",
            !is.na(qa_analyzed_at),
            qa_analyzed_at < stale_cutoff
          ) %>%
          nrow()
      }
    }

    # If any stale data found, show unified modal
    if (stale_quant_count > 0 || stale_qual_count > 0) {
      threshold_text <- if (QUALITATIVE_FRESHNESS_DAYS >= 1) {
        paste0(round(QUALITATIVE_FRESHNESS_DAYS), " day(s)")
      } else {
        paste0(round(QUALITATIVE_FRESHNESS_DAYS * 24, 1), " hour(s)")
      }

      status_lines <- list()
      if (stale_quant_count > 0) {
        status_lines <- c(status_lines, list(tags$p(paste0(stale_quant_count, " lesson(s) have quantitative analyses that are over ", threshold_text, " old."))))
      }
      if (stale_qual_count > 0) {
        status_lines <- c(status_lines, list(tags$p(paste0(stale_qual_count, " lesson(s) have qualitative analyses that are over ", threshold_text, " old."))))
      }

      showModal(modalDialog(
        title = "Stale Cache Detected",
        tagList(
          status_lines,
          tags$p("Would you like to:"),
          tags$ul(
            tags$li(tags$strong("Use Existing:"), " Use the cached analyses (faster, no re-computation)"),
            tags$li(tags$strong("Run Fresh:"), " Re-analyze stale lessons (fresh analyses may be used for non-stale lessons)")
          )
        ),
        footer = tagList(
          actionButton("useStaleCache", "Use Existing", icon = icon("database")),
          actionButton("runFreshCache", "Run Fresh", icon = icon("sync"), class = "btn-primary"),
          modalButton("Cancel")
        ),
        easyClose = FALSE,
        size = "m"
      ))
      return()
    }

    # No stale data - proceed normally
    start_analysis_processing(ids, selected_model, force_fresh = FALSE)
  })

  # Handle user choosing to use stale cache
  observeEvent(input$useStaleCache, {
    removeModal()
    ids <- selected_lesson_ids()
    selected_model <- input$qualitativeModel
    start_analysis_processing(ids, selected_model, force_fresh = FALSE, use_stale = TRUE)
  })

  # Handle user choosing to run fresh analysis
  observeEvent(input$runFreshCache, {
    removeModal()
    ids <- selected_lesson_ids()
    selected_model <- input$qualitativeModel
    start_analysis_processing(ids, selected_model, force_fresh = TRUE, use_stale = FALSE)
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
  # Global selected member labels (nodes) shared across tabs (supports multi-select)
  selected_members <- reactiveVal(character(0))
  # Event signatures to avoid re-processing the same UI events after re-render
  ev_sigs <- reactiveValues(
    lesson_sel = "", lesson_clk = "",
    metric_sel = "", metric_clk = "",
    bench_sel  = "", bench_clk  = ""
  )

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
                              hovertemplate = "%{x}<extra></extra>",
                              hoverinfo = "x",
                              marker = list(color = "#D1C8F1", line = list(color = "black", width = 0.5)),
                              line = list(color = "black", width = 0.5)) %>%
        plotly::layout(yaxis = list(visible = FALSE), xaxis = list(range = c(0, xr[2])), showlegend = FALSE)
    } else {
      top <- plotly::plot_ly(x = ~df$value, type = "histogram", nbinsx = 20, selectedpoints = NA,
                              marker = list(color = "#D1C8F1", line = list(color = "black", width = 0.5))) %>%
        plotly::layout(yaxis = list(visible = FALSE), xaxis = list(range = c(0, xr[2])), showlegend = FALSE)
    }
    # Preserve label order
    df$sec_label <- factor(df$sec_label, levels = unique(df$sec_label))
    # Match group analysis border behavior: remove borders if many bars
    line_w_sec <- if (nrow(df) > 15) 0 else 0.5
    bars <- plotly::plot_ly(df, x = ~value, y = ~sec_label, type = "bar", orientation = "h",
                             marker = list(color = "#D1C8F1", line = list(color = "black", width = line_w_sec)),
                             hovertemplate = "%{y}: %{x}<extra></extra>") %>%
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

  observeEvent({
    list(
      plotly::event_data("plotly_click", source = "sectionBars"),
      plotly::event_data("plotly_selected", source = "sectionBars")
    )
  }, {
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
  }, ignoreNULL = TRUE, ignoreInit = TRUE)

  # ---- NEW: Lesson charts using shared scale + ggiraph ----
  lesson_plot_data <- reactive({
    # Use lesson_metrics_acc as the source of truth for lesson-level data
    # This includes both quantitative (summed from sections) and qualitative (lesson-level) metrics
    base <- lesson_metrics_acc()
    req(!is.null(base), nrow(base) > 0, input$lessonMetric)
    m <- input$lessonMetric
    # If the requested metric column is missing, return empty safely
    if (!(m %in% names(base))) {
      return(list(df = tibble::tibble(node_id = character(), node = character(), value = numeric(), grp = character()), metric_name = m))
    }
    level <- input$aggLevel %||% "lesson"
    grp_by <- input$groupBy %||% "none"
    base[[m]] <- suppressWarnings(as.numeric(base[[m]]))
    base <- base[is.finite(base[[m]]), , drop = FALSE]

    # Ensure lesson_id exists in base
    if (!"lesson_id" %in% names(base)) {
      return(list(df = tibble::tibble(node_id = character(), node = character(), value = numeric(), grp = character()), metric_name = m))
    }

    if (identical(level, "lesson")) {
      # At lesson level, base already has the correct aggregation
      df_small <- base %>%
        dplyr::select(lesson_id, value = dplyr::all_of(m)) %>%
        dplyr::left_join(content_tree %>% dplyr::select(lesson_id, lesson, course, module, chapter) %>% dplyr::distinct(), by = "lesson_id") %>%
        dplyr::transmute(node_id = lesson_id,
                         node = dplyr::coalesce(lesson, lesson_id),
                         grp = if (identical(grp_by, "none")) "All" else .data[[grp_by]],
                         value = as.numeric(value)) %>%
        dplyr::distinct(node, .keep_all = TRUE)
    } else {
      # For higher levels (chapter/module/course), aggregate from lesson level
      if (!(level %in% names(content_tree))) return(list(df = tibble::tibble(node = character(), value = numeric()), metric_name = m))

      # Determine if this is a QA metric (should use mean) or quantitative metric (should use sum)
      is_qa_metric <- grepl("^qa_", m)

      # Determine the ID column name based on aggregation level
      id_col <- paste0(level, "_id")

      df_small <- base %>%
        dplyr::select(lesson_id, value = dplyr::all_of(m)) %>%
        dplyr::left_join(content_tree %>% dplyr::select(lesson_id, course_id, course, module_id, module, chapter_id, chapter, lesson) %>% dplyr::distinct(), by = "lesson_id") %>%
        dplyr::group_by(node = .data[[level]]) %>%
        dplyr::summarise(value = if (is_qa_metric) mean(value, na.rm = TRUE) else sum(value, na.rm = TRUE),
                         grp = if (identical(grp_by, "none")) "All" else dplyr::first(.data[[grp_by]]),
                         node_id = dplyr::first(.data[[id_col]]),
                         .groups = "drop") %>%
        dplyr::distinct(node, .keep_all = TRUE) %>%
        dplyr::mutate(value = as.numeric(value), grp = as.character(grp)) %>%
        dplyr::relocate(node_id, .before = node)
    }
    df_small <- df_small %>% dplyr::filter(is.finite(value))
    list(df = df_small, metric_name = m)
  })

  lesson_shared_x <- reactive({
    pd <- lesson_plot_data()
    compute_shared_x(datasets = list(pd$df, pd$df), x_cols = c("value","value"), n_breaks = 6)
  })

  lesson_highlights <- reactive({
    # Prefer global selected members, fallback to table selection
    labs <- selected_members()
    if (length(labs) > 0) return(labs)
    pd <- lesson_plot_data()
    sel <- input$lessonMetricsTable_rows_selected
    if (!is.null(sel) && length(sel) >= 1) {
      sel <- sel[sel >= 1 & sel <= nrow(pd$df)]
      if (length(sel)) return(pd$df$node[sel])
    }
    character(0)
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
    groups <- if ("grp" %in% names(df)) unique(as.character(df$grp)) else "All"

    # Order groups by their position in the content tree (reversed for bottom-to-top display)
    grp_by <- input$groupBy %||% "none"
    if (grp_by != "none" && grp_by %in% names(content_tree)) {
      tree_group_order <- unique(content_tree[[grp_by]])
      groups <- tree_group_order[tree_group_order %in% groups]
      # Add any groups not in tree (shouldn't happen, but fallback)
      groups <- c(groups, setdiff(unique(as.character(df$grp)), groups))
      # Reverse so first items appear at bottom (natural reading order)
      groups <- rev(groups)
    }

    pal_vec <- grDevices::colorRampPalette(RColorBrewer::brewer.pal(8, "Set2"))(max(1, length(groups)))
    pal <- stats::setNames(pal_vec, groups)
    if (identical(dist_type, "box")) {
      top <- plotly::plot_ly()
      for (g in groups) {
        sub <- df[df$grp %in% g | (!"grp" %in% names(df)), , drop = FALSE]
        # Place each group's box on its own y category so the label appears on the left
        top <- top %>% plotly::add_trace(x = sub$value, y = rep(g, nrow(sub)), type = "box", boxpoints = "outliers",
                                         orientation = "h", name = g, fillcolor = unname(pal[g]),
                                         key = sub$node,
                                         marker = list(color = unname(pal[g]), line = list(color = "black", width = 0.5)),
                                         line = list(color = "black", width = 0.5))
      }
      top <- top %>% plotly::layout(
        yaxis = list(title = "", categoryorder = "array", categoryarray = groups, automargin = TRUE, showgrid = FALSE),
        xaxis = list(range = c(0, xr[2])),
        showlegend = TRUE
      )
    } else {
      # Create binned histogram data for selection
      nbins <- 20
      bin_width <- (xr[2] - 0) / nbins

      # Get selected labels for highlighting bins
      sel_labels <- lesson_highlights()

      top <- plotly::plot_ly()
      for (g in groups) {
        sub <- df[df$grp %in% g | (!"grp" %in% names(df)), , drop = FALSE]

        if (nrow(sub) > 0) {
          # Create bins and assign each value to a bin
          sub$bin <- cut(sub$value, breaks = seq(0, xr[2], by = bin_width),
                        include.lowest = TRUE, right = FALSE)

          # Count items per bin and collect node IDs for each bin
          bin_counts <- sub %>%
            dplyr::group_by(bin) %>%
            dplyr::summarise(
              count = dplyr::n(),
              bin_start = min(as.numeric(gsub("\\[|\\(|,.*", "", as.character(bin)))),
              nodes = list(as.character(node)),
              .groups = "drop"
            )

          # Create unique bin IDs for each bin (group_binindex)
          bin_counts$bin_id <- paste0(g, "_bin", seq_len(nrow(bin_counts)))

          # Determine color for each bin - black if any nodes in the bin are selected
          bin_colors <- sapply(bin_counts$nodes, function(nodes_in_bin) {
            if (length(sel_labels) > 0 && any(nodes_in_bin %in% sel_labels)) {
              "black"
            } else {
              unname(pal[g])
            }
          })

          # Create bar trace with all nodes in the bin as the key (for selection)
          top <- top %>% plotly::add_trace(
            x = bin_counts$bin_start + bin_width/2,
            y = bin_counts$count,
            type = "bar",
            width = bin_width * 0.9,
            name = g,
            key = bin_counts$bin_id,
            marker = list(color = bin_colors, line = list(color = "black", width = 0.5)),
            opacity = 0.6,
            customdata = bin_counts$nodes,
            hovertemplate = paste0(g, "<br>Count: %{y}<extra></extra>"),
            showlegend = TRUE
          )
        }
      }
      top <- top %>% plotly::layout(
        yaxis = list(title = "Count"),
        xaxis = list(range = c(0, xr[2])),
        barmode = "stack",
        showlegend = TRUE
      )
    }
    # Order bars by node order in the tree for the selected aggregation level
    lvl_for_order <- input$aggLevel %||% "lesson"
    tree_order <- if (lvl_for_order %in% names(content_tree)) {
      unique(content_tree[[lvl_for_order]])
    } else {
      unique(as.character(df$node))
    }
    # Keep only nodes present in df, in tree order
    level_nodes <- tree_order[tree_order %in% as.character(df$node)]
    # Fallback to current order if empty for any reason
    if (!length(level_nodes)) level_nodes <- unique(as.character(df$node))
    # Reverse the order so bars appear bottom-to-top in natural reading order
    level_nodes <- rev(level_nodes)
    df$node <- factor(df$node, levels = level_nodes)
    bar_colors <- if ("grp" %in% names(df)) pal[as.character(df$grp)] else rep("#D1C8F1", nrow(df))
    bar_colors[is.na(bar_colors)] <- "#D1C8F1"
    # Highlight selected bars in black (supports multi-select)
    sel_labels <- lesson_highlights()
    if (length(sel_labels) >= 1) {
      hits <- which(as.character(df$node) %in% sel_labels)
      if (length(hits) >= 1) bar_colors[hits] <- "black"
    }
    line_w <- if (nrow(df) > 15) 0 else 0.5
    bars <- plotly::plot_ly(df, x = ~value, y = ~node, key = ~as.character(node), type = "bar", orientation = "h",
                             marker = list(color = bar_colors, line = list(color = "black", width = line_w)),
                             hovertemplate = "%{y}: %{x}<extra></extra>") %>%
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
    plt <- plt %>% plotly::event_register('plotly_selected') %>% plotly::event_register('plotly_click') %>%
      plotly::event_register('plotly_doubleclick') %>% plotly::event_register('plotly_deselect')
    plt$x$source <- 'lessonBars'
    plt
  })

  # Populate benchmark metrics choices
  observe({
    df <- tryCatch(agg_metrics(), error = function(e) NULL)
    # Default to full list if data not yet available
    ok <- if (!is.null(df) && nrow(df) > 0) intersect(numeric_metric_names, names(df)) else numeric_metric_names
    if (length(ok) == 0) ok <- numeric_metric_names
    current <- isolate(input$benchmarkMetrics)
    sel <- if (!is.null(current) && length(current) >= 1) intersect(current, ok) else head(ok, 1)
    shinyWidgets::updatePickerInput(session, "benchmarkMetrics", choices = ok, selected = sel)
  })

  # Helper to compute group df for an arbitrary metric at current aggLevel
  get_group_values <- function(m) {
    # Use lesson_metrics_acc as the source of truth (includes both quantitative and qualitative metrics)
    base <- lesson_metrics_acc()
    if (is.null(base) || nrow(base) == 0 || !m %in% names(base)) {
      return(tibble::tibble(node = character(), value = numeric(), grp = character()))
    }
    lvl <- input$aggLevel %||% "lesson"
    grp_by <- input$groupBy %||% "none"

    # Convert to numeric and filter finite values
    base[[m]] <- suppressWarnings(as.numeric(base[[m]]))
    base <- base[is.finite(base[[m]]), , drop = FALSE]

    if (identical(lvl, "lesson")) {
      # At lesson level, base already has the correct aggregation
      out <- base %>%
        dplyr::select(lesson_id, value = dplyr::all_of(m)) %>%
        dplyr::left_join(content_tree %>% dplyr::select(lesson_id, lesson, course, module, chapter) %>% dplyr::distinct(), by = "lesson_id") %>%
        dplyr::transmute(node = dplyr::coalesce(lesson, lesson_id),
                         value = as.numeric(value),
                         grp = if (identical(grp_by, "none")) "All" else .data[[grp_by]]) %>%
        dplyr::distinct(node, .keep_all = TRUE)
    } else if (lvl %in% names(content_tree)) {
      # For higher levels, determine if QA metric (use mean) or quantitative (use sum)
      is_qa_metric <- grepl("^qa_", m)

      out <- base %>%
        dplyr::select(lesson_id, value = dplyr::all_of(m)) %>%
        dplyr::left_join(content_tree %>% dplyr::select(lesson_id, course, module, chapter, lesson) %>% dplyr::distinct(), by = "lesson_id") %>%
        dplyr::group_by(node = .data[[lvl]]) %>%
        dplyr::summarise(value = if (is_qa_metric) mean(value, na.rm = TRUE) else sum(value, na.rm = TRUE),
                         grp = if (identical(grp_by, "none")) "All" else first(.data[[grp_by]]),
                         .groups = "drop") %>%
        dplyr::distinct(node, .keep_all = TRUE) %>%
        dplyr::mutate(value = as.numeric(value), grp = as.character(grp))
    } else {
      out <- tibble::tibble(node = character(), value = numeric(), grp = character())
    }
    out
  }

  # Helper: build number line per rules for a metric
  build_benchmark_plot <- function(df, metric_name) {
    # Use global selected members if present; otherwise derive from events/table
    sel_labs <- selected_members()
    sel_labels <- character(0)
    if (length(sel_labs) > 0) {
      sel_labels <- sel_labs
    } else {
      sel_ev  <- safe_event_data('plotly_selected', 'lessonBars')
      click_ev <- safe_event_data('plotly_click', 'lessonBars')
      if (!is.null(sel_ev) && NROW(sel_ev)) {
        sel_labels <- unique(as.character(sel_ev$y))
      } else if (!is.null(click_ev)) {
        # Check if this is a histogram bin click (has customdata with list of nodes)
        if (!is.null(click_ev$customdata) && length(click_ev$customdata) > 0) {
          # Histogram bin click - select all nodes in the bin
          sel_labels <- unlist(click_ev$customdata[[1]])
        } else if (!is.null(click_ev$y)) {
          # Regular bar click
          sel_labels <- as.character(click_ev$y[1])
        }
      } else {
        row <- input$lessonMetricsTable_rows_selected
        if (!is.null(row) && length(row) == 1 && row >= 1 && row <= nrow(df)) sel_labels <- df$node[row]
      }
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

    # Determine selected group color (based on Group By and palette)
    # Build palette for groups; sanitize NAs/empties
    if ("grp" %in% names(df)) {
      df$grp <- as.character(df$grp)
      df$grp[is.na(df$grp) | df$grp == ""] <- "(NA)"
      groups <- unique(df$grp)
    } else {
      groups <- "All"
    }
    if (length(groups) == 0) groups <- "All"
    pal_vec <- grDevices::colorRampPalette(RColorBrewer::brewer.pal(8, "Set2"))(max(1, length(groups)))
    pal <- stats::setNames(pal_vec, groups)
    # Sanitize group labels for palette/selection
    if ("grp" %in% names(df)) {
      df$grp <- as.character(df$grp)
      df$grp[is.na(df$grp) | df$grp == ""] <- "(NA)"
    }
    selected_grp <- if (length(sel_labels) >= 1 && "grp" %in% names(df)) {
      g <- df$grp[match(sel_labels[1], df$node)]
      if (is.na(g) || length(g) == 0) "(NA)" else as.character(g)
    } else {
      "(NA)"
    }
    sc_try <- unname(pal[selected_grp])
    sel_color <- if (length(sc_try) == 0 || is.na(sc_try)) "#D1C8F1" else sc_try

    p <- plotly::plot_ly()
    # Rules
    if (n < 10) {
      # show all dots + median line (+ optional mean)
      if (nrow(peers_df)) {
        p <- p %>% plotly::add_trace(data = peers_df, x = ~value, y = ~y, type = 'scatter', mode = 'markers',
                                     key = ~node,
                                     marker = list(color = '#D1C8F1', size = 10, line = list(color = 'black', width = 0.2)),
                                     hoverinfo = 'text',
                                     text = ~paste0(peers_df$node, ': ', signif(peers_df$value, 5)))
      }
      if (nrow(selected_df)) {
        p <- p %>% plotly::add_trace(data = selected_df, x = ~value, y = ~y, type = 'scatter', mode = 'markers',
                                     key = ~node,
                                     marker = list(color = 'black', size = 12, line = list(color = 'black', width = 0.6)),
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
          list(type = 'rect', x0 = q[1], x1 = q[3], y0 = -0.15, y1 = 0.15, line = list(color = 'black', width = 1), fillcolor = sel_color, opacity = 0.5),
          list(type = 'line', x0 = q[2], x1 = q[2], y0 = -0.2, y1 = 0.2, line = list(color = 'black', width = 1.5))
        ))
      }
      tails <- peers_df %>% dplyr::filter((is.finite(q[1]) & value < q[1]) | (is.finite(q[3]) & value > q[3]))
      if (nrow(tails)) {
        p <- p %>% plotly::add_trace(data = tails, x = ~value, y = ~y, type = 'scatter', mode = 'markers',
                                     key = ~node,
                                     marker = list(color = '#D1C8F1', size = 10, line = list(color = 'black', width = 0.2)),
                                     hoverinfo = 'text',
                                     text = ~paste0(tails$node, ': ', signif(tails$value, 5)))
      }
      if (nrow(selected_df)) {
        p <- p %>% plotly::add_trace(data = selected_df, x = ~value, y = ~y, type = 'scatter', mode = 'markers',
                                     key = ~node,
                                     marker = list(color = 'black', size = 12, line = list(color = 'black', width = 0.6)),
                                     hoverinfo = 'text',
                                     text = ~paste0(selected_df$node, ' (selected): ', signif(selected_df$value, 5)))
      }
      p <- p %>% plotly::layout(shapes = shapes)
    } else {
      # n >= 20 : box + whiskers with outliers
      p <- p %>% plotly::add_trace(x = ~vals, y = ~rep(0, length(vals)), type = 'box', orientation = 'h',
                                   boxpoints = 'outliers', hoverinfo = 'skip', showlegend = FALSE,
                                   fillcolor = sel_color,
                                   marker = list(color = sel_color, line = list(color = "black", width = 0.6)),
                                   line = list(color = "black", width = 1))
      if (nrow(selected_df)) {
        p <- p %>% plotly::add_trace(data = selected_df, x = ~value, y = ~y, type = 'scatter', mode = 'markers',
                                     key = ~node,
                                     marker = list(color = 'black', size = 12, line = list(color = 'black', width = 0.6)),
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
          p <- build_benchmark_plot(df, metric) %>%
            plotly::event_register('plotly_click') %>%
            plotly::event_register('plotly_selected') %>%
            plotly::event_register('plotly_doubleclick') %>%
            plotly::event_register('plotly_deselect')
          p$x$source <- 'benchmark'
          p %>%
            plotly::layout(dragmode = "select") %>%
            plotly::config(
              displaylogo = FALSE,
              scrollZoom = FALSE,
              modeBarButtonsToRemove = c(
                'zoom2d','pan2d','lasso2d','zoomIn2d','zoomOut2d',
                'hoverClosestCartesian','hoverCompareCartesian','toggleSpikelines'
              )
            )
        })
      })
    }
  })

  # ---- Metric analysis: scatterplot matrix for selected metrics ----
  observe({
    df <- tryCatch(agg_metrics(), error = function(e) NULL)
    ok <- if (!is.null(df) && nrow(df) > 0) intersect(numeric_metric_names, names(df)) else numeric_metric_names
    if (length(ok) == 0) ok <- numeric_metric_names
    # Preselect up to 4 metrics if none selected
    current <- isolate(input$metricMatrixMetrics)
    default_sel <- head(ok, 4)
    sel <- if (!is.null(current) && length(current)) intersect(current, ok) else default_sel
    shinyWidgets::updatePickerInput(session, "metricMatrixMetrics", choices = ok, selected = sel)
  })

  output$metricMatrixPlot <- plotly::renderPlotly({
    mets <- input$metricMatrixMetrics
    # Prevent duplicate metrics which create same x/y and duplicate tooltip lines
    if (!is.null(mets) && length(mets)) {
      mets <- unique(mets)
    }
    req(!is.null(mets), length(mets) >= 2)
    # Build wide data of selected metrics for current level
    dfs <- lapply(mets, function(m) {
      d <- get_group_values(m)
      d <- dplyr::select(d, node, value) %>% dplyr::distinct(node, .keep_all = TRUE)
      names(d) <- c("node", m)
      d
    })
    wide <- Reduce(function(a, b) dplyr::full_join(a, b, by = "node"), dfs)
    # Determine selection
    sel_labs <- selected_members()
    sel_labels <- character(0)
    if (length(sel_labs) > 0) {
      sel_labels <- sel_labs
    } else {
      sel_ev  <- safe_event_data('plotly_selected', 'lessonBars')
      click_ev <- safe_event_data('plotly_click', 'lessonBars')
      if (!is.null(sel_ev) && NROW(sel_ev)) {
        sel_labels <- unique(as.character(sel_ev$y))
      } else if (!is.null(click_ev) && !is.null(click_ev$y)) {
        sel_labels <- as.character(click_ev$y[1])
      } else {
        row <- input$lessonMetricsTable_rows_selected
        if (!is.null(row) && length(row) == 1 && row >= 1 && row <= nrow(wide)) sel_labels <- wide$node[row]
      }
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
            hovertemplate_str <- paste0("%{text}<br>", xmet, ": %{x}<br>", ymet, ": %{y}<extra></extra>")
            p <- p %>% plotly::add_trace(
              data = peers,
              x = peers[[xmet]], y = peers[[ymet]],
              type = 'scatter', mode = 'markers',
              marker = list(color = '#D1C8F1', size = 7, line = list(color = 'black', width = 0.2)),
              key = peers$node,
              text = peers$node,
              hovertemplate = hovertemplate_str
            )
          }
          if (nrow(sels)) {
            hovertemplate_str_sel <- paste0("%{text}<br>", xmet, ": %{x}<br>", ymet, ": %{y}<extra></extra>")
            p <- p %>% plotly::add_trace(
              data = sels,
              x = sels[[xmet]], y = sels[[ymet]],
              type = 'scatter', mode = 'markers',
              marker = list(color = 'black', size = 9, line = list(color = 'black', width = 0.6)),
              key = sels$node,
              text = paste0(sels$node, " (selected)"),
              hovertemplate = hovertemplate_str_sel
            )
          }
          p <- p %>% plotly::layout(xaxis = list(title = if (i == k) xmet else "", showgrid = FALSE),
                                    yaxis = list(title = if (j == 1) ymet else "", showgrid = FALSE))
        }
        plots[[length(plots) + 1L]] <- p
      }
    }
    h <- 220 * k
    subplot <- plotly::subplot(plots, nrows = k, shareX = FALSE, shareY = FALSE, margin = 0.02) %>%
      plotly::layout(margin = list(l = 60, r = 20, t = 10, b = 40), showlegend = FALSE) %>%
      plotly::event_register('plotly_click') %>%
      plotly::event_register('plotly_selected') %>%
      plotly::event_register('plotly_doubleclick') %>%
      plotly::event_register('plotly_deselect')
    subplot$x$source <- 'metricMatrix'
    subplot$sizingPolicy$defaultHeight <- h
    # Match interaction/config with other graphs: selection-enabled, minimal toolbar
    subplot <- subplot %>%
      plotly::layout(dragmode = "select") %>%
      plotly::config(
        displaylogo = FALSE,
        scrollZoom = FALSE,
        modeBarButtonsToRemove = c(
          'zoom2d','pan2d','lasso2d','zoomIn2d','zoomOut2d',
          'hoverClosestCartesian','hoverCompareCartesian','toggleSpikelines'
        )
      )
    subplot
  })

  # Clicking/Selecting in the metric matrix updates global selection
  observe({
    ev <- safe_event_data('plotly_click', 'metricMatrix')
    if (is.null(ev) || is.null(ev$key)) return()
    lab <- as.character(ev$key)[1]
    if (!is.null(lab) && nchar(lab)) {
      sig <- paste0("MC|", lab)
      if (!identical(sig, ev_sigs$metric_clk)) {
        ev_sigs$metric_clk <- sig
        cur <- isolate(selected_members())
        if (lab %in% cur) cur <- setdiff(cur, lab) else cur <- sort(unique(c(cur, lab)))
        selected_members(cur)
        lesson_filter(list(type = "labels", values = cur))
      }
    }
  })
  observe({
    sel <- safe_event_data('plotly_selected', 'metricMatrix')
    if (is.null(sel) || !NROW(sel) || is.null(sel$key)) return()
    labs <- unique(as.character(sel$key))
    sig <- paste0("MS|", paste(sort(labs), collapse = "|"))
    if (!identical(sig, ev_sigs$metric_sel)) {
      ev_sigs$metric_sel <- sig
      cur <- isolate(selected_members())
      new_sel <- sort(unique(c(cur, labs)))
      if (!setequal(new_sel, cur)) {
        selected_members(new_sel)
        lesson_filter(list(type = "labels", values = new_sel))
      }
    }
  })

  # Deselect on background click/doubleclick in Group analysis
  observe({
    ev <- safe_event_data('plotly_doubleclick', 'lessonBars')
    de <- safe_event_data('plotly_deselect', 'lessonBars')
    if (!is.null(ev) || !is.null(de)) {
      lesson_filter(NULL)
      selected_members(character(0))
      selectRows(lesson_proxy, integer(0))
    }
  })

  # Deselect on background click/doubleclick in Metric analysis
  observe({
    ev <- safe_event_data('plotly_doubleclick', 'metricMatrix')
    de <- safe_event_data('plotly_deselect', 'metricMatrix')
    if (!is.null(ev) || !is.null(de)) {
      lesson_filter(NULL)
      selected_members(character(0))
      selectRows(lesson_proxy, integer(0))
    }
  })

  # Benchmark interactions: toggle on click, union on box-select, clear on deselect/dblclick
  observe({
    clk <- safe_event_data('plotly_click', 'benchmark')
    if (is.null(clk) || is.null(clk$key)) return()
    lab <- as.character(clk$key)[1]
    if (!is.null(lab) && nchar(lab)) {
      sig <- paste0("BC|", lab)
      if (!identical(sig, ev_sigs$bench_clk)) {
        ev_sigs$bench_clk <- sig
        cur <- isolate(selected_members())
        if (lab %in% cur) cur <- setdiff(cur, lab) else cur <- sort(unique(c(cur, lab)))
        selected_members(cur)
        lesson_filter(list(type = "labels", values = cur))
      }
    }
  })
  observe({
    sel <- safe_event_data('plotly_selected', 'benchmark')
    if (is.null(sel) || !NROW(sel) || is.null(sel$key)) return()
    labs <- unique(as.character(sel$key))
    sig <- paste0("BS|", paste(sort(labs), collapse = "|"))
    if (!identical(sig, ev_sigs$bench_sel)) {
      ev_sigs$bench_sel <- sig
      cur <- isolate(selected_members())
      new_sel <- sort(unique(c(cur, labs)))
      if (!setequal(new_sel, cur)) {
        selected_members(new_sel)
        lesson_filter(list(type = "labels", values = new_sel))
      }
    }
  })
  observe({
    dc <- safe_event_data('plotly_doubleclick', 'benchmark')
    de <- safe_event_data('plotly_deselect', 'benchmark')
    if (!is.null(dc) || !is.null(de)) {
      lesson_filter(NULL)
      selected_members(character(0))
      selectRows(lesson_proxy, integer(0))
    }
  })
  observe({
    ev <- safe_event_data("plotly_click", "lessonBars")
    sel <- safe_event_data("plotly_selected", "lessonBars")
    if (!is.null(sel) && NROW(sel)) {
      labs <- NULL
      # Check if this is histogram bin selection (has customdata with list of nodes)
      if (!is.null(sel$customdata) && length(sel$customdata) > 0) {
        # Extract all nodes from all selected bins
        # customdata can be a list of lists, flatten all node vectors
        all_nodes <- lapply(seq_len(length(sel$customdata)), function(i) {
          cd <- sel$customdata[[i]]
          if (is.list(cd)) {
            # cd is a list containing the nodes vector
            unlist(cd)
          } else if (is.character(cd)) {
            # cd is already a character vector
            cd
          } else {
            character(0)
          }
        })
        labs <- unique(unlist(all_nodes))
      } else if (!is.null(sel$key)) {
        labs <- unique(as.character(sel$key))
      } else if (!is.null(sel$y)) {
        labs <- unique(as.character(sel$y))
      }

      if (!is.null(labs) && length(labs) > 0) {
        sig <- paste0("S|", paste(sort(labs), collapse = "|"))
        if (!identical(sig, ev_sigs$lesson_sel)) {
          ev_sigs$lesson_sel <- sig
          # For box selection, replace the selection (don't union with current)
          new_sel <- sort(unique(labs))
          if (!setequal(new_sel, selected_members())) {
            selected_members(new_sel)
            lesson_filter(list(type = "labels", values = new_sel))
          }
        }
      }
      return()
    }
    if (!is.null(ev)) {
      labs <- NULL
      # Check if this is a histogram bin click (has customdata with list of nodes)
      if (!is.null(ev$customdata) && length(ev$customdata) > 0) {
        # Histogram bin click - select all nodes in the bin
        labs <- unlist(ev$customdata[[1]])
      } else if (!is.null(ev$key)) {
        labs <- as.character(ev$key)[1]
      } else if (!is.null(ev$y)) {
        labs <- as.character(ev$y)[1]
      }
      if (!is.null(labs) && length(labs) > 0) {
        sig <- paste0("C|", paste(sort(labs), collapse = "|"))
        if (!identical(sig, ev_sigs$lesson_clk)) {
          ev_sigs$lesson_clk <- sig
          cur <- isolate(selected_members())
          # For bin clicks (multiple labels), replace selection
          # For single bar clicks, toggle selection
          if (length(labs) > 1) {
            # Bin click - replace selection with all nodes in bin
            new_sel <- sort(unique(labs))
          } else {
            # Single bar click - toggle
            if (labs %in% cur) {
              new_sel <- setdiff(cur, labs)
            } else {
              new_sel <- sort(unique(c(cur, labs)))
            }
          }
          if (!setequal(new_sel, cur)) {
            selected_members(new_sel)
            lesson_filter(list(type = "labels", values = new_sel))
          }
        }
      }
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
      "latex_display","latex_display_multi","nodes_native","nodes_custom",
      "cc_stacks_total","cc_stacks_single","cc_stacks_multi","cc_questions_total"
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
    # Only trust accumulator if it already contains the latest metric columns (incl. concept-checks and QA)
    required_cols <- c("headings_h1","headings_h2","headings_h3","headings_h6","nodes_native","nodes_custom",
                       "cc_stacks_total","cc_stacks_single","cc_stacks_multi","cc_questions_total")
    # Note: QA columns are optional (only present if qualitative analysis was run)
    if (!is.null(acc) && nrow(acc) > 0 && all(required_cols %in% names(acc))) return(acc)
    df <- section_metrics_refactored()
    if (is.null(df) || nrow(df) == 0) return(tibble::tibble())
    # Ensure heading-by-level columns exist for new metric support even with older cached rows
    ensure_cols <- c("headings_h1","headings_h2","headings_h3","headings_h6","nodes_native","nodes_custom",
                     "cc_stacks_total","cc_stacks_single","cc_stacks_multi","cc_questions_total")
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
        cc_stacks_total = sum(cc_stacks_total, na.rm = TRUE),
        cc_stacks_single = sum(cc_stacks_single, na.rm = TRUE),
        cc_stacks_multi = sum(cc_stacks_multi, na.rm = TRUE),
        cc_questions_total = sum(cc_questions_total, na.rm = TRUE),
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
    # Use plot data as the source - it's already filtered and aggregated correctly
    pd <- tryCatch(lesson_plot_data(), error = function(e) NULL)
    if (is.null(pd) || is.null(pd$df) || !is.data.frame(pd$df)) {
      return(DT::datatable(data.frame()))
    }

    # Build display table showing the selected metric
    tbl <- pd$df %>%
      dplyr::transmute(
        id = as.integer(.data$node_id %||% as.character(.data$node)),
        node = as.character(.data$node),
        group = .data$grp %||% "All",
        metric = isolate(input$lessonMetric) %||% pd$metric_name,
        value = .data$value
      )

    # Filter table to selected members only (if any are selected)
    sel_members <- selected_members()
    if (length(sel_members) > 0) {
      tbl <- tbl %>% dplyr::filter(node %in% sel_members)
    }

    DT::datatable(
      tbl,
      rownames = FALSE,
      options = list(pageLength = 10, scrollX = TRUE),
      selection = 'single'
    ) %>%
      DT::formatRound(columns = "value", digits = 2)
  })

  # Download handler for group analysis table
  output$downloadGroupAnalysis <- downloadHandler(
    filename = function() {
      metric_name <- input$lessonMetric %||% "metric"
      agg_level <- input$aggLevel %||% "level"
      timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
      paste0("group_analysis_", metric_name, "_", agg_level, "_", timestamp, ".csv")
    },
    content = function(file) {
      pd <- tryCatch(lesson_plot_data(), error = function(e) NULL)
      if (is.null(pd) || is.null(pd$df) || !is.data.frame(pd$df)) {
        write.csv(data.frame(), file, row.names = FALSE)
        return()
      }

      # Build the same table structure as displayed
      tbl <- pd$df %>%
        dplyr::transmute(
          id = as.integer(.data$node_id %||% as.character(.data$node)),
          node = as.character(.data$node),
          group = .data$grp %||% "All",
          metric = isolate(input$lessonMetric) %||% pd$metric_name,
          value = .data$value
        )

      # Filter table to selected members only (if any are selected)
      sel_members <- selected_members()
      if (length(sel_members) > 0) {
        tbl <- tbl %>% dplyr::filter(node %in% sel_members)
      }

      write.csv(tbl, file, row.names = FALSE)
    }
  )

  # Metric selection and distribution plot for Lesson Analysis
  numeric_metric_names <- c(
    "words_total","words_nomath","chars_total","sentences_nomath",
    "blocks_total","words_sum","headings_count","headings_h1","headings_h2","headings_h3","headings_h6","images_count",
    "tables_count","lists_count","latex_total","latex_inline",
    "latex_display","latex_display_multi","nodes_native","nodes_custom",
    "cc_stacks_total","cc_stacks_single","cc_stacks_multi","cc_questions_total",
    "qa_template_fit","qa_template_structure_compliance","qa_cognitive_integrity",
    "qa_template_boundary_discipline","qa_progressive_model_principle",
    "qa_lesson_economy_and_cognitive_load","qa_priority_index"
  )

  observe({
    # Populate metric choices robustly; don't depend on data being present
    df <- tryCatch(agg_metrics(), error = function(e) NULL)
    ok <- if (!is.null(df)) intersect(numeric_metric_names, names(df)) else numeric_metric_names
    if (length(ok) == 0) ok <- numeric_metric_names
    current <- isolate(input$lessonMetric)
    sel <- if (!is.null(current) && current %in% ok) current else ok[[1]]
    updateSelectInput(session, "lessonMetric", choices = ok, selected = sel)
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

  # Populate Group By choices: higher levels above current aggLevel
  observe({
    level_order <- c("course","module","chapter","lesson")
    present <- intersect(level_order, names(content_tree))
    lvl <- input$aggLevel %||% "lesson"
    if (!(lvl %in% present)) lvl <- tail(present, 1)
    idx <- match(lvl, level_order)
    higher <- if (!is.na(idx) && idx > 1) level_order[seq_len(idx - 1)] else character(0)
    higher <- intersect(higher, present)
    choices <- c("None" = "none", setNames(higher, stringr::str_to_title(higher)))
    updateSelectInput(session, "groupBy", choices = choices, selected = "none")
  })
  # Aggregated metrics by selected level
  agg_metrics <- reactive({
    level <- input$aggLevel %||% "lesson"
    les <- lesson_metrics()
    if (is.null(les) || nrow(les) == 0) return(tibble::tibble())
    # Ensure heading-by-level cols exist
    ensure_cols_zero <- c("headings_h1","headings_h2","headings_h3","headings_h6","nodes_native","nodes_custom",
                          "cc_stacks_total","cc_stacks_single","cc_stacks_multi","cc_questions_total")
    ensure_cols_na <- c("qa_template_fit","qa_template_structure_compliance","qa_cognitive_integrity",
                        "qa_template_boundary_discipline","qa_progressive_model_principle",
                        "qa_lesson_economy_and_cognitive_load","qa_priority_index")
    for (nm in setdiff(ensure_cols_zero, names(les))) les[[nm]] <- 0L
    for (nm in setdiff(ensure_cols_na, names(les))) les[[nm]] <- NA_real_
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
        cc_stacks_total = sum(cc_stacks_total, na.rm = TRUE),
        cc_stacks_single = sum(cc_stacks_single, na.rm = TRUE),
        cc_stacks_multi = sum(cc_stacks_multi, na.rm = TRUE),
        cc_questions_total = sum(cc_questions_total, na.rm = TRUE),
        images_count = sum(images_count, na.rm = TRUE),
        tables_count = sum(tables_count, na.rm = TRUE),
        lists_count = sum(lists_count, na.rm = TRUE),
        latex_total = sum(latex_total, na.rm = TRUE),
        latex_inline = sum(latex_inline, na.rm = TRUE),
        latex_display = sum(latex_display, na.rm = TRUE),
        latex_display_multi = sum(latex_display_multi, na.rm = TRUE),
        # QA metrics: average instead of sum (these are scores, not counts)
        qa_template_fit = mean(qa_template_fit, na.rm = TRUE),
        qa_template_structure_compliance = mean(qa_template_structure_compliance, na.rm = TRUE),
        qa_cognitive_integrity = mean(qa_cognitive_integrity, na.rm = TRUE),
        qa_template_boundary_discipline = mean(qa_template_boundary_discipline, na.rm = TRUE),
        qa_progressive_model_principle = mean(qa_progressive_model_principle, na.rm = TRUE),
        qa_lesson_economy_and_cognitive_load = mean(qa_lesson_economy_and_cognitive_load, na.rm = TRUE),
        qa_priority_index = mean(qa_priority_index, na.rm = TRUE),
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

  # Qualitative Analysis tab: distribution plot
  output$qualitativePlotly <- plotly::renderPlotly({
    df <- lesson_metrics_acc()
    if (is.null(df) || nrow(df) == 0 || !"qa_priority_index" %in% names(df)) {
      return(plotly::plotly_empty())
    }

    # Filter to only lessons with priority index
    df_plot <- df %>%
      dplyr::filter(!is.na(qa_priority_index)) %>%
      dplyr::select(lesson_id, lesson, qa_priority_index, qa_status)

    if (nrow(df_plot) == 0) {
      return(plotly::plotly_empty())
    }

    dist_type <- if (!is.null(input$qualitativeDist) && identical(input$qualitativeDist, "Boxplot")) "box" else "hist"
    xr <- range(c(0, df_plot$qa_priority_index), na.rm = TRUE)

    if (identical(dist_type, "box")) {
      # Boxplot with jittered points below
      plt <- plotly::plot_ly() %>%
        plotly::add_trace(
          x = df_plot$qa_priority_index,
          type = "box",
          orientation = "h",
          name = "Priority Index",
          fillcolor = "#8DD3C7",
          marker = list(color = "#8DD3C7"),
          line = list(color = "black", width = 1),
          boxpoints = FALSE,  # Don't show default points
          hoverinfo = "x"
        ) %>%
        plotly::add_trace(
          x = df_plot$qa_priority_index,
          y = stats::runif(nrow(df_plot), -0.3, -0.1),  # Jitter below the box
          type = "scatter",
          mode = "markers",
          marker = list(
            size = 8,
            color = "#8DD3C7",
            line = list(color = "black", width = 0.5)
          ),
          text = ~paste0(df_plot$lesson, " (", df_plot$lesson_id, ")"),
          hovertemplate = "%{text}: %{x}<extra></extra>",
          showlegend = FALSE
        ) %>%
        plotly::layout(
          yaxis = list(title = "", showgrid = FALSE, zeroline = FALSE, showticklabels = FALSE, range = c(-0.5, 0.5)),
          xaxis = list(title = "Priority Index", range = c(xr[1], xr[2])),
          showlegend = FALSE,
          margin = list(l = 60, r = 20, t = 20, b = 40)
        )
    } else {
      # Histogram
      plt <- plotly::plot_ly() %>%
        plotly::add_trace(
          x = df_plot$qa_priority_index,
          type = "histogram",
          nbinsx = 20,
          name = "Priority Index",
          marker = list(color = "#8DD3C7", line = list(color = "black", width = 0.5)),
          opacity = 0.7
        ) %>%
        plotly::layout(
          yaxis = list(title = "Count"),
          xaxis = list(title = "Priority Index", range = c(xr[1], xr[2])),
          showlegend = FALSE,
          margin = list(l = 60, r = 20, t = 20, b = 40)
        )
    }

    plt %>%
      plotly::config(
        displaylogo = FALSE,
        modeBarButtonsToRemove = c(
          'zoom2d','pan2d','lasso2d','zoomIn2d','zoomOut2d',
          'hoverClosestCartesian','hoverCompareCartesian','toggleSpikelines'
        )
      )
  })

  # Qualitative Analysis tab: table and modal reasoning viewer
  output$qualitativeResultsTable <- DT::renderDT({
    df <- lesson_metrics_acc()
    if (is.null(df) || nrow(df) == 0) {
      return(DT::datatable(data.frame()))
    }

    # Select only the columns we want to show + qualitative_json for modal
    display_cols <- c("lesson_id", "lesson", "qa_status", "qa_priority_index")

    out <- df %>%
      dplyr::select(dplyr::any_of(c(display_cols, "qualitative_json", "qa_error")))

    # Only proceed if we have the qualitative_json column
    if (nrow(out) == 0 || !"qualitative_json" %in% names(out)) {
      return(DT::datatable(out, options = list(pageLength = 10, scrollX = TRUE), selection = 'single'))
    }

    # Store qualitative_json for the modal, then remove hidden columns from display
    # This is cleaner than trying to hide columns with DataTables
    qualitative_data <- out$qualitative_json

    # Keep only the display columns
    display_out <- out %>%
      dplyr::select(lesson_id, lesson, qa_status, qa_priority_index)

    # Rename to friendly names
    names(display_out) <- c("Lesson ID", "Lesson", "Status", "Priority Index")

    DT::datatable(
      display_out,
      options = list(
        pageLength = 10,
        scrollX = TRUE
      ),
      selection = 'single'
    )
  })

  observeEvent(input$qualitativeResultsTable_rows_selected, {
    idx <- input$qualitativeResultsTable_rows_selected
    df <- tryCatch(lesson_metrics_acc(), error = function(e) NULL)
    if (is.null(idx) || length(idx) != 1 || is.null(df) || nrow(df) < idx) return()
    row <- df[idx, , drop = FALSE]
    qjson <- tryCatch(as.character(row$qualitative_json[[1]]), error = function(e) "")
    a <- tryCatch(jsonlite::fromJSON(qjson, simplifyVector = FALSE), error = function(e) NULL)

    # Build formatted display and plain text version
    content_parts <- list()
    plain_text_parts <- c()

    # Title
    plain_text_parts <- c(plain_text_parts, paste0("Qualitative Analysis - Lesson ", as.character(row$lesson_id %||% "")))
    plain_text_parts <- c(plain_text_parts, paste(rep("=", 60), collapse = ""))

    if (!is.null(a) && !is.null(a$analysis)) {
      analysis <- a$analysis

      # Lesson Typing Section
      if (!is.null(analysis$lesson_typing)) {
        lt <- analysis$lesson_typing
        typing_text <- paste0(
          "Inferred Type: ", lt$inferred_type$stage %||% "N/A", " / ", lt$inferred_type$sub_type %||% "N/A", "\n",
          "Justification: ", lt$typing_justification %||% "N/A"
        )
        content_parts[[length(content_parts) + 1]] <- tagList(
          tags$h4("Lesson Typing"),
          tags$pre(style = "white-space: pre-wrap; word-wrap: break-word;", typing_text)
        )
        plain_text_parts <- c(plain_text_parts, "", "LESSON TYPING", typing_text)
      }

      # Rubric Scores Section
      if (!is.null(analysis$rubric)) {
        rubric <- analysis$rubric
        rubric_text <- paste(
          paste0("Template Fit: ", rubric$template_fit$score %||% "?", "/3 - ", rubric$template_fit$reason %||% ""),
          paste0("Structure Compliance: ", rubric$template_structure_compliance$score %||% "?", "/3 - ", rubric$template_structure_compliance$reason %||% ""),
          paste0("Cognitive Integrity: ", rubric$cognitive_integrity$score %||% "?", "/3 - ", rubric$cognitive_integrity$reason %||% ""),
          paste0("Boundary Discipline: ", rubric$template_boundary_discipline$score %||% "?", "/3 - ", rubric$template_boundary_discipline$reason %||% ""),
          paste0("Progressive Model: ", rubric$progressive_model_principle$score %||% "?", "/3 - ", rubric$progressive_model_principle$reason %||% ""),
          paste0("Lesson Economy & Load: ", rubric$lesson_economy_and_cognitive_load$score %||% "?", "/3 - ", rubric$lesson_economy_and_cognitive_load$reason %||% ""),
          sep = "\n\n"
        )
        content_parts[[length(content_parts) + 1]] <- tagList(
          tags$h4("Rubric Scores"),
          tags$pre(style = "white-space: pre-wrap; word-wrap: break-word;", rubric_text)
        )
        plain_text_parts <- c(plain_text_parts, "", "RUBRIC SCORES", rubric_text)
      }

      # Priority Index Section
      if (!is.null(analysis$priority_index)) {
        pi <- analysis$priority_index
        priority_text <- paste(
          paste0("Priority Index: ", pi$priority_index %||% "N/A"),
          paste0("Priority Band: ", pi$priority_band %||% "N/A"),
          paste0("Base Sum: ", pi$base_sum %||% "N/A"),
          paste0("Overload Factor: ", pi$overload_factor %||% "N/A"),
          paste0("Explanation: ", pi$priority_explanation %||% "N/A"),
          sep = "\n"
        )
        content_parts[[length(content_parts) + 1]] <- tagList(
          tags$h4("Priority Index"),
          tags$pre(style = "white-space: pre-wrap; word-wrap: break-word;", priority_text)
        )
        plain_text_parts <- c(plain_text_parts, "", "PRIORITY INDEX", priority_text)
      }

      # Atomic Lesson Assessment
      if (!is.null(analysis$atomic_lesson_assessment)) {
        ala <- analysis$atomic_lesson_assessment
        atomic_text <- paste0(
          "Is Single Atomic Lesson: ", ala$is_single_atomic_lesson %||% "N/A", "\n",
          "Number of Arcs: ", length(ala$arcs) %||% 0
        )
        content_parts[[length(content_parts) + 1]] <- tagList(
          tags$h4("Atomic Lesson Assessment"),
          tags$pre(style = "white-space: pre-wrap; word-wrap: break-word;", atomic_text)
        )
        plain_text_parts <- c(plain_text_parts, "", "ATOMIC LESSON ASSESSMENT", atomic_text)
      }

      # Metadata
      meta_parts <- c(
        paste0("Model: ", a$model %||% "N/A"),
        paste0("Validation Passed: ", a$validation_passed %||% "N/A"),
        paste0("Iterations: ", a$iterations %||% "N/A"),
        paste0("Elapsed Time: ", round(a$elapsed_time %||% 0, 1), " seconds"),
        paste0("Total Tokens: ", a$tokens$total %||% "N/A")
      )

      # Add validation errors if present and validation failed
      if (!is.null(a$final_errors) && length(a$final_errors) > 0) {
        meta_parts <- c(
          meta_parts,
          "",
          "Validation Errors:",
          paste0("  ", seq_along(a$final_errors), ". ", a$final_errors)
        )
      }

      meta_text <- paste(meta_parts, collapse = "\n")

      content_parts[[length(content_parts) + 1]] <- tagList(
        tags$h4("Analysis Metadata"),
        tags$pre(style = "white-space: pre-wrap; word-wrap: break-word;", meta_text)
      )
      # Don't include metadata in the copied plain text
    }

    # Combine plain text parts
    plain_text_report <- paste(plain_text_parts, collapse = "\n")

    showModal(modalDialog(
      title = paste("Qualitative Analysis - Lesson", as.character(row$lesson_id %||% "")),
      tagList(
        tags$div(
          style = "margin-bottom: 12px;",
          actionButton("copyReportBtn", "Copy Report to Clipboard",
                      icon = icon("clipboard"),
                      style = "font-size: 13px;")
        ),
        if (length(content_parts) > 0) content_parts else tags$em("No analysis data available."),
        tags$hr(),
        tags$details(tags$summary("Raw JSON"), tags$pre(style = "white-space: pre-wrap; word-wrap: break-word;", qjson)),
        # Hidden textarea for clipboard copy
        tags$textarea(id = "reportTextArea", style = "position: absolute; left: -9999px;", plain_text_report)
      ),
      easyClose = TRUE,
      size = "l",
      footer = tagList(
        tags$script(HTML("
          $(document).on('click', '#copyReportBtn', function() {
            var copyText = document.getElementById('reportTextArea');
            copyText.style.position = 'static';
            copyText.select();
            document.execCommand('copy');
            copyText.style.position = 'absolute';
            $(this).html('<i class=\"fa fa-check\"></i> Copied!');
            setTimeout(function() {
              $('#copyReportBtn').html('<i class=\"fa fa-clipboard\"></i> Copy Report to Clipboard');
            }, 2000);
          });
        "))
      )
    ))
  })
  
}