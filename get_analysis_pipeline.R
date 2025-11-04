# Utility: nullish coalescing for R (treats NULL/length-0/NA scalar as missing)
`%||%` <- function(x, y) {
  if (is.null(x)) return(y)
  if (length(x) == 0L) return(y)
  if (is.atomic(x) && length(x) == 1L && is.na(x)) return(y)
  x
}

# JSON helper: validate and parse to nested list; returns NULL on invalid
safe_fromJSON <- function(x) {
  if (!isTRUE(jsonlite::validate(x))) return(NULL)
  jsonlite::fromJSON(x, simplifyVector = FALSE)
}

# Text walker: collects plain text recursively from a Tiptap doc, skipping LaTeX nodes
collect_text_excl_latex <- function(node) {
  if (is.null(node)) return("")
  if (is.list(node) && !is.null(node$type)) {
    if (identical(node$type, "text")) return(node$text %||% "")
    if (identical(node$type, "latex")) return("")
    if (!is.null(node$content)) {
      return(paste0(purrr::map_chr(node$content, collect_text_excl_latex), collapse = ""))
    }
    return("")
  }
  if (is.list(node) && !is.null(node$content)) {
    return(paste0(purrr::map_chr(node$content, collect_text_excl_latex), collapse = ""))
  }
  ""
}

# Extracts readable heading text using the full pipeline (keeps LaTeX as $...$)
heading_plaintext_from_node <- function(heading_node) {
  # Build a minimal doc with just this heading, then run full pipeline so LaTeX is preserved
  subdoc <- list(type = "doc", content = list(heading_node))
  subdoc_json <- jsonlite::toJSON(subdoc, auto_unbox = TRUE, null = "null", digits = NA)
  txt <- tryCatch(get_content_text(subdoc_json)$content_text, error = function(e) "")
  txt <- as.character(txt %||% "")
  stringr::str_squish(txt)
}

# Chooses a split level (default H2) to partition the doc into sections
find_split_level <- function(doc, default_level = 2) {
  if (is.null(doc$content) || !is.list(doc$content)) return(NA_integer_)
  levs <- purrr::map_int(doc$content, ~ ifelse(identical(.x$type, "heading"), .x$attrs$level %||% NA_integer_, NA_integer_))
  cand <- levs[!is.na(levs) & levs >= 2]
  if (!length(cand)) return(NA_integer_)
  if (any(cand == default_level)) default_level else as.integer(names(sort(table(cand), TRUE)[1]))
}

# Splits a Tiptap doc into titled sections, returning indices and per-section subdoc
split_doc_by_headings <- function(doc, default_level = 2, include_heading = FALSE, drop_empty_sections = TRUE) {
  if (is.null(doc) || is.null(doc$content) || !is.list(doc$content)) {
    return(tibble::tibble(section_index = integer(), section_title = character(), start_block = integer(), end_block = integer(), heading_block_index = integer(), subdoc = list()))
  }
  blocks <- doc$content
  n <- length(blocks)
  # Split on ANY heading level uniformly
  is_heading <- function(b) identical(b$type, "heading")
  idx <- which(purrr::map_lgl(blocks, is_heading))
  if (!length(idx)) {
    return(tibble::tibble(
      section_index = 1L,
      section_title = "Lead",
      start_block = 1L,
      end_block = n,
      heading_block_index = NA_integer_,
      subdoc = list(list(type = "doc", content = blocks))
    ))
  }
  # Build sections: optional Lead (pre-first-heading), then one section per heading
  ranges <- list(); titles <- character(); heading_ix <- integer()
  if (idx[1] > 1L) {
    ranges[[length(ranges) + 1L]] <- c(1L, idx[1] - 1L)
    titles <- c(titles, "Lead")
    heading_ix <- c(heading_ix, NA_integer_)
  }
  for (i in seq_along(idx)) {
    h <- idx[i]
    start <- if (include_heading) h else (h + 1L)
    end <- if (i < length(idx)) idx[i + 1L] - 1L else n
    ranges[[length(ranges) + 1L]] <- c(start, end)
    titles <- c(titles, heading_plaintext_from_node(blocks[[h]]))
    heading_ix <- c(heading_ix, h)
  }
  sec_tbl <- tibble::tibble(
    section_index = seq_along(ranges),
    section_title = titles,
    start_block = vapply(ranges, `[`, 1L, FUN.VALUE = integer(1)),
    end_block = vapply(ranges, `[`, 2L, FUN.VALUE = integer(1)),
    heading_block_index = heading_ix
  )
  # Build subdocs; allow empty sections (start > end) to become empty docs
  subdocs <- purrr::pmap(list(sec_tbl$start_block, sec_tbl$end_block), function(s, e) {
    if (is.na(s) || is.na(e) || s > e) list(type = "doc", content = list()) else list(type = "doc", content = blocks[s:e])
  })
  dplyr::mutate(sec_tbl, subdoc = subdocs)
}

# Walks a single block and extracts LaTeX metadata for analysis
extract_latex_in_block <- function(block) {
  out <- list()
  walk_block <- function(node, parent = NULL) {
    if (is.null(node)) return()
    if (is.list(node) && !is.null(node$type) && node$type == "latex") {
      src <- node$attrs$latex %||% ""
      parent_type <- if (is.list(parent) && !is.null(parent$type)) parent$type else NA_character_
      siblings <- if (is.list(parent) && !is.null(parent$content) && is.list(parent$content)) length(parent$content) else 0L
      paragraph_only <- identical(parent_type, "paragraph") && identical(siblings, 1L)
      context <- if (paragraph_only) "paragraph_latex_only" else "inline_in_paragraph"
      len_chars <- nchar(src, allowNA = TRUE)
      newline_count <- stringr::str_count(src, "\n") + stringr::str_count(src, "\\\\")
      label <- dplyr::case_when(
        context == "inline_in_paragraph" & len_chars <= 12 & newline_count == 0 ~ "unit_or_symbol",
        context == "inline_in_paragraph" & newline_count == 0 ~ "inline_expression",
        context == "paragraph_latex_only" & newline_count == 0 ~ "display_singleline",
        TRUE ~ "display_multiline"
      )
      out <<- append(out, list(tibble::tibble(context, len_chars, newline_count, label)))
    }
    if (!is.null(node$content) && is.list(node$content) && length(node$content) > 0L) {
      purrr::walk(node$content, ~ walk_block(.x, parent = node))
    }
  }
  walk_block(block, parent = NULL)
  if (!length(out)) tibble::tibble(context = character(), len_chars = integer(), newline_count = integer(), label = character()) else dplyr::bind_rows(out)
}

# Block feature extractors used for per-block and per-section metrics
an_node_type <- function(block_node) {
  btype <- block_node$type %||% NA_character_
  hlev <- if (identical(btype, "heading")) block_node$attrs$level %||% NA_integer_ else NA_integer_
  tibble::tibble(
    block_type = btype,
    heading_level = hlev,
    is_image = identical(btype, "image"),
    is_table = identical(btype, "table"),
    is_list = btype %in% c("bulletList", "orderedList")
  )
}

an_latex <- function(block_node) {
  L <- extract_latex_in_block(block_node)
  l_total <- nrow(L)
  l_inline <- sum(L$context == "inline_in_paragraph")
  l_disp <- sum(L$context == "paragraph_latex_only")
  l_ml <- sum(L$label == "display_multiline")
  tibble::tibble(
    latex_total = l_total,
    latex_inline = l_inline,
    latex_display = l_disp,
    latex_display_multiline = l_ml,
    inline_to_display_ratio = ifelse(l_disp > 0, l_inline / l_disp, NA_real_)
  )
}

an_text <- function(block_node, text_extractor = collect_text_excl_latex) {
  txt <- text_extractor(block_node)
  tibble::tibble(
    words_excl_math = stringr::str_count(txt, stringr::boundary("word")),
    raw_text_preview = stringr::str_trunc(txt, 140)
  )
}

analyze_block <- function(block_node) {
  dplyr::bind_cols(
    an_node_type(block_node),
    an_latex(block_node),
    an_text(block_node)
  )
}

# Helpers to enumerate top-level blocks and produce per-section block rows
list_top_level_blocks <- function(doc) {
  if (is.null(doc) || is.null(doc$content) || !is.list(doc$content)) return(list())
  doc$content
}

analyze_section_blocks <- function(lesson_id, section_index, subdoc) {
  blocks <- list_top_level_blocks(subdoc)
  if (!length(blocks)) return(tibble::tibble())
  purrr::imap_dfr(blocks, function(b, i) {
    dplyr::bind_cols(
      tibble::tibble(lesson_id = lesson_id, section_index = section_index, block_index = i),
      analyze_block(b)
    )
  })
}

# String cleanups used for text metrics (strip math) and sentence counting
strip_math <- function(x) {
  x %>%
    stringr::str_replace_all("\\$[^$]*\\$", " ") %>%
    stringr::str_replace_all("\\\\\\(.*?\\\\\\)", " ") %>%
    stringr::str_replace_all("\\\\\\[.*?\\\\\\]", " ") %>%
    stringr::str_replace_all("(?s)\\\\begin\\{[^}]+\\}.*?\\\\end\\{[^}]+\\}", " ")
}

safe_sentence_count <- function(txt) {
  txt <- stringr::str_squish(txt %||% "")
  if (nchar(txt) == 0) return(0L)
  length(stringr::str_split(txt, "(?<=[.!?])\\s+")[[1]])
}

# ---- Full tiptap -> plaintext pipeline (adapted from get_analysis.Rmd) ----

add_heading_space_end <- function(json_input) {
  # Ensure headings have trailing space when concatenated
  jq_query <- '
    .content |= map(
      if .type == "heading" and .content then
        .content |= map(
          if .text then .text |= . + " " else . end
        )
      else . end
    )
  '
  json_input %>% jqr::jq(jq_query)
}

add_paragraph_start_space <- function(json_input) {
  # Add leading space to the first paragraph text chunk for cleaner joins
  jq_query <- '.content |= map(
    if .type == "paragraph" then 
      .content |= (
        if length > 0 and .[0].text then
          .[0].text |= " " + .
        else . 
        end
      ) 
    else . 
    end
  )'
  json_input %>% jqr::jq(jq_query)
}

add_heading_space_start <- function(json_input) {
  # Ensure headings have leading space when concatenated
  jq_query <- '
    .content |= map(
      if .type == "heading" and .content then
        .content |= map(
          if .text then .text |= " " + . else . end
        )
      else . end
    )
  '
  json_input %>% jqr::jq(jq_query)
}

remove_italic_marks <- function(json_input) {
  # Drop italic marks to avoid duplicating emphasized text on collapse
  jq_query <- '
    walk(
      if type == "object" and .content? then
        .content |= map(
          if .marks? then
            .marks |= map(select(.type != "italic"))
          else .
          end
        )
      else .
      end
    )'
  json_input %>% jqr::jq(jq_query)
}

collapse_marked_text <- function(json_input) {
  # Collapse marked text nodes so their text is contiguous
  jq_query <- '[.. | objects | select(.text != null) | {text: .text, marked: (.marks? != null)}]
                | reverse 
                | reduce .[] as $item ([]; 
                    if ($item.marked and . != []) then [{text: ($item.text + .[0].text), marked: false}] + .[1:]
                    else [$item] + .
                    end)
                | reverse'
  json_input %>% jqr::jq(jq_query)
}

collapse_text_fields <- function(processed_json) {
  # Join text fields into a single string in original order
  jq_query <- '. | reverse | map(.text) | join(" ")'
  processed_json %>% jqr::jq(jq_query)
}

replace_tables_with_placeholders <- function(json_input) {
  # Replace top-level tables with TABLE### placeholders
  jq_query <- '
    def pad(n): (n|tostring) as $num | "000" + $num | .[-3:]; 
    def table_placeholder(i): "TABLE" + pad(i);
    reduce range(0; (.content | length)) as $i (.;
      if .content[$i].type == "table" then
        .content[$i] = { 
          "type": "paragraph", 
          "content": [{ "type": "text", "text": table_placeholder($i + 1) }] 
        }
      else 
        .
      end
    )'
  json_input %>% jqr::jq(jq_query)
}

extract_tables_as_dataframe <- function(json_input) {
  # Extract top-level tables for later replacement
  doc <- jsonlite::fromJSON(json_input, flatten = FALSE)
  content <- doc[["content"]]
  if (is.null(content) || !is.data.frame(content) || nrow(content) == 0) return(data.frame())
  tibble::as_tibble(content) %>%
    dplyr::mutate(table_id = paste0(toupper(type), stringr::str_pad(dplyr::row_number(), width = 3, pad = "0"))) %>%
    dplyr::filter(type == "table") %>%
    dplyr::select(table_id, content)
}

replace_nested_tables_with_placeholders3 <- function(json_input) {
  # Replace tables nested inside callouts with CALLOUT_TABLE_* placeholders
  if (is.list(json_input)) {
    json_input <- jsonlite::toJSON(json_input, auto_unbox = TRUE)
  }
  json_input %>%
    jqr::jq('.content[] |= 
        if .type == "callout" and (.content | type) == "array" then 
          .content = (.content | map(
            if .type == "table" then 
              { "type": "paragraph", "content": [{"type": "text", "text": ("CALLOUT_TABLE_" + (.attrs.id | tostring))}] } 
            else 
              . 
            end)) 
        else 
          . 
        end')
}

extract_tables_from_callouts <- function(json_input) {
  # Pull out nested tables from callouts to restore later
  doc <- jsonlite::fromJSON(json_input, flatten = TRUE)
  content <- doc[["content"]]
  if (is.null(content) || !is.data.frame(content) || nrow(content) == 0) return(data.frame())
  callout_nodes <- tibble::as_tibble(content) %>% dplyr::filter(type == "callout")
  nested_tables <- list()
  if (nrow(callout_nodes) > 0) {
    for (i in seq_len(nrow(callout_nodes))) {
      callout_content <- callout_nodes$content[[i]]
      if (is.data.frame(callout_content)) {
        for (j in seq_len(nrow(callout_content))) {
          nested_table <- callout_content[j, ] %>% dplyr::filter(type == "table")
          if (nrow(nested_table) > 0) {
            table_id <- nested_table$attrs.id[1]
            nested_table <- nested_table %>% dplyr::mutate(table_id = paste0("CALLOUT_TABLE_", table_id))
            nested_tables <- append(nested_tables, list(nested_table))
          }
        }
      }
    }
  }
  if (length(nested_tables) > 0) dplyr::bind_rows(nested_tables) %>% dplyr::select(table_id, content) else data.frame()
}

has_tables <- function(json_input) {
  doc <- jsonlite::fromJSON(json_input, flatten = FALSE)
  content <- doc[["content"]]
  if (is.null(content) || !is.data.frame(content) || nrow(content) == 0) return(FALSE)
  tibble::as_tibble(content) %>% dplyr::filter(type == "table") %>% nrow() > 0
}

has_nested_tables <- function(json_input) {
  # Detect tables nested in callouts via jq search
  q <- '
    .. | objects 
    | select(.type? == "callout" and .content?[]?.type == "table")
    | .type
  '
  res <- json_input %>% jqr::jq(q)
  length(res) > 0 && !identical(res, "")
}

replace_placeholders_with_tables3 <- function(document_string, table_df) {
  # Restore TABLE placeholders with computed text representations
  if (!is.data.frame(table_df) || nrow(table_df) == 0) return(document_string)
  out <- document_string
  for (i in seq_len(nrow(table_df))) {
    placeholder <- table_df$table_id[i]
    table_text <- convert_table_to_text3(table_df[i, ]$content)
    out <- stringr::str_replace(out, placeholder, table_text)
  }
  out
}

convert_table_to_text3 <- function(table_list) {
  # Convert a Tiptap table to a simple text table (one line per row)
  table_list_df <- as.data.frame(table_list)
  text_table <- list()
  for (row in seq_len(nrow(table_list_df))) {
    row_content <- table_list_df[row, "content"][[1]]
    if (!is.data.frame(row_content) || nrow(row_content) == 0) next
    row_texts <- list()
    for (column in seq_len(nrow(row_content))) {
      cell_content <- row_content[column, "content"]
      if (is.null(cell_content) || length(cell_content) == 0) {
        processed_text <- ""
      } else {
        cell_json <- jsonlite::toJSON(cell_content[[1]], auto_unbox = TRUE, pretty = TRUE)
        processed_text <- cell_json %>%
          remove_italic_marks() %>%
          collapse_marked_text() %>%
          collapse_text_fields()
        if (is.na(processed_text) || processed_text == "") processed_text <- " "
      }
      row_texts <- append(row_texts, processed_text)
    }
    text_table <- append(text_table, paste(row_texts, collapse = " | "))
  }
  paste0("\n", paste(text_table, collapse = "\n"), "\n")
}

has_latex <- function(json_input) {
  # Detect LaTeX nodes in the document
  q <- '
    .. | objects 
    | select(.type? == "latex" and .attrs?.id and .attrs?.latex)
    | .type
  '
  res <- json_input %>% jqr::jq(q)
  length(res) > 0 && !identical(res, "")
}

extract_latex_as_dataframe <- function(json_input) {
  # Extract LaTeX id+content into a data.frame for later restoration
  q <- '
    def extract_latex:
      .. | objects 
      | select(.type? == "latex" and .attrs?.id and .attrs?.latex)
      | {id: .attrs.id, latex: .attrs.latex};
    [extract_latex]
  '
  latex_json <- json_input %>% jqr::jq(q)
  latex_df <- jsonlite::fromJSON(latex_json, flatten = TRUE)
  if (is.data.frame(latex_df) && nrow(latex_df) > 0) latex_df$id <- paste0("LATEX-", latex_df$id)
  latex_df
}

replace_latex_with_placeholders <- function(json_input) {
  # Replace LaTeX nodes with LATEX-<id> placeholders to allow plain text collapsing
  q <- '
    walk(
      if type == "object" and .type? == "latex" and .attrs?.id then
        .text = ("LATEX-" + .attrs.id) | .type = "paragraph"
      else .
      end
    )
  '
  json_input %>% jqr::jq(q)
}

replace_placeholders_with_latex <- function(content_string, latex_df) {
  # Restore LaTeX placeholders with inline $...$ wrapping for readability
  if (!is.data.frame(latex_df) || nrow(latex_df) == 0) return(content_string)
  for (i in seq_len(nrow(latex_df))) {
    placeholder <- latex_df$id[i]
    wrapped_latex <- paste0("$", latex_df$latex[i], "$")
    content_string <- stringr::str_replace_all(content_string, stringr::fixed(placeholder), wrapped_latex)
  }
  content_string
}

content_string <- function(content_json) {
  # Collapse a Tiptap JSON string into a single readable string
  content_json %>%
    add_heading_space_end() %>%
    add_paragraph_start_space() %>%
    add_heading_space_start() %>%
    remove_italic_marks() %>%
    collapse_marked_text() %>%
    collapse_text_fields()
}

get_content_text <- function(json_input) {
  # Full pipeline: returns list(content_text, latex_df, regular_table_df, nested_table_df)
  if (is.list(json_input)) {
    json_input <- jsonlite::toJSON(json_input, auto_unbox = TRUE, pretty = TRUE)
  }
  if (!isTRUE(jsonlite::validate(json_input))) {
    return(list(content_text = "", regular_table_df = data.frame(), nested_table_df = data.frame(), latex_df = data.frame()))
  }
  latex_present <- has_latex(json_input)
  regular_tables_present <- has_tables(json_input)
  nested_tables_present <- has_nested_tables(json_input)
  latex_df <- data.frame(); regular_table_df <- data.frame(); nested_table_df <- data.frame()
  tryCatch({
    if (latex_present) {
      latex_df <- extract_latex_as_dataframe(json_input)
      json_input <- replace_latex_with_placeholders(json_input)
    }
    if (regular_tables_present) {
      regular_table_df <- extract_tables_as_dataframe(json_input)
      json_input <- replace_tables_with_placeholders(json_input)
    }
    if (nested_tables_present) {
      nested_table_df <- extract_tables_from_callouts(json_input)
      json_input <- replace_nested_tables_with_placeholders3(json_input)
    }
    txt <- json_input %>%
      content_string() %>%
      replace_placeholders_with_tables3(regular_table_df) %>%
      replace_placeholders_with_tables3(nested_table_df) %>%
      replace_placeholders_with_latex(latex_df)
    list(content_text = as.character(txt %||% ""), latex_df = latex_df, regular_table_df = regular_table_df, nested_table_df = nested_table_df)
  }, error = function(e) {
    message("Error in get_content_text: ", e)
    list(content_text = "", latex_df = data.frame(), regular_table_df = data.frame(), nested_table_df = data.frame())
  })
}

# Parse DB rows into validated docs (drop invalid/NA content)
parse_content_df <- function(pulled_content_df) {
  if (is.null(pulled_content_df) || nrow(pulled_content_df) == 0) return(tibble::tibble())
  pulled_content_df %>%
    dplyr::filter(!is.na(content)) %>%
    dplyr::mutate(doc = purrr::map(content, safe_fromJSON)) %>%
    dplyr::filter(!purrr::map_lgl(doc, is.null))
}

# Build per-section index: titles, ranges, subdoc JSON, and plaintext via full pipeline
build_sections_index <- function(content_parsed) {
  if (is.null(content_parsed) || nrow(content_parsed) == 0) return(tibble::tibble())
  idx <- content_parsed %>%
    dplyr::mutate(sections = purrr::map(doc, ~ split_doc_by_headings(.x, include_heading = TRUE))) %>%
    dplyr::select(lesson_id, sections) %>%
    tidyr::unnest(sections) %>%
    dplyr::arrange(lesson_id, section_index) %>%
    dplyr::mutate(
      section_title_norm = stringr::str_squish(section_title),
      subdoc_json = purrr::map_chr(subdoc, ~ jsonlite::toJSON(.x, auto_unbox = TRUE, null = "null", digits = NA)),
      plaintext = purrr::map_chr(subdoc, function(sd) {
        # Build content-only subdoc by removing a leading heading block
        if (!is.null(sd$content) && length(sd$content) > 0 && is.list(sd$content[[1]]) && identical(sd$content[[1]]$type, "heading")) {
          sd <- list(type = "doc", content = if (length(sd$content) > 1) sd$content[-1] else list())
        }
        out <- get_content_text(jsonlite::toJSON(sd, auto_unbox = TRUE, null = "null", digits = NA))$content_text
        out <- as.character(out %||% "")
        stringr::str_squish(out)
      })
    )
  idx
}

# Compute word/char/sentence metrics per section
compute_section_text_metrics <- function(sections_index) {
  if (is.null(sections_index) || nrow(sections_index) == 0) return(tibble::tibble())
  sections_index %>%
    dplyr::mutate(
      text_nomath = strip_math(plaintext),
      words_total = stringr::str_count(plaintext, stringr::boundary("word")),
      words_nomath = stringr::str_count(text_nomath, stringr::boundary("word")),
      chars_total = nchar(plaintext, allowNA = TRUE),
      sentences_nomath = purrr::map_int(text_nomath, safe_sentence_count),
      avg_words_per_sentence = dplyr::if_else(sentences_nomath > 0, words_nomath / sentences_nomath, NA_real_)
    ) %>%
    dplyr::select(lesson_id, section_index, section_title, words_total, words_nomath, chars_total, sentences_nomath, avg_words_per_sentence)
}

# Aggregate per-section block features into metrics (counts, words, etc.)
aggregate_blocks_by_section <- function(sections_index) {
  if (is.null(sections_index) || nrow(sections_index) == 0) return(tibble::tibble())

  blocks_by_section <- sections_index %>%
    dplyr::mutate(
      block_rows = purrr::pmap(
        list(lesson_id, section_index, subdoc),
        ~ analyze_section_blocks(..1, ..2, ..3)
      )
    )

  blocks_by_section %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      blocks_total        = nrow(block_rows),
      words_sum           = if (nrow(block_rows)) sum(block_rows$words_excl_math, na.rm = TRUE) else 0,
      words_mean          = if (nrow(block_rows)) mean(block_rows$words_excl_math, na.rm = TRUE) else NA_real_,
      words_median        = if (nrow(block_rows)) stats::median(block_rows$words_excl_math, na.rm = TRUE) else NA_real_,
      words_max           = if (nrow(block_rows)) max(block_rows$words_excl_math, na.rm = TRUE) else NA_real_,
      headings_count      = if (nrow(block_rows)) sum(block_rows$block_type == "heading", na.rm = TRUE) else 0,
      images_count        = if (nrow(block_rows)) sum(block_rows$is_image, na.rm = TRUE) else 0,
      tables_count        = if (nrow(block_rows)) sum(block_rows$is_table, na.rm = TRUE) else 0,
      lists_count         = if (nrow(block_rows)) sum(block_rows$is_list, na.rm = TRUE) else 0,
      latex_total         = if (nrow(block_rows)) sum(block_rows$latex_total, na.rm = TRUE) else 0,
      latex_inline        = if (nrow(block_rows)) sum(block_rows$latex_inline, na.rm = TRUE) else 0,
      latex_display       = if (nrow(block_rows)) sum(block_rows$latex_display, na.rm = TRUE) else 0,
      latex_display_multi = if (nrow(block_rows)) sum(block_rows$latex_display_multiline, na.rm = TRUE) else 0,
      inline_to_display_ratio = dplyr::if_else(latex_display > 0, latex_inline / latex_display, as.numeric(NA))
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(
      lesson_id, section_index,
      blocks_total, words_sum, words_mean, words_median, words_max,
      headings_count, images_count, tables_count, lists_count,
      latex_total, latex_inline, latex_display, latex_display_multi, inline_to_display_ratio
    )
}

# Orchestrates the full pipeline and returns final section-level table
build_section_metrics_table <- function(pulled_content_df) {
  parsed <- parse_content_df(pulled_content_df)
  if (nrow(parsed) == 0) return(tibble::tibble())
  sections_index <- build_sections_index(parsed)
  if (nrow(sections_index) == 0) return(tibble::tibble())
  block_agg <- aggregate_blocks_by_section(sections_index)
  text_metrics <- compute_section_text_metrics(sections_index)
  sections_index_static <- sections_index %>% dplyr::select(lesson_id, section_index, section_title_norm, start_block, end_block, subdoc_json, plaintext)
  out <- block_agg %>%
    dplyr::left_join(text_metrics, by = c("lesson_id", "section_index")) %>%
    dplyr::left_join(sections_index_static, by = c("lesson_id", "section_index")) %>%
    dplyr::select(
      lesson_id, section_index, section_title,
      plaintext,
      words_total, words_nomath, chars_total, sentences_nomath, avg_words_per_sentence,
      section_title_norm,
      blocks_total, words_sum, words_mean, words_median, words_max,
      headings_count, images_count, tables_count, lists_count,
      latex_total, latex_inline, latex_display, latex_display_multi, inline_to_display_ratio,
      start_block, end_block#,
      # subdoc_json  # Keep commented: large strings; enable if needed for debugging
    ) %>%
    dplyr::arrange(lesson_id, section_index)
  out
}


