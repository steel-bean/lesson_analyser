`%||%` <- function(x, y) {
  if (is.null(x)) return(y)
  if (length(x) == 0L) return(y)
  if (is.atomic(x) && length(x) == 1L && is.na(x)) return(y)
  x
}

if (!requireNamespace("httr", quietly = TRUE)) stop("Package 'httr' required")
if (!requireNamespace("jsonlite", quietly = TRUE)) stop("Package 'jsonlite' required")
if (!requireNamespace("glue", quietly = TRUE)) stop("Package 'glue' required")

get_openai_key <- function() {
  api_key <- Sys.getenv("OPENAI_API_KEY")
  if (api_key == "") stop("OPENAI_API_KEY not found in environment variables")
  api_key
}

AVAILABLE_MODELS <- c(
  "gpt-4o",
  "gpt-4.1",
  "gpt-4o-mini",
  "gpt-5.1",
  "gpt-5.1-2025-11-13",
  "gpt-5.1-chat-latest"
)

DEFAULT_MODEL <- "gpt-5.1"

OPENAI_API_URL <- "https://api.openai.com/v1/chat/completions"
get_api_endpoint <- function(model) OPENAI_API_URL

is_gpt5_model <- function(model) grepl("^gpt-5", model, ignore.case = TRUE)

INSTRUCTIONS_FILE <- "lesson_analyser_instructions.txt"
SCHEMA_FILE <- "qa_schema.json"

load_analysis_instructions <- function(file_path = INSTRUCTIONS_FILE) {
  if (!file.exists(file_path)) stop(glue::glue("Instructions file not found: {file_path}"))
  paste(readLines(file_path, warn = FALSE), collapse = "\n")
}

load_analysis_schema <- function(file_path = SCHEMA_FILE) {
  if (!file.exists(file_path)) stop(glue::glue("Schema file not found: {file_path}"))
  jsonlite::fromJSON(file_path, simplifyVector = FALSE)
}

.qa_instructions <- local({ load_analysis_instructions() })
.qa_schema <- local({ load_analysis_schema() })

send_openai_request <- function(
  api_key,
  messages,
  model = DEFAULT_MODEL,
  max_tokens = 4000,
  temperature = 1,
  retries = 3,
  reasoning_effort = "medium"
) {
  if (!is.character(api_key) || nchar(api_key) < 20) stop("Invalid API key")
  if (!is.list(messages) || length(messages) == 0) stop("Messages must be a non-empty list")

  request_body <- list(model = model, messages = messages)

  if (is_gpt5_model(model)) {
    request_body$max_completion_tokens <- max_tokens
    request_body$temperature <- temperature
    request_body$response_format <- list(type = "json_object")
    if (!is.null(reasoning_effort) && reasoning_effort != "") {
      request_body$reasoning_effort <- reasoning_effort
    }
  } else {
    request_body$max_tokens <- max_tokens
    request_body$temperature <- temperature
    request_body$response_format <- list(type = "json_object")
  }

  for (attempt in 1:retries) {
    response <- tryCatch({
      httr::POST(
        url = get_api_endpoint(model),
        httr::add_headers(
          Authorization = paste("Bearer", api_key),
          `Content-Type` = "application/json"
        ),
        encode = "json",
        body = request_body,
        httr::timeout(120)
      )
    }, error = function(e) NULL)

    if (is.null(response)) {
      if (attempt < retries) {
        Sys.sleep(2^attempt)
        next
      } else {
        stop("Failed to connect to OpenAI API after ", retries, " attempts")
      }
    }

    status <- httr::status_code(response)

    if (status == 200) {
      return(jsonlite::fromJSON(httr::content(response, as = "text", encoding = "UTF-8"), flatten = TRUE))
    } else if (status == 429) {
      Sys.sleep(2^attempt)
    } else {
      error_content <- httr::content(response, as = "text", encoding = "UTF-8")
      message("DEBUG: API error response:\n", error_content)
      error_msg <- tryCatch({
        ej <- jsonlite::fromJSON(error_content)
        if (!is.null(ej$error$message)) ej$error$message else error_content
      }, error = function(e) error_content)
      stop("API request failed with status ", status, ": ", error_msg)
    }
  }

  stop("Max retries reached")
}

extract_json_from_response <- function(response_text) {
  # If input is a character vector, collapse it
  if (length(response_text) > 1) {
    response_text <- paste(response_text, collapse = "\n")
  }

  # Trim whitespace
  response_text <- trimws(response_text)

  # Remove everything up to ```json (if it exists)
  if (grepl("```json", response_text)) {
    response_text <- sub("^.*?```json\\s*", "", response_text)
  } else if (grepl("```", response_text)) {
    response_text <- sub("^.*?```\\s*", "", response_text)
  }

  # Remove everything after final ```
  response_text <- sub("```\\s*$", "", response_text)

  # Optionally, trim off any trailing commentary after closing }
  if (grepl("\\}\\s*[^}]*$", response_text)) {
    response_text <- sub("(\\}\\s*)[^}]*$", "\\1", response_text)
  }

  response_text <- trimws(response_text)

  # Final validation
  if (!jsonlite::validate(response_text)) {
    stop("Extracted text is not valid JSON")
  }

  return(response_text)
}

extract_analysis_response <- function(api_response) {
  if (is.null(api_response$choices) || length(api_response$choices) == 0) {
    stop("No choices in API response")
  }
  content <- api_response$choices$message.content[1]
  if (is.null(content) || content == "") {
    # Log the full API response for debugging
    response_json <- jsonlite::toJSON(api_response, auto_unbox = TRUE, pretty = TRUE)
    message("DEBUG: Full API response with empty content:\n", response_json)

    # Check for refusal or other finish reasons
    finish_reason <- api_response$choices$finish_reason[1]
    if (!is.null(finish_reason) && finish_reason != "stop") {
      stop("Empty content in API response. Finish reason: ", finish_reason)
    }

    stop("Empty content in API response")
  }

  # Clean the JSON response (remove markdown fences, etc.)
  cleaned_json <- tryCatch({
    extract_json_from_response(content)
  }, error = function(e) {
    # If cleaning fails, try parsing the raw content
    content
  })

  analysis <- tryCatch({
    jsonlite::fromJSON(cleaned_json, simplifyVector = FALSE)
  }, error = function(e) NULL)
  if (is.null(analysis)) return(NULL)

  tokens <- list(
    prompt = api_response$usage$prompt_tokens %||% NA,
    completion = api_response$usage$completion_tokens %||% NA,
    total = api_response$usage$total_tokens %||% NA
  )

  list(
    analysis = analysis,
    tokens = tokens,
    finish_reason = api_response$choices$finish_reason[1]
  )
}

validate_analysis_with_feedback <- function(analysis, schema) {
  errors <- character()

  validate_node <- function(data, schema_node, path = "") {
    node_errors <- character()
    schema_props <- schema_node$properties
    if (is.null(schema_props) && !is.null(schema_node$type)) return(node_errors)

    if (!is.null(schema_node$required)) {
      required_fields <- unlist(schema_node$required)
      missing_fields <- setdiff(required_fields, names(data))
      if (length(missing_fields) > 0) {
        field_path <- if (nchar(path) > 0) paste0(path, ".") else ""
        node_errors <- c(node_errors, paste0(
          "Missing required field(s) at '", field_path, "': ",
          paste(missing_fields, collapse = ", ")
        ))
      }
    }

    if (!is.null(schema_props)) {
      for (prop_name in names(schema_props)) {
        prop_schema <- schema_props[[prop_name]]
        field_path <- if (nchar(path) > 0) paste0(path, ".", prop_name) else prop_name
        if (!prop_name %in% names(data)) next
        prop_data <- data[[prop_name]]

        expected_type <- prop_schema$type
        if (!is.null(expected_type)) {
          if (is.list(expected_type)) expected_type <- expected_type[[1]]
          valid_type <- FALSE
          if (expected_type == "object") {
            valid_type <- is.list(prop_data) && !is.null(names(prop_data))
          } else if (expected_type == "array") {
            valid_type <- is.list(prop_data)
          } else if (expected_type == "string") {
            valid_type <- is.character(prop_data)
          } else if (expected_type == "integer") {
            valid_type <- is.numeric(prop_data) && prop_data == round(prop_data)
          } else if (expected_type == "number") {
            valid_type <- is.numeric(prop_data)
          } else if (expected_type == "boolean") {
            # Accept actual booleans OR string representations
            valid_type <- is.logical(prop_data) ||
                         (is.character(prop_data) && tolower(prop_data) %in% c("true", "false"))
          }
          if (!valid_type) {
            node_errors <- c(node_errors,
              paste0("Field '", field_path, "' has incorrect type. Expected: ",
                     expected_type, ", Got: ", class(prop_data)[1]))
          }
        }

        if (!is.null(prop_schema$minimum) && is.numeric(prop_data)) {
          if (prop_data < prop_schema$minimum) {
            node_errors <- c(node_errors, paste0(
              "Field '", field_path, "' value ", prop_data,
              " is below minimum ", prop_schema$minimum
            ))
          }
        }
        if (!is.null(prop_schema$maximum) && is.numeric(prop_data)) {
          if (prop_data > prop_schema$maximum) {
            node_errors <- c(node_errors, paste0(
              "Field '", field_path, "' value ", prop_data,
              " exceeds maximum ", prop_schema$maximum
            ))
          }
        }

        if (!is.null(prop_schema$enum)) {
          valid_values <- unlist(prop_schema$enum)
          valid_values_non_null <- valid_values[!sapply(valid_values, is.null)]
          if (!is.null(prop_data) && !prop_data %in% valid_values_non_null) {
            node_errors <- c(node_errors, paste0(
              "Field '", field_path, "' has invalid value '", prop_data, "'."
            ))
          }
        }

        if (!is.null(prop_schema$properties) && is.list(prop_data)) {
          node_errors <- c(node_errors, validate_node(prop_data, prop_schema, field_path))
        }
        if (!is.null(prop_schema$items) && is.list(prop_data)) {
          for (i in seq_along(prop_data)) {
            item_path <- paste0(field_path, "[", i, "]")
            if (!is.null(prop_schema$items$properties)) {
              node_errors <- c(node_errors, validate_node(prop_data[[i]], prop_schema$items, item_path))
            }
          }
        }
      }
    }
    node_errors
  }

  errs <- validate_node(analysis, schema, "")
  if (length(errs) > 0) {
    feedback_message <- paste0(
      "The JSON response has the following validation errors:\n\n",
      paste(paste0(seq_along(errs), ". ", errs), collapse = "\n"),
      "\n\nPlease correct these issues and return a complete, valid JSON object that exactly matches the schema."
    )
    return(list(is_valid = FALSE, errors = errs, feedback_message = feedback_message))
  }
  list(is_valid = TRUE, errors = NULL, feedback_message = NULL)
}

analyze_lesson_qualitative <- function(
  lesson_text,
  lesson_id = NULL,
  lesson_title = NULL,
  model = DEFAULT_MODEL,
  api_key = NULL,
  include_metadata = TRUE,
  max_iterations = 3,
  reasoning_effort = "medium",
  log_validations = TRUE
) {
  if (is.null(api_key)) api_key <- get_openai_key()
  if (is.null(lesson_text) || nchar(lesson_text) < 100) stop("Lesson text is too short or missing")

  # Log start of analysis
  lesson_label <- if (!is.null(lesson_title)) {
    paste0(lesson_id, " (", lesson_title, ")")
  } else if (!is.null(lesson_id)) {
    lesson_id
  } else {
    "unknown"
  }
  message("\n=== Starting qualitative analysis for lesson ", lesson_label, " ===")
  message("Model: ", model, " | Max iterations: ", max_iterations)

  # Create validation log directory if logging is enabled
  validation_log_dir <- NULL
  if (log_validations) {
    validation_log_dir <- file.path("qualitative_validation_logs", format(Sys.time(), "%Y%m%d_%H%M%S"))
    if (!dir.exists(validation_log_dir)) {
      dir.create(validation_log_dir, recursive = TRUE)
    }
  }

  metadata_text <- ""
  if (include_metadata && (!is.null(lesson_id) || !is.null(lesson_title))) {
    parts <- c()
    if (!is.null(lesson_id)) parts <- c(parts, paste("Lesson ID:", lesson_id))
    if (!is.null(lesson_title)) parts <- c(parts, paste("Lesson Title:", lesson_title))
    metadata_text <- paste("\n\n--- Lesson Metadata ---\n", paste(parts, collapse = "\n"), "\n--- End Metadata ---\n\n", sep = "")
  }

  user_prompt <- paste0(
    metadata_text,
    "--- Lesson Content ---\n\n",
    lesson_text,
    "\n\n--- End Lesson Content ---\n\n",
    "Please analyze this lesson according to the instructions and return a JSON object matching the schema provided in the system instructions."
  )

  messages <- list(
    list(role = "system", content = .qa_instructions),
    list(role = "user", content = user_prompt)
  )

  total_token_count <- 0; prompt_token_count <- 0; completion_token_count <- 0
  start_time <- Sys.time()
  last_extracted <- NULL
  last_validation_result <- NULL
  iteration_logs <- list()

  for (iteration in 1:max_iterations) {
    message("  Iteration ", iteration, "/", max_iterations, ": Sending request to API...")

    api_response <- send_openai_request(
      api_key = api_key,
      messages = messages,
      model = model,
      max_tokens = 4000,
      temperature = 1,
      reasoning_effort = reasoning_effort
    )

    message("  Iteration ", iteration, "/", max_iterations, ": Received response, extracting analysis...")

    extracted <- extract_analysis_response(api_response)
    if (is.null(extracted)) stop("Failed to extract analysis from API response")
    last_extracted <- extracted

    total_token_count <- total_token_count + (extracted$tokens$total %||% 0)
    prompt_token_count <- prompt_token_count + (extracted$tokens$prompt %||% 0)
    completion_token_count <- completion_token_count + (extracted$tokens$completion %||% 0)

    message("  Iteration ", iteration, "/", max_iterations, ": Validating response...")

    validation_result <- validate_analysis_with_feedback(extracted$analysis, .qa_schema)
    last_validation_result <- validation_result

    # Log this iteration
    iteration_log <- list(
      iteration = iteration,
      timestamp = Sys.time(),
      is_valid = validation_result$is_valid,
      response_json = extracted$analysis,
      errors = validation_result$errors,
      feedback_message = validation_result$feedback_message
    )
    iteration_logs[[iteration]] <- iteration_log

    # Save iteration log to file
    if (log_validations && !is.null(validation_log_dir)) {
      log_filename <- file.path(
        validation_log_dir,
        paste0("lesson_", lesson_id %||% "unknown", "_iteration_", iteration, ".json")
      )
      jsonlite::write_json(
        iteration_log,
        log_filename,
        pretty = TRUE,
        auto_unbox = TRUE,
        null = "null"
      )
    }

    if (validation_result$is_valid) {
      elapsed_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
      message("  ✓ Validation PASSED on iteration ", iteration, "/", max_iterations)
      message("=== Completed successfully in ", round(elapsed_time, 1), "s | Tokens: ", total_token_count, " ===\n")
      return(list(
        lesson_id = lesson_id,
        lesson_title = lesson_title,
        model = model,
        timestamp = Sys.time(),
        elapsed_time = elapsed_time,
        tokens = list(prompt = prompt_token_count, completion = completion_token_count, total = total_token_count),
        analysis = extracted$analysis,
        validation_passed = TRUE,
        iterations = iteration,
        iteration_logs = iteration_logs
      ))
    } else {
      message("  ✗ Validation FAILED on iteration ", iteration, "/", max_iterations)
      if (length(validation_result$errors) > 0) {
        message("    Errors: ", paste(head(validation_result$errors, 3), collapse = "; "))
        if (length(validation_result$errors) > 3) {
          message("    ... and ", length(validation_result$errors) - 3, " more")
        }
      }
      message("    Sending feedback and retrying...")
      messages <- append(messages, list(
        list(role = "assistant", content = jsonlite::toJSON(extracted$analysis, auto_unbox = TRUE)),
        list(role = "user", content = validation_result$feedback_message)
      ))
    }
  }

  # If we reach here, validation failed on all iterations
  elapsed_time <- as.numeric(difftime(Sys.time(), start_time, units = "secs"))
  message("  ✗ All iterations exhausted - validation still failing")
  message("=== Failed after ", max_iterations, " iterations (", round(elapsed_time, 1), "s) ===\n")
  list(
    lesson_id = lesson_id,
    lesson_title = lesson_title,
    model = model,
    timestamp = Sys.time(),
    elapsed_time = elapsed_time,
    tokens = list(prompt = prompt_token_count, completion = completion_token_count, total = total_token_count),
    analysis = last_extracted$analysis,
    validation_passed = FALSE,
    iterations = max_iterations,
    final_errors = last_validation_result$errors %||% character(0),
    iteration_logs = iteration_logs
  )
}

# Save analysis result to JSON file
save_analysis_result <- function(result, output_dir = "qualitative_analysis_results") {
  if (!dir.exists(output_dir)) {
    dir.create(output_dir, recursive = TRUE)
  }

  filename <- paste0(
    "lesson_",
    result$lesson_id,
    "_",
    format(result$timestamp, "%Y%m%d_%H%M%S"),
    ".json"
  )

  filepath <- file.path(output_dir, filename)

  jsonlite::write_json(
    result,
    filepath,
    pretty = TRUE,
    auto_unbox = TRUE,
    null = "null"
  )

  invisible(filepath)
}

