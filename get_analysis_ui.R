# Define UI for application that draws a histogram
ui <- fluidPage(
  tags$h2("getAnalysis"),
  sidebarLayout(
    sidebarPanel(
      width = 4,
      shinyWidgets::treeInput(
        inputId = "lessonTree",
        label = "Select lessons:",
        choices = create_tree(content_tree_display),
        #selected = "X",
        returnValue = "text",
        closeDepth = 0
      )
    ),
    mainPanel(
      width = 8,
      tags$div(
        style = "margin-bottom: 12px; display: flex; align-items: center; gap: 12px;",
        actionButton("pullContent", "Analyse content", style = "margin-top: 7px;"),
        tags$div(
          style = "margin-top: 0;",
          shinyWidgets::pickerInput("qualitativeModel", "Select model for qualitative analysis",
            choices = list(
              "Do not perform qualitative analysis" = "",
              "gpt-5.1" = "gpt-5.1",
              "gpt-5-mini" = "gpt-5-mini"
              # "gpt-5.1-2025-11-13" = "gpt-5.1-2025-11-13",
              # "gpt-5.1-chat-latest" = "gpt-5.1-chat-latest",
              # "gpt-4o" = "gpt-4o",
              # "gpt-4.1" = "gpt-4.1",
              # "gpt-4o-mini" = "gpt-4o-mini"
            ),
            selected = "",
            width = "320px"
          )
        ),
        tags$span(textOutput("analysisProgress", inline = TRUE), style = "color:#666; margin-top: 7px;")
      ),
      tabsetPanel(
        id = "mainTabs",
        #tabPanel("Selection Summary", DTOutput("summaryTable")),
        #tabPanel("Selected IDs", verbatimTextOutput("selectedLessonIds")),
        tabPanel("Section-Level Analysis",
          div(
            div(style = "max-width: 960px; margin: 0 auto;",
              div(style = "display:flex; gap:16px; align-items:center;",
                selectInput("sectionMetric", "Metric", choices = NULL, width = "300px"),
                selectInput("sectionDist", "Distribution", choices = c("Boxplot","Histogram"), selected = "Boxplot", width = "200px")
              ),
              plotly::plotlyOutput("sectionPlotly", height = "560px")
            ),
            
            DTOutput("sectionMetricsTable")
          )
        ),
        tabPanel("Group analysis",
          div(
            div(style = "max-width: 960px; margin: 0 auto;",
              div(style = "display:flex; gap:16px; align-items:center;",
                selectInput("lessonMetric", "Metric", choices = NULL, width = "300px"),
                selectInput("aggLevel", "Aggregate by", choices = NULL, width = "260px"),
                selectInput("groupBy", "Group By", choices = NULL, width = "200px"),
                selectInput("lessonDist", "Distribution", choices = c("Boxplot","Histogram"), selected = "Boxplot", width = "200px")
              ),
              plotly::plotlyOutput("lessonPlotly", height = "560px")
            ),
            
            DTOutput("lessonMetricsTable")
          )
        ),
        tabPanel("Benchmark analysis",
          div(
            div(style = "max-width: 960px; margin: 0 auto;",
              div(style = "display:flex; gap:16px; align-items:center; margin-bottom:12px;",
                shinyWidgets::pickerInput("benchmarkMetrics", "Metrics", choices = NULL, multiple = TRUE,
                  options = list(`actions-box` = TRUE, `selected-text-format` = "count > 1"), width = "320px")
              ),
              uiOutput("benchmarkContainer")
            )
          )
        ),
        tabPanel("Metric analysis",
          div(
            div(style = "max-width: 960px; margin: 0 auto;",
              div(style = "display:flex; gap:16px; align-items:center; margin-bottom:12px;",
                shinyWidgets::pickerInput("metricMatrixMetrics", "Metrics", choices = NULL, multiple = TRUE,
                  options = list(`actions-box` = TRUE, `selected-text-format` = "count > 1"), width = "420px")
              ),
              plotly::plotlyOutput("metricMatrixPlot", height = "560px")
            )
          )
        ),
        tabPanel("Qualitative Analysis",
                 div(
                   div(style = "max-width: 960px; margin: 0 auto;",
                       div(style = "display:flex; gap:16px; align-items:center; margin-bottom:12px;",
                           selectInput("qualitativeDist", "Distribution",
                                      choices = c("Boxplot","Histogram"),
                                      selected = "Boxplot",
                                      width = "200px")
                       ),
                       plotly::plotlyOutput("qualitativePlotly", height = "300px"),
                       DTOutput("qualitativeResultsTable")
                   )
                 )
        )
      )
    )
  )
)





