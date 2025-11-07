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
        actionButton("pullContent", "Analyse content"),
        tags$span(textOutput("analysisProgress", inline = TRUE), style = "color:#666;")
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
              plotOutput("sectionMetricDist", height = "220px"),
              plotOutput("sectionMetricPlot", height = "480px", click = "sectionPlot_click")
            ),
            DTOutput("sectionMetricsTable")
          )
        ),
        tabPanel("Lesson Analysis",
          div(
            div(style = "max-width: 960px; margin: 0 auto;",
              div(style = "display:flex; gap:16px; align-items:center;",
                selectInput("lessonMetric", "Metric", choices = NULL, width = "300px"),
                selectInput("aggLevel", "Aggregate by", choices = NULL, width = "260px"),
                selectInput("lessonDist", "Distribution", choices = c("Boxplot","Histogram"), selected = "Boxplot", width = "200px")
              ),
              plotOutput("lessonMetricDist", height = "220px"),
              plotOutput("lessonMetricPlot", height = "480px", click = "lessonPlot_click")
            ),
            DTOutput("lessonMetricsTable")
          )
        )
      )
    )
  )
)





