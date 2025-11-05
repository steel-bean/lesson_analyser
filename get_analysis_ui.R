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
      tags$div(style = "margin-bottom: 12px;", actionButton("pullContent", "Analyse content")),
      tabsetPanel(
        id = "mainTabs",
        #tabPanel("Selection Summary", DTOutput("summaryTable")),
        #tabPanel("Selected IDs", verbatimTextOutput("selectedLessonIds")),
        tabPanel("Section-Level Analysis",
          div(
            DTOutput("sectionMetricsTable")
          )
        ),
        tabPanel("Lesson Analysis",
          div(
            selectInput("lessonMetric", "Metric", choices = NULL, width = "300px"),
            plotOutput("lessonMetricPlot", height = "300px"),
            DTOutput("lessonMetricsTable")
          )
        )
      )
    )
  )
)





