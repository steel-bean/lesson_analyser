source("get_analysis_qualitative_pipeline.R")
source("get_analysis_global.R")
source("get_analysis_ui.R")
source("get_analysis_server.R")

# Run the application 
shinyApp(ui = ui, server = server)
