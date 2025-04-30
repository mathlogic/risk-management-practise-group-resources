rm(list = ls()) 
cat('\014') 
gc() 
set.seed(42) 

# Time tracking
track_time = data.frame()
tdiff <- function(start, end = Sys.time(), msg=""){
  data.frame(step = msg, time = as.numeric(difftime(end,start,units = "secs")))}

t0 = Sys.time()
# Load libraries
lst <- c("sparklyr", "dplyr", "ggplot2", "data.table", "openxlsx", "magrittr",
         "TUISG", "comet", "hrbrthemes", "lubridate", "dtplyr", "dia", "pleiadis", 
         "titanng", "purrr")
lapply(lst, require, character.only = TRUE,)

# Load custom tools
.libPaths("~")
# library(customtools)
library(xgboost)
library(coreUtils)
library(TUISG)
library(titanng)
library(comet)
library(pleiadis)
library(dia)
library(tidyverse)
library(reshape2)
library(mlr)
library(xgboost)
library(fst)
library(gt)
library(plotly)
library(readr)
library(dplyr)
# source("~/utils/vishnu_Rutils1.R")
# source("~/utils/vishnu_Rutils2.R")

(track_time = bind_rows(track_time, tdiff(start = t0,msg = "Load libraries and functions")))




data_path <- "data_path"
bin_path <- "bin_path"
model_path <- "model_path"


bin = readRDS(bin_path)
data <- read_csv(data_path)
xgb <- readRDS(model_path)


model_var = xgb$feature_names
data = data %>% mutate_at(model_var,~ ifelse(is.na(.) | . <0, -99, .))

data_bin <- applyBinning(binning = bin, data = data[,c(model_var, "target")], target = "target")


library("sqldf")
cnames=colnames(data_bin)
cnames <- cnames[!cnames %in% 'target']
if (exists("output")) rm(output)
for (i in 1:length(cnames))
{
  var_name <- cnames[i]
  print(var_name)
  a<-sprintf("select %s,count(*) as cnt, sum(target) as cnt1 from data_bin group by %s", var_name, var_name)
  a1<-sqldf(a)
  a1$var_name<-var_name
  colnames(a1)<-c("bin","total","bad","var_name")
  #print(nrow(a1))
  if (i==1) output<-a1 else output<-rbind(output,a1)
}


output$good<-output$total-output$bad
total_bad <- sum(data_bin$target)
total_good <-nrow(data_bin) - total_bad
output$pbad<-output$bad/total_bad
output$pgood<-output$good/total_good
output$woe<-log(output$pbad/output$pgood)
output$iv<-output$woe*(output$pbad-output$pgood)

write_csv(output, "path/IV_woe.csv")









