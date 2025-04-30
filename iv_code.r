## Step for creating default bins
x_train <- train_data[, c(model_var, target, "weight")] 
x_train = x_train %>%
  mutate_at(model_var, ~ ifelse(is.na(.) | . <0, -99, .))

# Binning
initParallel(seq)
bins <- newBinning(
  data = x_train
  , target = target
  , weights = "weight" ## if needed
  , varsToInclude = model_var
)

saveRDS(bins, bin_obj)
###############################################################################

wellBinned <- names(bins$rejectCodes[bins$rejectCodes == 0])
iv <- lapply(bins$bins, function(x)
{
  sum(x$IV)
})
iv <- unlist(iv)
iv <- data.frame(Attribute = names(iv), IV = iv) %>% arrange(desc(IV)) %>% filter( IV != Inf)
iv <- left_join(iv, dictionary, by = "Attribute")
iv_top_vars_df <- iv %>% filter (IV>0.1)

## Intersection between variables_IV > 0.1 AND variables_Feature importance > 0
Feature_imp_vars <- as.vector(Feature_imp$Attribute)
iv_top_vars <- as.vector(iv_top_vars_df$Attribute)
model_vars1 <- union(intersect(wellBinned,iv_top_vars), intersect(wellBinned,Feature_imp_vars))

var_df <- data.frame("Attribute" = model_vars1)
var_df1 <- inner_join(var_df, iv, by = "Attribute") %>% arrange(desc(IV))
vars_after_IV_FI = paste0(model_saving_dir, vars_after_IV_FI,"_", model_version, ".csv")

write.csv(var_df1, vars_after_IV_FI)

final_model_vars <- as.vector(var_df1$Attribute)
saveRDS(final_model_vars, paste0(model_saving_dir, "final_model_vars.rds"))

##############################################################################
