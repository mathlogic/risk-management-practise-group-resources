# Logistic Regression with Generalized Linear Models (GLM) in R

### 1. Introduction to Generalized Linear Models (GLM)

A Generalized Linear Model (GLM) is a flexible extension of the traditional linear regression model. It helps model different types of outcomes like counts, 
proportions, or binary values—not just continuous numbers.

Every GLM has three main parts:
1.	A probability distribution for the response variable
    o	This could be a normal distribution (like in regular linear regression), or other types like Bernoulli (for binary outcomes) or Poisson (for counts).

2.	 A linear predictor
    o	A combination of input variables and their weights (coefficients), usually written as:
    η = X^T β

3.	A link function
    o	This function connects the linear predictor η to the expected value of the response variable μ = E(Y|X).   
    o	The link function varies with the distribution. For example, the logit function is used for binary outcomes.


The overall GLM equation is:
g(μ) = X^T β or μ = g⁻¹(X^T β)

### 2. Logistic Regression Model: Key Characteristics

Logistic Regression (LR) is a type of GLM for modeling binary outcomes (e.g., yes/no, 1/0).

Key Features of Logistic Regression:
1.	Handles binary outcomes:
    o	Predicts probabilities for two possible outcomes.
    
2.	 Uses the logit link function:
    o	The model estimates the log-odds of the event occurring:
 
    o	Then converts it back to probability:
 

3.	Estimates coefficients using maximum likelihood
    o	Instead of minimizing squared error like in linear regression, LR finds the set of coefficients that make the observed data most likely.

4.	Supports model evaluation through classification metrics
    o	Metrics like AUC (Area Under the Curve), KS (Kolmogorov-Smirnov), and Gini coefficient help measure how well the model distinguishes between classes.

5.	Feature interpretability
    o	Each coefficient represents the effect of a variable on the log-odds of the outcome. A positive coefficient increases the odds of the outcome being 1.

6.	Can handle categorical and continuous predictors
o	With the right preprocessing (like dummy encoding), LR can include both types of variables.


### 3. The Code Step-by-Step and Explanation

Here’s a simplified and cleaned-up version of the code used to train and evaluate a logistic regression model in R using GLM.

Code- 

Step 1: Load Data

train_data <- read_csv("train.csv")
test_data <- read_csv("test.csv")


Step 2: Prepare Variables

model_var <- c("var1", "var2", "var3")
x_train <- train_data %>% mutate_at(model_var, ~ifelse(is.na(.) | . < 0, -99, .))


Step 3: Binning and WOE Calculation

bins <- newBinning(data = x_train, target = "target", varsToInclude = model_var)
seg_bin <- interBin(bins)
train_bin <- applyBinning(binning = seg_bin, data = x_train, target = "target")


Step 4: Calculate WOE

for (var_name in model_var) {
  query <- sprintf("SELECT %s, SUM(weight) AS total, SUM(target * weight) AS event, SUM(non_event_flag * weight) AS non_event FROM train_bin GROUP BY %s", var_name, var_name)
  a1 <- sqldf(query)
  output <- bind_rows(output, a1)
}
output$woe <- log(output$event / sum(output$event) / (output$non_event / sum(output$non_event)))

Step 5: Applying WOE to Training Data
train_woe <- train_bin

for (var in model_var) {
  df <- output %>%
    filter(var_name == var) %>%
    select(bin, woe)
  colnames(df)[1] <- var
  train_woe <- left_join(train_woe, df, by = var)
  new_col_name <- paste0(var, "_woe")
  colnames(train_woe)[colnames(train_woe) == "woe"] <- new_col_name
}


Step 6: Build Model

woe_vars <- paste0(model_var, "_woe")
train_woe_data <- train_woe %>% select(all_of(woe_vars), target)

glm_model <- glm(target ~ ., data = train_woe_data, family = binomial(link = "logit"))
summary(glm_model)

Step 7: Model Evaluation Functions


### 4. Variable Selection Criteria

1.	Coefficient ≥ 0
    o	The variable has a non-negative effect on the log-odds of the target event occurring. 

2.	p-value < 0.001
    o	A low p-value implies that there's strong statistical evidence the variable is related to the target. 
    o	Reduces noise in the model
    o	Ensures reliability of predictors
    
3.	VIF < 2
    o	A VIF of 1 means no correlation; VIF > 2 suggests a moderate correlation.
    o	Lower VIF ensures your model isn't affected by highly correlated variables, which can make coefficients unstable and hard to interpret.
    o	Helps improve model generalizability and reduces overfitting.


![image.png](image.png)

![image.png](image.png)

### Reference

•	https://medium.com/@sahin.samia/a-comprehensive-introduction-to-generalized-linear-models-fd773d460c1d	

•	https://www.researchgate.net/publication/228396785_Generalized_Linear_Models



