# MODEL PERFROMANCE MEASURES

This document contains a comprehensive overview of different model performance measures used in Risk modelling along with their codes in R.
Model performance measures are essential for evaluating the effectiveness of predictive models. These metrics help in understanding how well a model is performing and guide improvements. Commonly used measures include Rank-order tables, KS statistics, Gini coefficient, Gains table, Lift chart, and AUC-ROC.

## 1.Rank-Order Table

It is a key model performance measure used to evaluate how well the model separates the binary classes, i.e. Defaulters from Non-Defaulters.
Below are the key steps to create the Rank-Order table:

- Step1: After applying the model on the labeled data, we can predict the probability of each record

- Step2: Now, sort the data by probability in ascending order and split it into 10 equal parts and rank them as 1 to 10, with Rank 1 assigned to the group with lowest probability and Rank 10 to the group with the highest probability

- Step3: Aggregate the data by Rank and get the count of population, count of Defaulters, and count of non-defaulters in each decile.

- Step4: Calculate the event-rate of each decile by (count of Defaulters/Total population of that decile)*100. This should follow the trend of the decile Rank i.e. For the highest decile, event-rate should be maximum and for the lowest decile, event-rate should be minimum. Ideally, we should not have a break in this trend throughout the deciles.

Generally, we keep the rank-order table in descending order of decile rank.

## 2.KS Statistic Calculation

Kolmogorov-Smirnov (KS) statistic measures how well the binary classifier model separates the Defaulter class from non-defaulter class. The range of KS statistic is between 0 and 1. Higher the value better the model in separating the Defaulter class from non-defaulter class.
Below are the key steps to calculate the KS table:

- Step1: We already have the count of defaulters, count of non-defaulters and event-rate from the rank-order table. Now calculate the cumulative count of event and non-event to make columns ‘Cumulative event’ and ‘Cumulative non-event’.
- Step2: Now calculate the %Cumulative event and %Cumulative non-event as:

    %Cumulative event = (Cumulative event/Total event in the modeling population)*100

    %Cumulative non-event = (Cumulative non-event/Total non-event in the modeling population)*100
- Step3: The KS column is computed as the difference between % Cumulative event and % Cumulative non-event. The point of maximum separation is the KS statistics.

The range of KS values is from 0 to 1. Ideally, the KS of test & OOT should be similar to the KS of train within a drop of 7%.

## 3.Gains Table

Cumulative Gains is the same as %Cumulative event. Lift is defined as the ratio between Cumulative Gains and population %age up to that decile.
Assume we randomly select a sample of 10% population, we would expect around 10% event rate as it is random, but if we select that 10% population from the topmost decile, we will get the event-rate as %Cumulative event of that decile.
Below is a table to understand the Gains table and KS table.

| Decile | Population | Count of event | Event-rate (%) | Count of non-event | Cumulative event count | Cumulative non-event count | %Cumulative event | %Cumulative non-event | KS (%) | Cumulative Gains (%) | Lift |
|--------|------------|----------------|----------------|---------------------|-------------------------|-----------------------------|--------------------|-------------------------|--------|------------------------|------|
| 10     | 1000       | 295            | 29.5           | 705                 | 295                     | 705                         | 37.5               | 7.7                     | 29.8   | 37.5                   | 3.75 |
| 9      | 1000       | 176            | 17.6           | 824                 | 471                     | 1529                        | 59.8               | 16.6                    | 43.3   | 59.8                   | 5.98 |
| 8      | 1000       | 115            | 11.5           | 885                 | 586                     | 2414                        | 74.5               | 26.2                    | 48.3   | 74.5                   | 7.45 |
| 7      | 1000       | 75             | 7.5            | 925                 | 661                     | 3339                        | 84.0               | 36.2                    | 47.7   | 84.0                   | 8.40 |
| 6      | 1000       | 35             | 3.5            | 965                 | 696                     | 4304                        | 88.4               | 46.7                    | 41.7   | 88.4                   | 8.84 |
| 5      | 1000       | 30             | 3.0            | 970                 | 726                     | 5274                        | 92.2               | 57.2                    | 35.0   | 92.2                   | 9.22 |
| 4      | 1000       | 23             | 2.3            | 977                 | 749                     | 6251                        | 95.2               | 67.8                    | 27.3   | 95.2                   | 9.52 |
| 3      | 1000       | 18             | 1.8            | 982                 | 767                     | 7233                        | 97.5               | 78.5                    | 19.0   | 97.5                   | 9.75 |
| 2      | 1000       | 13             | 1.3            | 987                 | 780                     | 8220                        | 99.1               | 89.2                    | 9.9    | 99.1                   | 9.91 |
| 1      | 1000       | 7              | 0.7            | 993                 | 787                     | 9213                        | 100.0              | 100.0                   | 0.0    | 100.0                  | 10.00 |


## 4.AUC-ROC curve

![image.png](image.png)

Above is a plot between %Cumulative event(y-axis) and %Cumulative non-event (x-axis). This is known as AUC-ROC (Receiver Operating Characteristics) curve and the area under this curve is known as AUC-ROC.
The value of AUC ranges from 0 to 1. It can also be interpreted as the curve between sensitivity and (1-specificity) where sensitivity is True positive rate(%Cumulative event in this case) and (1-specificity) is False positive rate(%Cumulative non-event in this case).

## 5.Gini Coefficient

The Gini Coefficient is an index to measure inequality. The value of the Gini Coefficient can be between 0 to 1. The higher the Gini coefficient, the better the model is.
Mathematically we can easily derive Gini from AUC using the formula: Gini = 2 * AUC – 1.



