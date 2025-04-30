## Capture Conversion Analysis

In credit risk modelling, capture-conversion analysis is essential for evaluating the effectiveness of delinquency triggers—such as 30+, 60+, or 90+ Days Past Due (DPD)—in identifying accounts that will eventually default over longer time horizons (e.g., within 24 months).
This dual analysis is critical for shaping early warning systems, optimizing collections strategies, and guiding risk-based decisions, such as account treatments or limit adjustments. By ensuring interventions are both timely and targeted, institutions can significantly reduce losses while enhancing operational effectiveness.

Capture rate measures the proportion of the eventual bad population that is flagged by each early delinquency definition

Capture Rate = Number of accounts that hit DPD and eventually default/Total number of eventual defaults

For a given MOB value, as DPD increases, the numerator gets smaller (fewer accounts reach target DPD), so capture rate tends to decrease.

Conversion analysis assesses how many of those flagged accounts actually transition into bad outcomes. 

Conversion Rate = (Total number of accounts that hit DPD and eventually default)/( Number of accounts that hit DPD)

For a given MOB value, as DPD increases, the denominator shrinks faster than the numerator (since severe delinquency is rare but serious), so conversion rate tends to increase.

Together, these metrics provide a holistic view of each trigger’s predictive power and efficiency, balancing early risk detection with precision.

Let eventual bads be defined as 180+ DPD in 18 MOB. 
| DPD               | Accounts with DPD | Eventual Bads Captured  | Capture Rate | Conversion Rate |
|-------------------|-------------------|-------------------------|--------------|-----------------|
| 30+ DPD 9MOB      | 1,000             | 600                     | 80%          | 60%             |
| 60+ DPD 9MOB      | 600               | 500                     | 67%          | 83%             |
| 90+ DPD 9MOB      | 300               | 270                     | 36%          | 90%             |
|Total Eventual Bads| —                 | **750**                 | —            | —               |

Based on the table above, short-term targets can be selected depending on how early delinquents need to be identified, balanced against the trade-off between capture rate and conversion rate.


```python

```
