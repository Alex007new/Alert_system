# Alert_system

Depending on the nature of the behavior of the metrics, the availability of historical data and the conditions\
in which it is necessary to recognize anomalies, various detection methods are used,
which can be divided into the following groups:
>
 - visual methods
 - statistical methods
 - algorithms based on time series forecasting
 - metric methods
 - machine learning methods
 - ensembles of algorithms
> 
We have prepared an alert system for our application. Statistical methods were used to detect anomalies. We used **Aeroflow** as our orchestrator.\
The system checks key metrics **every 15 minutes** (active users in the news feed / messenger, views, likes, CTR, number of messages sent).\
When an abnormal value is detected, an alert is sent to the chat Telegram - a message with the following information: metric, its value, deviation value.\
The message contains additional information that will help in the study of the causes of the anomaly - a link to the dashboard (prepared in **Superset**). 
