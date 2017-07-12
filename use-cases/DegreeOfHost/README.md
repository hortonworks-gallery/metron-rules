DegreeOfHost
============

This looks for hosts which have communicated with more hosts than their 95 percentile of connections over the last hour, also over corresponding time windows (same day over previous weeks).

This can be useful for finding hosts which have suddenly become very chatty, and may have been infected with something or behaving oddly.

Data Source: yaf, netflow
This example can also be used against a data source which includes logs of all conversations. Some firewall and dpi sources may given this information.
