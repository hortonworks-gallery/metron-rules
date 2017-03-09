DegreeInOfHost
==============

Measure the number of distinct hosts which are talking to each host.

This can be very useful for determining if a given host is acting as a server of some sort.

The example uses HyperLogLogPlus to determine the cardinality.


Relies On
---------
YAF telemetry (netflow)
