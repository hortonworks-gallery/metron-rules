DegreeOutOfHost
===============

Measure the number of distinct hosts a host is talking to.

This can be very useful for determining if a given host has suddenly started talking to more other machines than it would normally.

The example uses HyperLogLogPlus to determine the cardinality.


Relies On
---------
YAF telemetry (netflow)
