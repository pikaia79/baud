# Distributed Cluster Managment

BaudStorage is a globally distributed system for scaling geographically. 

regions -> cells -> racks  -> nodes

## JDOS per cell

usually each cell/IDC has one JDOS (Kubernetes)  cluster.  

## BaudMaster

## Object Space

Distribution policies can be configured flexibly by space: 

* one master + possible slave regions

* distribution among multiple regions - each partition has its replicas in one region, or reach partition has its replicas across different regions


