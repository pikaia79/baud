# Backup and Restore 

point-in-time snapshot of a space

## Backup Storage Infrastructure

we use ContainerFS

## Create a backup 

1, backupCtl ==> CS  ==> PS (change partition state so no longer accept queries to the partition replica)r 

2.1, set the partition as BACKUP

2.2, stop replication

2.3, remember the repl position number and copy the files to the backup service

## Restore a backup



