# SAPonAWS Hana DB Snapshot CLI Designs



__Steps__

1. Check if instance is running
2. Get private IP address (can also be passed in on the CLI)
3. Check if HANA is running and in a state where the snapshot can be taken 
4. Issue the SQL to create a snapshot 
5. Check the state of the snapshot backup job to make sure it is ok to continue
6. Freeze the HANA Data file system 
7. Determine the EBS volumes that are part of the HANA Data filesystem 
8. Start Snapshot of the EBS volumes 
9. Thaw filesystem 
10. Wait for snapshot to finish 
11. Confirm the snapshot backup job in HANA 
12. Write logs/return 



__TAGS__

- HANA-Data - created and marked when using the SAP CloudFormation (maintained on snapshots and retreated volumes)
- 

​	



