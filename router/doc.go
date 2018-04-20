/*

## Document API
the CRUD operations:
create: PUT dbname/spacename
read: GET dbname/spacename/docid
update: POST dbname/spacename/docid
delete: DELETE dbname/spacename/docid
Partial Update, Conditional Update
http body as JSON format to contains document

implementation:
core in mem data structure:
map dbname->dbInfo
	dbInfo has map spacename -> spaceInfo
		spaceInfo has a sorted list to slotInfo (query slotInfo by slotId)

limited:
dbname max 100 char
spacename max 100 char
docid 64bit

update:
1、retrieve single db+space+slots info from master when missing cache
2、retrieve single db+space+slots info from master when ps returned error code

*/
package router
