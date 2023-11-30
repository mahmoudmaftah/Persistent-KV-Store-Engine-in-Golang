Author : Mahmoud maftah.
Project : Persistent KV-Store.
Language : Golang.


System interface:

Hello there, this Program is a persistent KV store, that could be used through the 
interface explained below, we are providing 4 strong different types of request, abstracting all the 
Specifications and implementation details.


Note : You can use this syntax if you are on Windows cmd.
===================================================================
Set Request structure : 
curl -X POST "http://localhost:8080/set?key=mahmoud&value=maftah"
===================================================================
Get Resuest structure :
curl -X POST "http://localhost:8080/get?key=mahmoud&value=maftah"
===================================================================
Del Request structure :
curl -X POST "http://localhost:8080/del?key=mahmoud"
===================================================================
Stop Request structure :
curl -X POST "http://localhost:8080/stop"
===================================================================



If you are on Windows cmd, you can use these commands to get an idea of the System speed, and latency:

// Set 2000 records: 
for /L %i in (0,1,2000) do curl -X POST "http://localhost:8080/set?key=Key_%i&value=Value_%i"

// Get the 2000 records
for /L %i in (0,1,2000) do curl -X POST "http://localhost:8080/get?key=Key_%i"

// Only delete even keys:
for /L %i in (0,2,2000) do curl -X POST "http://localhost:8080/set?key=Key_%i"

// Try to get the corresponding values again:
for /L %i in (0,1,2000) do curl -X POST "http://localhost:8080/get?key=Key_%i"


****************************************************************************************************
// At the end if the changes you made caused more than 10SST files to be created, you can witness the compaction algothm's effect
// by stopping the KV-store engine.

curl -X POST "http://localhost:8080/stop"


// Note01 : After having stopped the kv engine Closing the program becomes safe.

// Note02 : Even without sending the stop request, the system is fault taulerant and will manage
// to restart in a proper way.

// Note03 : You can run 'go test' Command to run testcases.