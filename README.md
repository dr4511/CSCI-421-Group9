# CSCI-421-Group9

## Members:
- Aidan Roullet aar5022
- Aidan Ohline abo3854
- Andrew Menkes adm5040
- Denise Reyes Lujan dr4511
- Pato Solis pgs5983

## Build:
1. cd src
2. javac JottQL.java
3. java JottQL \<dbLocation\> \<pageSize\> \<bufferSize\> \<indexing\>

## Args:
- dbLocation - Absolute or relative path to the database directory
- pageSize - Size of a page in bytes. Ignored on database restart.
- bufferSize - Maximum number of pages the buffer can hold.
- indexing - Enable (True) or disable (False) indexing. Ignored on restart.