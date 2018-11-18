# nifi-mergeparity-bundle

Merges a Flow File from data shards and accompanying Reed Solomon error
correction parity files back into the file's original form, accounting
for potentially missing parity files.

Makes use of Backblaze's Java Reed Solomon implementation, available at:
https://github.com/Backblaze/JavaReedSolomon/tree/master/src

## Status

Experimental, not for production use.

## Parameters

 - dataShards - number of data shards to split the file into
 - parityShards - number of parity shards to generate
 
## Author

Matt Bargenquast
