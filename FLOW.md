# Flow

# Request path

```js
// Request comes in

// Is it a write?
// Yes, it's a write

// Calculate partition key hash

// Get nodes which are replicas for that partition

// Am i the leader?
// I am not the leader, fail

// Get current skiplist for partition

// Write it into the skiplist

// Return

// Is it a read?
// Yes, it's a read

// Get nodes which are replicas for that partition

// I am one of the nodes?
// I am not one of the nodes, fail

// Get current skiplist for partition

// Lookup value in skip list, is it there?
// It is there, return it
// else
// It is not there
// Get all SS tables for that partition
// For each SS table
// Is it there?
// It is there, return it
// Return none
```

# Leadership loop

```js
// Do I have a record of the instance IPs that is less than 5 seconds old?
// I do not
// Download instance IPs list

// Do I have a record of the hash ring?
// I do not
// Generate a new hash ring
// Send prepare hash ring request to other nodes
```
