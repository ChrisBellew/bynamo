# Bynamo - An implementation of the Dynamo whitepaper

## Notes

- Read committed

## Possible optimisations

- Interrupt sequential scan of SS tables on disk if a matching entry has been written to the LSM tree.
