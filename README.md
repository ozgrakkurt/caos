# Concurrent Append Only Segment-list

This is a data structure that can be accessed from multiple threads without any locking while
 a single thread can append to it atomically whout locking.

Intended to be used for sharing list of indices, offsets of similar stuff between multiple readers
 while a writer can update the list atomically. This allows building an append only ACID datastore without compomising on performance.
