# Flow

Here we present the flow of a single read or write request from the transaction runtime
all the way to the OS. As you can see, there are many layers of read-caching and
write-buffering involved.

Blue arrow means a call triggered by read.

Red arrow means a call triggered by write.

Black arrow means a non-trivial data dependency. For example:

* Nodes which are read on TrieStorage go to TrieRecorder to generate proof, so they
are connected with black arrow.
* The diagram still shows an accounting cache next to memtrie lookups. It has been
removed: a lookup now only counts the trie nodes it visits to compute costs.

<!-- Editable source: https://docs.google.com/presentation/d/1_iU5GfznFDUMUNi_7szBRd5hDrjqBxr8ap7eTCK-lZA/edit#slide=id.p  -->
![Diagram with read and write request flow](https://github.com/user-attachments/assets/232ae746-3f86-4a15-8a3a-08a544a88834)
