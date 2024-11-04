# Flow

Here we present the flow of a single read or write request from the transaction runtime
all the way to the OS. As you can see, there are many layers of read-caching and
write-buffering involved.

Blue arrow means a call triggered by read.

Red arrow means a call triggered by write.

Black arrow means a non-trivial data dependency. For example:
* Nodes which are read on TrieStorage go to TrieRecorder to generate proof, so they
are connected with black arrow.
* Memtrie lookup needs current state of accounting cache to compute costs. When
query completes, accounting cache is updated with memtrie nodes. So they are connected
with bidirectional black arrow.

<!-- https://docs.google.com/presentation/d/1_iU5GfznFDUMUNi_7szBRd5hDrjqBxr8ap7eTCK-lZA/edit#slide=id.p  -->
![Diagram with read and write request flow](https://private-user-images.githubusercontent.com/8607261/382874468-c2a1a441-7eb4-46de-a727-473d60d2bb18.svg?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3MzA3NDQ3ODgsIm5iZiI6MTczMDc0NDQ4OCwicGF0aCI6Ii84NjA3MjYxLzM4Mjg3NDQ2OC1jMmExYTQ0MS03ZWI0LTQ2ZGUtYTcyNy00NzNkNjBkMmJiMTguc3ZnP1gtQW16LUFsZ29yaXRobT1BV1M0LUhNQUMtU0hBMjU2JlgtQW16LUNyZWRlbnRpYWw9QUtJQVZDT0RZTFNBNTNQUUs0WkElMkYyMDI0MTEwNCUyRnVzLWVhc3QtMSUyRnMzJTJGYXdzNF9yZXF1ZXN0JlgtQW16LURhdGU9MjAyNDExMDRUMTgyMTI4WiZYLUFtei1FeHBpcmVzPTMwMCZYLUFtei1TaWduYXR1cmU9ZGY4ZmQ3MjA0YWI4ZWFmNGY5MTA5YzBlNTg0MzI4OTBjMDhkOGY0OTY1NDA5Zjk1OWRlODU4NDIxZGU5MDRmZiZYLUFtei1TaWduZWRIZWFkZXJzPWhvc3QifQ.mWX24TIYt8GRqHtg3zVA5XUhcYrBhWjmjO7vRD_D75w)