# Read and Write Flow for Storage Requests

The storage subsystem of nearcore is complex and has many layers. Here we
present the flow of a single read or write request from the transaction runtime
all the way to the OS. As you can see, there are many layers of read-caching and
write-buffering involved.

<!-- https://docs.google.com/presentation/d/1kHR8ONffUaCaBiJ4KM23h1tcfe4Z-_yKn2gaqlExaiY/edit#slide=id.p  -->
![Diagram with read and write request flow](https://user-images.githubusercontent.com/6342444/215088748-028b754f-16be-4f56-9edd-6ce58ff1c9ef.svg)