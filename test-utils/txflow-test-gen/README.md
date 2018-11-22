Visual test gen for TxFlow
==========================

This is a visual editor for TxFlow graphs.

It contains an alternative naive implementation of TxFlow in JavaScript, that always DFSes from scratch without precaching to reduce the chance of errors.

Generated TxFlow graphs annotated with the alternative TxFlow implementation can then be converted into Rust unit tests.

# How to use:

Open index.html in the browser.

On top there are four buttons represnting four users. Pressing on the button creates a new message for the user and selects it.

Clicking on an existing message selects or deselects it. When a new message is created, all the selected messages become it's parents.

Whenever any number of messages is selected, the TxFlow graph is traversed from the selected messages, and annotated with epochs,
representatives, kickouts and endorsements.

The kickout messages are marked with bold letter 'K';

The representative messages are marked in a form 'R5(A,B)' where 5 is what epoch the representative message is for (it might be different
from the message epoch, if the representative message was not created via an epoch increase) and A and B are endorsements.

# Comments, Serializing / Deserializing

Use the `comment` text area to provide a short description of the test.

The `Serialized` text area contains JSON of the txflow. It is autoupdated whenever the graph is changed.

To save the serialialized JSON just copy-paste it from the textarea.

To restore to previously saved JSON copy-paste it back to the textarea and press `Deserialize`.

# Generating Rust test cases

Enter a function name for the test into the textbox under the `Serialized` textarea, and press `Gen Rust Test`.

Presently it only tests epochs, representative and kickout, but not endorsements.

