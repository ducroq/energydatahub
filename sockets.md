# Energy Data Socket

To communicate data between NI myRIO and Raspberry Pi we will use sockets as they provide lightweight, direct, and efficient communication for resource-constrained devices, with custom data formats and protocols. 
Sockets enable real-time data exchange between devices, making them suitable for applications where low latency is critical. Web servers might introduce some overhead due to the HTTP request-response cycle.
For simple data exchange scenarios, setting up sockets might involve less development overhead compared to building a full-fledged web server application.

## Raspberry Pi server

We use sockets on Raspberry Pi to communicate with multiple NI myRIO clients simultaneously. 
To allow for scalability, ensure that Raspberry Pi has sufficient processing power and memory resources to handle communication with multiple clients efficiently.

For a more reliable and predictable connection setup, it's generally recommended to assign a fixed IP address to the Raspberry Pi server. 

Implement logic to manage accepted connections from multiple clients. This might involve using techniques like:
Threads: Create separate threads for each client, allowing the server to handle communication with multiple clients concurrently.

Raspberry Pi Python Sockets Tutorial: https://realpython.com/courses/python-sockets-part-1/

## NI myRIO client

On each NI myRIO, a client socket is created and a connection is established to the server socket's IP address and port number running on the Raspberry Pi.

NI myRIO TCP/IP Communication: https://forums.ni.com/t5/Academic-Hardware-Products-myDAQ/Possible-to-connect-the-myRIO-to-a-router-via-USB-to-Ethernet/td-p/2785470


## Data structure

Data is interchanged using the JSON open standard file format that uses human-readable text to store and transmit data objects consisting of attribute–value pairs and arrays.
While not strictly necessary for machine-to-machine communication, JSON remains human-readable, making it easier to debug and understand the data structure.

The energyDataSocket will use the following JSON data structure:
```python
schema = {
  "timestamp": {
    "type": "string"
  },
  "day-ahead-price": {
    "type": "float",
    "min": 0.0
  },
  "power_factor": {  # Example of adding validation for new data point
    "type": "float",
    "between": (0.0, 1.0)
  }
}
```
JSON allows to dynamically add new attributes and their corresponding values, this eliminates the need to modify the format for every new data point.

[Cerberus](https://docs.python-cerberus.org/) provides powerful yet simple and lightweight data validation functionality out of the box and is designed to be easily extensible, allowing for custom validation.



## Security

Password Protection (Optional):  Consider adding a basic password authentication layer on the server-side. NI myRIO clients would need to provide a valid password during connection establishment to gain access. This adds an extra layer of security, but keep in mind password strength and implement secure password storage mechanisms.

Transport Layer Security (TLS/SSL): TLS/SSL encrypts the data stream between the server and clients, ensuring confidentiality and data integrity during communication. This is a more robust security measure, especially if the data being exchanged is sensitive. Implementing TLS/SSL requires additional configuration and certificate management on both the server and client sides.

Basic Password Authentication with Python Sockets: https://realpython.com/courses/python-sockets-part-1/

TLS/SSL with Python Sockets: https://realpython.com/courses/python-sockets-part-1/




Open Sockets:  
On the Raspberry Pi, use socket programming functions to establish a TCP/IP socket connection with the NI myRIO's IP address and designated port number.

Send Data Packets:  Once connected, utilize the send function (or its equivalent in your chosen language) to transmit the formatted data packets over the socket to the NI myRIO.

Receiving Data on NI myRIO:

Listen for Incoming Data:  On the NI myRIO, create a socket and configure it to listen for incoming connections on the same port number used by the Raspberry Pi.

Receive Data Packets:  When a connection is established and data arrives from the Raspberry Pi, use the recv function (or its equivalent) to receive the data packets.

Parse Data:  Parse the received data packets to extract the timestamps and energy values based on the chosen format (e.g., split on commas in the CSV example).

Establish Sockets:  Sockets are software endpoints that facilitate communication between applications on a network.  Your code will involve creating sockets on both devices, specifying the IP address and port number for  connection.

Data Exchange: Once connected, you can send and receive data using functions like send and recv  (or their equivalents in your chosen language) within your program loops. Define a data format (e.g., comma-separated values, binary) for efficient transmission and parsing on the receiving end.


Error Handling:  Implement error handling mechanisms in your code to gracefully handle situations like connection failures, timeouts, or invalid data received.
Implement error handling routines on both Raspberry Pi and NI myRIO to handle potential issues like connection failures or malformed data packets.
obust error handling is crucial in a multi-client scenario. The server should handle situations like connection failures, timeouts, or invalid data from any client.

Data Validation:  Validate the received data on the Raspberry Pi to ensure it matches the expected format and contains no errors.
Validate the received data on the NI myRIO to ensure it matches the expected format and contains no errors.

Synchronization: If your project requires coordinated actions based on data exchange, consider implementing handshake protocols or message acknowledgements to ensure both devices are in sync.

Security: If your network environment is not secure, consider implementing additional security measures like access control or authentication mechanisms to restrict unauthorized access to the server from other devices on the network.

