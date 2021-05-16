**DISTRIBUTED HASH TABLE FOR DISTRIBUTED DNS**

**Installing Dependencies**

The current project uses `Python3+` version.

To install the dependencies use

    pip3 install -r requirements.txt

**Code Structure**


The project used the Flask framework for both server and the client. The server folder contains all the files related to the server.


The client folder contains the flask-based web app, which talks to the API created using a cluster of servers.

**Assumptions**

The client is created based on the assumption that one of the servers from the cluster is running on port `8080`  and can be modified accordingly.

**Brief-Details for Setting It up**


We can spin up the multiple servers on different ports and IPs, but they must be attached in a ring to communicate with each other. All the servers handle the requests, and they reroute the requests to appropriate servers in the ring, handling the given range of hash-values of the entered key.

**Execution of Code**

To execute the server use -

    python3 server.py port

To attach to a ring

    python3 server.py port -r ring_port

To execute the client use -

    python3 client.py
