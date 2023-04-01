To run the peer:
-> python general.py
to bind to an OS assigned port

OR
-> python general.py <port>
to bind to <port>

Starting the peer also starts the CLI client.
The address for the client is conveniently printed to the terminal after startup.
It can be connected to with "telnet <host> <port>" e.g. "telnet localhost 15000"


The class heirarchy looks like:

AbstractBaseServer <- BaseServer <- PeerServer <- DBServer <- ConsensusServer
          ^                              ^                          ^
	  CLIServer                    GossipServer                     |
          ^------------------------------^-----------------------Server

The code is multithreaded. We start() a server instance which spawn 3 threads: one for gossiping, one for pruning peers, and one to try to initialize the Database from a peer.
We block on the main thread and handle peer messages in a different thread, which crash nicely if the message was malformed.
