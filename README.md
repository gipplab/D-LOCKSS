D-LOCKSS (Go Edition)

This is a Go-based implementation of the Decentralized "Lots of Copies Keep Stuff Safe" concept using IPFS and Gossipsub.

Each running instance of this application acts as a peer in a decentralized network, collectively ensuring the preservation of PDF documents.

Features

Embedded IPFS Node: Runs a full Kubo (IPFS) node as part of the application.

Directory Watching: Automatically adds PDFs placed in the ./pdfs_to_add directory.

Gossipsub Discovery: Announces new files to peers and discovers files from peers using a shared Gossipsub topic.

Health Monitoring: Periodically checks the number of providers for every file it tracks.

Self-Healing (Auto-Pinning): If a file's provider count drops below a threshold (default: 5), the node will automatically download and pin it to increase redundancy.

How to Run

1. Prerequisites

You must have Go 1.19 or later installed.

A working internet connection and an environment that allows P2P connections (e.g., not behind a highly restrictive firewall).

2. Setup

Create a Directory:

mkdir d-lockss-go
cd d-lockss-go


Save the Files:
Save the main.go and go.mod files from this response into the d-lockss-go directory.

Install Dependencies:
Open a terminal in the directory and run go mod tidy. This will download all the required IPFS and libp2p modules specified in go.mod.

go mod tidy


3. Running the Application

Create the "Add" Directory:
The application watches a directory named pdfs_to_add. Create it.

mkdir pdfs_to_add


Run the Node:
Start the application from your terminal:

go run .


You're Live!
The application will first initialize an IPFS repository (in a new folder named .dlockss_repo) and then start the node. You will see logs indicating it's online and subscribed to the Gossipsub topic.

2025/11/12 10:30:00 No IPFS repo found. Initializing new one at ./.dlockss_repo
...
2025/11/12 10:30:05 Node is online. Peer ID: 12D3KooW...
2025/11/12 10:30:05 Subscribed to Gossipsub topic: /d-lockss-pdf-archive/1.0.0
2025/11/12 10:30:05 Watching for new files in: ./pdfs_to_add
2025/11/12 10:30:05 Starting content monitoring loop...
2025/11/12 10:30:05 Running periodic retrievability check...


4. How to Use

Add a File:
Find any PDF document on your computer and copy it into the ./pdfs_to_add directory.

Watch the Logs:
Within 10 seconds, the application will detect the file, add it to IPFS, pin it, and broadcast it to the network. The original file will be moved to a new ./added_pdfs directory.

2025/11/12 10:31:00 Found new PDF to add: my-document.pdf
2025/11/12 10:31:01 Added and pinned file my-document.pdf. CID: Qm...
2025/11/12 10:31:01 Tracking locally added file: my-document.pdf (Qm...)
2025/11/12 10:31:01 Announcing new file to network: my-document.pdf


Run a Second Peer:
To see the decentralized part in action, open a new terminal window. Copy the entire d-lockss-go directory to a new location (e.g., d-lockss-go-peer2), cd into it, and run go run . there.

This second peer will connect to the network, and within a minute, it will hear the gossip message from the first peer:

2025/11/12 10:32:15 Tracking new file from peer: my-document.pdf (Qm...)
2025/11/12 10:32:15 Checking providers for: my-document.pdf (Qm...)
2025/11/12 10:32:16 Found 1 other providers for my-document.pdf
2CSS/11/12 10:32:16 Provider count for my-document.pdf is low (1). Pinning now...
2025/11/12 10:32:18 Successfully pinned my-document.pdf to ensure availability.


The second peer saw the file had only 1 provider (peer 1), which is less than 5, so it automatically pinned the file itself. Now the file has 2 providers!



TODO:

- request replication by CID to spread content somewhat equally
- provide a UI using helia to browse and retrieve PDFs
- sharding via channels and CIDs
    - CID prefix defines the channel to join
    - 64 channels maximum to join
    - Every peer joins channels and asks for content in this channel 
    - First channel where no content is available prevents going deeper until content for this channel is available
    - The main channel has no prefix. 
    - to check replication factor, it start with the deepest channel and makes it way up until main channel. 
     - only nodes with a common CID overlap qualify as replicators. 
     - potential relicators meet at a certain prefix channel. Once this channel has 16*16 (256) members, the channel is not used anymore. Instead the next deeper channel is used. 
     - PROBLEM: If you host a millon CIDs, you would need to join a million channels - SHOWSTOPPER
    - alternatvely we could hold everyone in the same channel
        - now we have a limited bandwith. 
        - we could control who is allowed to post at what time through their PeerID
        - But everyone would have a diffent bandwidth. Therefore SHOWSTOPPER as well
    - are we okay with missing messages?
        - I guess yes. That would mean that we get an update eventually later, but propabilistically some day
    - similar then block size in Blockhain, we need to fine tune message frequency in the gossipsub channel