# BitTorrent Client Implementation Flowchart

This document outlines the step-by-step process of how our BitTorrent client works, explaining each major component and its role in the implementation.

## 1. Parsing the .torrent File

```go
var torrent TorrentFile
err = bencode.Unmarshal(file, &torrent)
```

**Purpose**: Extract metadata from the .torrent file using bencode decoding.

**What Happens**:
- The .torrent file is a bencoded dictionary containing metadata
- It includes the tracker URL (announce), file information (name, length), and piece information
- The client uses the `bencode-go` library to parse this information into a structured format

## 2. Generating the Info Hash

```go
infoHash := sha1.New()
err = bencode.Marshal(infoHash, torrent.Info)
infoHashSum := infoHash.Sum(nil)
infoHashHex := hex.EncodeToString(infoHashSum)
```

**Purpose**: Create a unique identifier for the torrent based on its content.

**What Happens**:
- The info hash is a SHA-1 hash of the bencoded info dictionary
- This hash is used to identify the specific torrent to trackers and peers
- Peers use this to verify they're talking about the same file

## 3. Connecting to the Tracker

```go
params := url.Values{
    "info_hash":  {string(infoHashSum)},
    "peer_id":    {"-PC0001-123456789012"},
    "port":       {"6881"},
    "uploaded":   {"0"},
    "downloaded": {"0"},
    "left":       {fmt.Sprintf("%d", torrent.Info.Length)},
    "compact":    {"1"},
}
trackerURL := fmt.Sprintf("%s?%s", torrent.Announce, params.Encode())
resp, err := http.Get(trackerURL)
```

**Purpose**: Announce to the tracker and request a list of peers.

**What Happens**:
- The client constructs a request to the tracker URL specified in the torrent file
- It sends parameters including the info hash, a unique peer ID, and download status
- The compact=1 parameter requests peers in binary format to reduce response size

## 4. Processing Tracker Response

```go
var trackerResp TrackerResponse
err = bencode.Unmarshal(resp.Body, &trackerResp)
```

**Purpose**: Extract the list of peers to connect to.

**What Happens**:
- The tracker responds with a bencoded dictionary
- The response includes an interval (how often to re-contact the tracker) and a peers list
- With compact=1, peers are encoded as 6-byte entries (4 bytes for IP, 2 for port)
- The client decodes these into usable peer addresses

## 5. Connecting to Peers and Handshaking

```go
func handlePeerConnection(address, infoHash, peerID string, torrent TorrentFile, resultChan chan<- pieceResult, pieceQueue <-chan int) {
    conn, err := net.DialTimeout("tcp", address, 5*time.Second)
    
    handshake := createHandshake(infoHash, peerID)
    _, err = conn.Write(handshake)
    
    response := make([]byte, 68)
    _, err = io.ReadFull(conn, response)
```

**Purpose**: Establish connections with peers and perform the BitTorrent handshake.

**What Happens**:
- The client opens TCP connections to each peer
- It sends a handshake message containing:
  - Protocol identifier ("BitTorrent protocol")
  - Reserved bytes (for extensions)
  - The info hash (to identify the torrent)
  - The peer ID (to identify the client)
- It verifies the peer responds with a valid handshake

## 6. Peer Message Exchange

```go
// Send Interested message
err = sendInterested(conn)

// Wait for Unchoke message
err = waitForUnchoke(conn)
```

**Purpose**: Manage the BitTorrent peer protocol messaging.

**What Happens**:
- After handshaking, peers exchange messages to coordinate downloading
- The client sends an "interested" message to indicate it wants to download
- It waits for an "unchoke" message, which gives permission to request pieces
- This implements BitTorrent's tit-for-tat choking algorithm

## 7. Requesting and Downloading Pieces

```go
for begin := 0; begin < currentPieceLength; begin += blockSize {
    length := blockSize
    if begin+length > currentPieceLength {
        length = currentPieceLength - begin // Handle last block
    }

    // Request the block from the peer
    err = requestPiece(conn, pieceIndex, begin, length)
    
    // Receive the block from the peer
    block, err := receivePiece(conn, length)
    
    // Append the block to the piece buffer
    pieceBuffer = append(pieceBuffer, block...)
}
```

**Purpose**: Download the file piece by piece using multiple peers.

**What Happens**:
- The file is divided into pieces (typically 256KB to 1MB each)
- Each piece is further divided into blocks (typically 16KB each) for efficient transfer
- The client requests blocks from peers using "request" messages
- Peers respond with "piece" messages containing the requested data
- The client assembles blocks into complete pieces

## 8. Piece Validation

```go
// All blocks for the piece received, calculate SHA-1 hash
computedHash := sha1.Sum(pieceBuffer)
expectedHash := []byte(torrent.Info.Pieces[pieceIndex*20 : (pieceIndex+1)*20])

// Validate the piece
if bytes.Equal(computedHash[:], expectedHash) {
    // Piece is valid
} else {
    // Piece validation failed
}
```

**Purpose**: Ensure downloaded pieces are correct and uncorrupted.

**What Happens**:
- The .torrent file contains SHA-1 hashes of each piece in the "pieces" field
- After downloading a complete piece, the client calculates its SHA-1 hash
- It compares this with the expected hash from the torrent file
- If they match, the piece is valid; if not, it's re-downloaded

## 9. Work Distribution and Parallelization

```go
func downloadTorrent(torrent TorrentFile, infoHashHex string, peerID string, peers []string) error {
    // Create channels for work distribution and result collection
    resultChan := make(chan pieceResult)
    pieceQueues := make([]chan int, len(peers))
    
    // Start a goroutine for each peer
    for i, peer := range peers {
        pieceQueues[i] = make(chan int, 5) // Buffer for 5 pieces
        go handlePeerConnection(peer, infoHashHex, peerID, torrent, resultChan, pieceQueues[i])
    }
```

**Purpose**: Optimize download speed by using multiple peers simultaneously.

**What Happens**:
- The client distributes the download work across multiple peer connections
- Each peer is assigned different pieces to download
- Goroutines and channels manage concurrent downloading
- This implements BitTorrent's "rarest first" piece selection strategy

## 10. File Assembly

```go
// Write the pieces to a file
outputFile, err := os.Create(torrent.Info.Name)
if err != nil {
    return fmt.Errorf("error creating output file: %v", err)
}
defer outputFile.Close()

for _, piece := range pieces {
    if piece != nil {
        _, err = outputFile.Write(piece)
        if err != nil {
            return fmt.Errorf("error writing piece to file: %v", err)
        }
    }
}
```

**Purpose**: Assemble downloaded pieces into the final file.

**What Happens**:
- After all pieces are downloaded and validated, they're written to disk
- The pieces are written in order to create the complete file
- The output file name is taken from the torrent metadata
- This completes the download process

## Conclusion

This BitTorrent client implementation demonstrates the key components of the protocol:

1. Distributed downloading from multiple peers
2. Data integrity verification using SHA-1 hashes
3. Peer wire protocol including handshaking and messaging
4. Tracker communication for peer discovery
5. Parallel downloading for efficient bandwidth utilization

The implementation focuses on the core functionality while omitting some advanced features like DHT, PEX, upload throttling, and multi-file torrent support that might be found in more complete clients.

