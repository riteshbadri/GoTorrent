//package peerToPeer
//
//import (
//	"bytes"
//	"crypto/sha1"
//	"fmt"
//	"goTorrent/client"
//	"goTorrent/message"
//	"goTorrent/peers"
//	"log"
//	"net"
//	"runtime"
//	"time"
//)
//
//// MaxBlockSize is the largest number of bytes a request can ask for
//const MaxBlockSize = 16384
//
//// MaxBacklog is the number of unfulfilled requests a client can have in its pipeline
//const MaxBacklog = 5
//
//// Torrent holds data required to download a torrent from a list of peers
//
//type Torrent struct {
//	Peers       []peers.Peer
//	PeerID      [20]byte
//	InfoHash    [20]byte
//	PieceHashes [][20]byte
//	PieceLength int
//	Length      int
//	Name        string
//}
//
//type pieceWork struct {
//	index  int
//	hash   [20]byte
//	length int
//}
//
//type pieceResult struct {
//	index int
//	buf   []byte
//}
//
//type pieceProgress struct {
//	index      int
//	client     *client.Client
//	buf        []byte
//	downloaded int
//	requested  int
//	backlog    int
//}
//
//func (state *pieceProgress) readMessage() error {
//	msg, err := state.client.Read() // this call blocks
//	if err != nil {
//		return err
//	}
//
//	if msg == nil { // keep-alive
//		return nil
//	}
//
//	switch msg.ID {
//	case message.MsgUnchoke:
//		state.client.Choked = false
//	case message.MsgChoke:
//		state.client.Choked = true
//	case message.MsgHave:
//		index, err := message.ParseHave(msg)
//		if err != nil {
//			return err
//		}
//		state.client.Bitfield.SetPiece(index)
//	case message.MsgPiece:
//		n, err := message.ParsePiece(state.index, state.buf, msg)
//		if err != nil {
//			return err
//		}
//		state.downloaded += n
//		state.backlog--
//	}
//	return nil
//}
//
//func attemptDownloadPiece(c *client.Client, pw *pieceWork) ([]byte, error) {
//	state := pieceProgress{
//		index:  pw.index,
//		client: c,
//		buf:    make([]byte, pw.length),
//	}
//
//	// Setting a deadline helps get unresponsive peers unstuck.
//	// 30 seconds is more than enough time to download a 262 KB piece
//	err := c.Conn.SetDeadline(time.Now().Add(30 * time.Second))
//	if err != nil {
//		return nil, err
//	}
//	defer func(Conn net.Conn, t time.Time) {
//		err := Conn.SetDeadline(t)
//		if err != nil {
//
//		}
//	}(c.Conn, time.Time{}) // Disable the deadline
//
//	for state.downloaded < pw.length {
//		// If unchoked, send requests until we have enough unfulfilled requests
//		if !state.client.Choked {
//			for state.backlog < MaxBacklog && state.requested < pw.length {
//				blockSize := MaxBlockSize
//				// Last block might be shorter than the typical block
//				if pw.length-state.requested < blockSize {
//					blockSize = pw.length - state.requested
//				}
//
//				err := c.SendRequest(pw.index, state.requested, blockSize)
//				if err != nil {
//					return nil, err
//				}
//				state.backlog++
//				state.requested += blockSize
//			}
//		}
//
//		err := state.readMessage()
//		if err != nil {
//			return nil, err
//		}
//	}
//
//	return state.buf, nil
//}
//
//func checkIntegrity(pw *pieceWork, buf []byte) error {
//	hash := sha1.Sum(buf)
//	if !bytes.Equal(hash[:], pw.hash[:]) {
//		return fmt.Errorf("Index %d failed integrity check", pw.index)
//	}
//	return nil
//}
//
//func (t *Torrent) startDownloadWorker(peer peers.Peer, workQueue chan *pieceWork, results chan *pieceResult) {
//	c, err := client.New(peer, t.PeerID, t.InfoHash)
//	if err != nil {
//		log.Printf("Could not handshake with %s. Disconnecting\n", peer.IP)
//		return
//	}
//	defer c.Conn.Close()
//	log.Printf("Completed handshake with %s\n", peer.IP)
//
//	c.SendUnchoke()
//	c.SendInterested()
//
//	for pw := range workQueue {
//		if !c.Bitfield.HasPiece(pw.index) {
//			workQueue <- pw // Put piece back on the queue
//			continue
//		}
//
//		// Download the piece
//		buf, err := attemptDownloadPiece(c, pw)
//		if err != nil {
//			log.Println("Exiting", err)
//			workQueue <- pw // Put piece back on the queue
//			return
//		}
//
//		err = checkIntegrity(pw, buf)
//		if err != nil {
//			log.Printf("Piece #%d failed integrity check\n", pw.index)
//			workQueue <- pw // Put piece back on the queue
//			continue
//		}
//
//		c.SendHave(pw.index)
//		results <- &pieceResult{pw.index, buf}
//	}
//}
//
//func (t *Torrent) calculateBoundsForPiece(index int) (begin int, end int) {
//	begin = index * t.PieceLength
//	end = begin + t.PieceLength
//	if end > t.Length {
//		end = t.Length
//	}
//	return begin, end
//}
//
//func (t *Torrent) calculatePieceSize(index int) int {
//	begin, end := t.calculateBoundsForPiece(index)
//	return end - begin
//}
//
//// Download downloads the torrent. This stores the entire file in memory.
//
//func (t *Torrent) Download() ([]byte, error) {
//	log.Println("Starting download for", t.Name)
//	// Init queues for workers to retrieve work and send results
//	workQueue := make(chan *pieceWork, len(t.PieceHashes))
//	results := make(chan *pieceResult)
//	for index, hash := range t.PieceHashes {
//		length := t.calculatePieceSize(index)
//		workQueue <- &pieceWork{index, hash, length}
//	}
//
//	// Start workers
//	for _, peer := range t.Peers {
//		go t.startDownloadWorker(peer, workQueue, results)
//	}
//
//	// Collect results into a buffer until full
//	buf := make([]byte, t.Length)
//	donePieces := 0
//	for donePieces < len(t.PieceHashes) {
//		res := <-results
//		begin, end := t.calculateBoundsForPiece(res.index)
//		copy(buf[begin:end], res.buf)
//		donePieces++
//
//		percent := float64(donePieces) / float64(len(t.PieceHashes)) * 100
//		numWorkers := runtime.NumGoroutine() - 1 // subtract 1 for main thread
//		log.Printf("(%0.2f%%) Downloaded piece #%d from %d peers\n", percent, res.index, numWorkers)
//	}
//	close(workQueue)
//
//	return buf, nil
//}

package peerToPeer

import (
	"bytes"
	"context"
	"crypto/sha1"
	"fmt" // Import fmt for direct printing
	"goTorrent/client"
	"goTorrent/message"
	"goTorrent/peers"
	"log"
	"runtime"
	"sync"
	"time"
)

// MaxBlockSize is the largest number of bytes a request can ask for
const MaxBlockSize = 16384

// MaxBacklog is the number of unfulfilled requests a client can have in its pipeline
const MaxBacklog = 5

// Torrent holds data required to download a torrent from a list of peers
type Torrent struct {
	Peers       []peers.Peer
	PeerID      [20]byte
	InfoHash    [20]byte
	PieceHashes [][20]byte
	PieceLength int
	Length      int
	Name        string
}

type pieceWork struct {
	index  int
	hash   [20]byte
	length int
}

type pieceResult struct {
	index int
	buf   []byte
	err   error // Add an error field
}

type pieceProgress struct {
	index      int
	client     *client.Client
	buf        []byte
	downloaded int
	requested  int
	backlog    int
}

func (state *pieceProgress) readMessage() error {
	msg, err := state.client.Read() // this call blocks
	if err != nil {
		return err
	}

	if msg == nil { // keep-alive
		return nil
	}

	switch msg.ID {
	case message.MsgUnchoke:
		state.client.Choked = false
	case message.MsgChoke:
		state.client.Choked = true
	case message.MsgHave:
		index, err := message.ParseHave(msg)
		if err != nil {
			return err
		}
		state.client.Bitfield.SetPiece(index)
	case message.MsgPiece:
		n, err := message.ParsePiece(state.index, state.buf, msg)
		if err != nil {
			return err
		}
		state.downloaded += n
		state.backlog--
	}
	return nil
}

func attemptDownloadPiece(c *client.Client, pw *pieceWork) ([]byte, error) {
	state := pieceProgress{
		index:  pw.index,
		client: c,
		buf:    make([]byte, pw.length),
	}

	err := c.Conn.SetDeadline(time.Now().Add(30 * time.Second))
	if err != nil {
		return nil, err
	}
	// Correctly defer clearing the deadline
	defer func() {
		if err := c.Conn.SetDeadline(time.Time{}); err != nil {
			log.Printf("Error clearing deadline for %s: %v\n", c.Peer.IP, err)
		}
	}()

	for state.downloaded < pw.length {
		if !state.client.Choked {
			for state.backlog < MaxBacklog && state.requested < pw.length {
				blockSize := MaxBlockSize
				if pw.length-state.requested < blockSize {
					blockSize = pw.length - state.requested
				}

				err := c.SendRequest(pw.index, state.requested, blockSize)
				if err != nil {
					return nil, err
				}
				state.backlog++
				state.requested += blockSize
			}
		}

		err := state.readMessage()
		if err != nil {
			return nil, err
		}
	}

	return state.buf, nil
}

func checkIntegrity(pw *pieceWork, buf []byte) error {
	hash := sha1.Sum(buf)
	if !bytes.Equal(hash[:], pw.hash[:]) {
		return fmt.Errorf("Index %d failed integrity check", pw.index)
	}
	return nil
}

func (t *Torrent) startDownloadWorker(ctx context.Context, peer peers.Peer, pieceRequests chan *pieceWork, pieceResults chan *pieceResult, wg *sync.WaitGroup) {
	defer wg.Done()

	// Using fmt.Printf for immediate output to debug if log.Printf is buffered
	fmt.Printf("Worker for %s: STARTING. Attempting to connect to peer...\n", peer.IP)
	c, err := client.New(peer, t.PeerID, t.InfoHash)
	if err != nil {
		fmt.Printf("Worker for %s: ERROR connecting/handshaking: %v. Disconnecting.\n", peer.IP, err)
		return
	}
	defer c.Conn.Close()
	fmt.Printf("Worker for %s: Completed handshake. Peer info: %+v\n", peer.IP, c.Peer) // Log peer info too

	// The rest of the log.Printf calls can remain, as the primary connection issue is at the start
	log.Printf("Worker for %s: Sending unchoke and interested.\n", peer.IP)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Worker for %s shutting down due to context cancellation.\n", peer.IP)
			return
		case pw, ok := <-pieceRequests:
			if !ok {
				log.Printf("Worker for %s: Piece request channel closed. Shutting down.\n", peer.IP)
				return
			}

			if !c.Bitfield.HasPiece(pw.index) {
				log.Printf("Worker for %s: Peer does not have piece #%d. Reporting failure.\n", peer.IP, pw.index)
				select { // Use select to avoid blocking on pieceResults if context is done
				case pieceResults <- &pieceResult{index: pw.index, err: fmt.Errorf("peer does not have piece")}:
				case <-ctx.Done():
					log.Printf("Worker for %s: Context cancelled while reporting 'peer does not have piece' for #%d.\n", peer.IP, pw.index)
					return
				}
				continue
			}

			log.Printf("Worker for %s: Attempting to download piece #%d (length %d).\n", peer.IP, pw.index, pw.length)
			buf, err := attemptDownloadPiece(c, pw)
			if err != nil {
				log.Printf("Worker for %s: Error downloading piece #%d: %v. Reporting failure.\n", peer.IP, pw.index, err)
				select { // Use select to avoid blocking on pieceResults if context is done
				case pieceResults <- &pieceResult{index: pw.index, err: fmt.Errorf("download failed: %w", err)}:
				case <-ctx.Done():
					log.Printf("Worker for %s: Context cancelled while reporting download failure for #%d.\n", peer.IP, pw.index)
					return
				}
				continue
			}

			err = checkIntegrity(pw, buf)
			if err != nil {
				log.Printf("Worker for %s: Piece #%d failed integrity check. Reporting failure.\n", peer.IP, pw.index)
				select { // Use select to avoid blocking on pieceResults if context is done
				case pieceResults <- &pieceResult{index: pw.index, err: fmt.Errorf("integrity check failed: %w", err)}:
				case <-ctx.Done():
					log.Printf("Worker for %s: Context cancelled while reporting integrity failure for #%d.\n", peer.IP, pw.index)
					return
				}
				continue
			}

			c.SendHave(pw.index)
			log.Printf("Worker for %s: Successfully downloaded and verified piece #%d.\n", peer.IP, pw.index)
			select { // Use select to avoid blocking on pieceResults if context is done
			case pieceResults <- &pieceResult{index: pw.index, buf: buf}:
			case <-ctx.Done():
				log.Printf("Worker for %s: Context cancelled while sending successful result for #%d.\n", peer.IP, pw.index)
				return
			}
		}
	}
}

func (t *Torrent) calculateBoundsForPiece(index int) (begin int, end int) {
	begin = index * t.PieceLength
	end = begin + t.PieceLength
	if end > t.Length {
		end = t.Length
	}
	return begin, end
}

func (t *Torrent) calculatePieceSize(index int) int {
	begin, end := t.calculateBoundsForPiece(index)
	return end - begin
}

// Download downloads the torrent. This stores the entire file in memory.
func (t *Torrent) Download() ([]byte, error) {
	log.Println("Starting download for", t.Name)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Ensure the context is cancelled when Download exits

	// 1. Make pieceRequests a buffered channel to prevent initial assignment from blocking.
	pieceRequests := make(chan *pieceWork, len(t.PieceHashes))
	pieceResults := make(chan *pieceResult)

	var wg sync.WaitGroup

	for _, peer := range t.Peers {
		wg.Add(1)
		go t.startDownloadWorker(ctx, peer, pieceRequests, pieceResults, &wg)
	}

	// This goroutine waits for all workers to shut down.
	go func() {
		wg.Wait()
		log.Println("All worker goroutines have called Done().")
		// After all workers are done, if the piece manager hasn't finished,
		// we might need to signal it to shut down if there's no progress.
		// The piece manager will also check ctx.Done() and handle this.
	}()

	downloadedBuf := make([]byte, t.Length)
	piecesNeeded := make(map[int]struct{})
	pendingRequests := make(map[int]struct{})
	var mu sync.Mutex // Mutex for protecting shared state (piecesNeeded, pendingRequests, piecesCompleted, downloadErr)

	for i := range t.PieceHashes {
		piecesNeeded[i] = struct{}{}
	}

	totalPieces := len(t.PieceHashes)
	piecesCompleted := 0
	downloadErr := (error)(nil)

	// --- Piece Manager Goroutine ---
	go func() {
		defer func() {
			log.Println("Piece manager shutting down. Closing pieceRequests and pieceResults.")
			close(pieceRequests) // No more pieces will be requested
			close(pieceResults)  // Signal main goroutine that no more results will come
		}()

		// Populate initial work queue directly. Since pieceRequests is buffered, this won't block.
		for index := range t.PieceHashes {
			select {
			case <-ctx.Done():
				log.Println("Initial piece assignment interrupted by context cancellation.")
				return
			default:
				// Continue to send if not cancelled
			}

			mu.Lock()
			if _, stillNeeded := piecesNeeded[index]; stillNeeded {
				if _, isPending := pendingRequests[index]; !isPending {
					pieceRequests <- &pieceWork{
						index:  index,
						hash:   t.PieceHashes[index],
						length: t.calculatePieceSize(index),
					}
					pendingRequests[index] = struct{}{}
				}
			}
			mu.Unlock()
		}
		log.Println("All initial pieces dispatched to work queue.")

		// Start a separate goroutine to periodically re-assign pending pieces
		// that might have failed or weren't picked up by workers.
		go func() {
			for {
				select {
				case <-ctx.Done():
					log.Println("Piece re-assignment goroutine stopping due to context cancellation.")
					return
				case <-time.After(1 * time.Second): // Periodically check
					mu.Lock()
					if piecesCompleted == totalPieces {
						mu.Unlock()
						return
					}

					// If workers have finished and no pieces are completed, then something is wrong.
					// This relies on the main goroutine calling cancel() after wg.Wait() finishes if pieces are not done.
					// The primary exit for this goroutine is ctx.Done().

					for index := range piecesNeeded {
						if _, isPending := pendingRequests[index]; !isPending {
							select {
							case pieceRequests <- &pieceWork{
								index:  index,
								hash:   t.PieceHashes[index],
								length: t.calculatePieceSize(index),
							}:
								pendingRequests[index] = struct{}{}
							case <-ctx.Done():
								log.Println("Piece re-assignment: Context cancelled during re-assignment attempt.")
								mu.Unlock()
								return
							default:
								// Cannot send right now (channel full). Will try again later.
							}
						}
					}
					mu.Unlock()
				}
			}
		}()

		for {
			select {
			case <-ctx.Done():
				log.Println("Piece manager: Context cancelled, stopping piece management.")
				mu.Lock()
				// If we exit due to cancellation, ensure downloadErr reflects it if no other error occurred
				if downloadErr == nil {
					downloadErr = ctx.Err()
				}
				mu.Unlock()
				return
			case res, ok := <-pieceResults:
				if !ok {
					log.Println("Piece manager: pieceResults channel closed externally. This should not happen if manager is in control.")
					mu.Lock()
					downloadErr = fmt.Errorf("results channel closed prematurely")
					mu.Unlock()
					cancel() // Signal main Download to stop
					return
				}

				mu.Lock()
				if res.err != nil {
					log.Printf("Received error for piece #%d: %v.\n", res.index, res.err)
					// If the piece was pending and still needed, re-queue it
					if _, wasPending := pendingRequests[res.index]; wasPending {
						delete(pendingRequests, res.index)
						if _, stillNeeded := piecesNeeded[res.index]; stillNeeded {
							select {
							case pieceRequests <- &pieceWork{
								index:  res.index,
								hash:   t.PieceHashes[res.index],
								length: t.calculatePieceSize(res.index),
							}:
								pendingRequests[res.index] = struct{}{}
								log.Printf("Re-queued piece #%d after failure.\n", res.index)
							case <-ctx.Done():
								log.Println("Piece manager: Context cancelled while re-queuing failed piece. Exiting.")
								mu.Unlock()
								return
							default:
								log.Printf("Piece manager: Could not immediately re-queue piece #%d. Will retry later.\n", res.index)
							}
						} else {
							log.Printf("Piece #%d failed, but already completed by another worker or not needed. Ignoring failure for re-queue.\n", res.index)
						}
					}

				} else { // Successful download
					if _, stillNeeded := piecesNeeded[res.index]; !stillNeeded {
						log.Printf("Ignoring duplicate successful piece #%d result (already completed).\n", res.index)
						delete(pendingRequests, res.index)
						mu.Unlock()
						continue
					}

					delete(piecesNeeded, res.index)
					delete(pendingRequests, res.index)
					piecesCompleted++

					begin, end := t.calculateBoundsForPiece(res.index)
					copy(downloadedBuf[begin:end], res.buf)

					percent := float64(piecesCompleted) / float64(totalPieces) * 100
					numWorkers := runtime.NumGoroutine() - 1
					log.Printf("(%0.2f%%) Downloaded piece #%d from %d peers. %d pieces remaining.\n", percent, res.index, numWorkers, len(piecesNeeded))

					if piecesCompleted == totalPieces {
						mu.Unlock()
						cancel() // Signal all other goroutines to shut down
						log.Println("Piece manager: All pieces downloaded. Signaling shutdown.")
						return
					}
				}
				mu.Unlock()
			}
		}
	}()

	// The main goroutine waits for all workers to finish.
	// This wg.Wait() is crucial to know when no more active workers are expected.
	wg.Wait()
	log.Println("All workers have finished their execution and `wg.Wait()` completed.")

	// --- CRITICAL ADDITION ---
	// If all workers are done, but not all pieces are completed,
	// it means something went wrong (e.g., all peers failed to connect or provide pieces).
	// In this scenario, we must explicitly signal the piece manager to shut down.
	mu.Lock()
	currentPiecesCompleted := piecesCompleted
	currentTotalPieces := totalPieces
	mu.Unlock()

	if currentPiecesCompleted != currentTotalPieces {
		log.Println("Workers finished, but not all pieces are completed. Signalling piece manager to shut down via context cancellation.")
		cancel() // This will cause the piece manager to exit its select loop via ctx.Done()
	}
	// --- END CRITICAL ADDITION ---

	// The main goroutine then waits for the piece manager to close `pieceResults`.
	// This loop will now correctly unblock once the piece manager's defer runs.
	for range pieceResults {
		// Draining the channel until it's closed.
	}

	mu.Lock()
	finalPiecesCompleted := piecesCompleted
	finalDownloadErr := downloadErr
	mu.Unlock()

	if finalPiecesCompleted != totalPieces {
		if finalDownloadErr != nil {
			log.Printf("Download terminated with error: %v\n", finalDownloadErr)
			return downloadedBuf, finalDownloadErr
		}
		log.Printf("Download incomplete: expected %d pieces, got %d.\n", totalPieces, finalPiecesCompleted)
		return downloadedBuf, fmt.Errorf("download incomplete: expected %d pieces, got %d", totalPieces, finalPiecesCompleted)
	}

	log.Println("Torrent download complete.")
	return downloadedBuf, nil
}
