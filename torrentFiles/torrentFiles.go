package torrentFiles

import (
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"github.com/jackpal/bencode-go"
	"goTorrent/peerToPeer"
	"os"
)

const Port uint = 6881

type bencodeInfo struct {
	Pieces       string `bencode:"pieces"`
	PiecesLength int    `bencode:"pieces length"`
	Length       int    `bencode:"length"`
	Name         string `bencode:"name"`
}

type BencodeTorrent struct {
	Announce string      `bencode:"announce"`
	Info     bencodeInfo `bencode:"info"`
}

// TorrentFile consists of all the impotant aspects to identify a unique torrent
type TorrentFile struct {
	Announce    string
	InfoHash    [20]byte
	PieceHashes [][20]byte
	PieceLength int
	Length      int
	Name        string
}

// hash converts the Info of bencodeTorrent into an SHA-1 hash
func (i *bencodeInfo) hash() ([20]byte, error) {
	var buf bytes.Buffer
	// converting decoded info into bytes (bytes buffer used, [20]bytes because SHA-1 hash length is fixed 20 bytes)
	err := bencode.Marshal(&buf, *i)
	if err != nil {
		// if error return empty byte slice
		return [20]byte{}, err
	}
	// if no error, convert the byte slice into sha-1 hash. sha1.sum(bytes) appends the bytes to sha-1 form
	h := sha1.Sum(buf.Bytes())
	fmt.Println("sha1 sum", hex.EncodeToString(h[:]))
	return h, nil
}

func (i *bencodeInfo) splitPiecesHash() ([][20]byte, error) {
	// [][20]byte because each piece hash is of 20 bytes, and we dont know how many piece hasehs are there
	hashLen := 20 // length of each SHA-1 hash
	buf := []byte(i.Pieces)
	if (len(buf) % hashLen) != 0 {
		err := fmt.Errorf("received corrupted pieces of length %d", len(buf))
		return nil, err
	}
	numOfHashes := len(buf) / hashLen
	hashes := make([][20]byte, numOfHashes)
	for i := 0; i < numOfHashes; i++ {
		copy(hashes[i][:], buf[i*hashLen:(i+1)*hashLen])
	}
	fmt.Println("hashes", hashes)
	return hashes, nil
}

func (bto *BencodeTorrent) toTorrentfile() (TorrentFile, error) {
	// first parse infoHash
	infoHash, err := bto.Info.hash()
	if err != nil {
		// return empty torrent file
		return TorrentFile{}, err
	}
	// pieces is a string of all the piece hashes (in string format) with 20 bytes size of each piece hash.
	// time to hash pieces (split the pieces into slices with [20]bytes each.)
	piecesHash, err := bto.Info.splitPiecesHash()
	if err != nil {
		return TorrentFile{}, err
	}
	t := TorrentFile{
		Announce:    bto.Announce,
		InfoHash:    infoHash,
		PieceHashes: piecesHash,
		PieceLength: bto.Info.PiecesLength,
		Length:      bto.Info.Length,
		Name:        bto.Info.Name,
	}
	return t, nil
}

func (t *TorrentFile) DownloadToFile(path string) error {
	var peerID [20]byte
	_, err := rand.Read(peerID[:])
	if err != nil {
		return err
	}

	peers, err := t.requestPeers(peerID, uint16(Port))
	if err != nil {
		return err
	}

	torrent := peerToPeer.Torrent{
		Peers:       peers,
		PeerID:      peerID,
		InfoHash:    t.InfoHash,
		PieceHashes: t.PieceHashes,
		PieceLength: t.PieceLength,
		Length:      t.Length,
		Name:        t.Name,
	}
	buf, err := torrent.Download()
	if err != nil {
		return err
	}

	outFile, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func(outFile *os.File) {
		err := outFile.Close()
		if err != nil {

		}
	}(outFile)
	_, err = outFile.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func Open(path string) (TorrentFile, error) {
	file, err := os.Open(path)
	if err != nil {
		return TorrentFile{}, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	bto := BencodeTorrent{}
	err = bencode.Unmarshal(file, &bto)
	if err != nil {
		return TorrentFile{}, err
	}
	return bto.toTorrentfile()
}
