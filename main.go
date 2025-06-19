package main

import (
	"goTorrent/torrentFiles"
	"log"
)

//var wg sync.WaitGroup

func main() {
	inPath := "debian-12.11.0-amd64-netinst.iso.torrent"
	//inPath := os.Args[1]
	outPath := "debian.iso"
	//outPath := os.Args[2]

	//wg.Add(1)

	tf, err := torrentFiles.Open(inPath)
	if err != nil {
		log.Fatal(err)
	}

	err = tf.DownloadToFile(outPath)
	if err != nil {
		log.Fatal(err)
	}

	//wg.Wait()
	//fmt.Println("Waiting for all threads to finish")

}
