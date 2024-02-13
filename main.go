package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"math/big"
	"os"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

var (
	ethClient *ethclient.Client
	signer    types.Signer
)

type TransactionData struct {
	Hash      string
	From      string
	To        string
	Value     string
	timestamp uint64
}

func fetchTransactions(startBlock, endBlock *big.Int, walletAddress common.Address, wg *sync.WaitGroup, txDataChan chan<- TransactionData) {
	defer wg.Done()

	for startBlock.Cmp(endBlock) <= 0 {
		block, err := ethClient.BlockByNumber(context.Background(), startBlock)
		if err != nil {
			log.Printf("Failed to get block %s: %v", startBlock, err)
			continue
		}

		for _, tx := range block.Transactions() {
			from, err := types.Sender(signer, tx)
			if err != nil {
				log.Printf("Failed to get sender: %v", err)
				continue
			}

			to := ""
			if tx.To() != nil {
				to = tx.To().Hex()
			}

			if from == walletAddress || to == walletAddress.Hex() {
				txDataChan <- TransactionData{
					Hash:      tx.Hash().Hex(),
					From:      from.Hex(),
					To:        to,
					Value:     tx.Value().String(),
					timestamp: block.Time(),
				}
			}
		}

		startBlock.Add(startBlock, big.NewInt(1))
	}
}

func writeCSV(txDataChan <-chan TransactionData, done <-chan bool) {
	file, err := os.Create("transactions.csv")
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{"Transaction Hash", "From", "To", "Value", "Timestamp"})

	for {
		select {
		case txData := <-txDataChan:
			if txData.Hash == "" {
				continue
			}
			t := time.Unix(int64(txData.timestamp), 0).UTC().Format(time.RFC3339)
			writer.Write([]string{txData.Hash, txData.From, txData.To, txData.Value, t})
		case <-done:
			return
		}
	}
}

func main() {
	var err error
	ethClient, err = ethclient.Dial("http://127.0.0.1:8545")
	if err != nil {
		log.Fatalf("Failed to connect to the Ethereum client: %v", err)
	}

	// Global Signer
	signer = types.LatestSignerForChainID(big.NewInt(222))

	walletAddress := common.HexToAddress(os.Args[1])
	startBlock := big.NewInt(0)
	endBlock := big.NewInt(6000)
	numWorkers := 10

	txDataChan := make(chan TransactionData, 1000)
	done := make(chan bool)

	var wg sync.WaitGroup
	blockSize := new(big.Int).Sub(endBlock, startBlock).Int64() / int64(numWorkers)

	// Start timing
	startTime := time.Now()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		segmentStart := new(big.Int).SetInt64(startBlock.Int64() + int64(i)*blockSize)
		segmentEnd := new(big.Int).SetInt64(segmentStart.Int64() + blockSize - 1)
		if i == numWorkers-1 {
			segmentEnd.Set(endBlock)
		}

		go fetchTransactions(segmentStart, segmentEnd, walletAddress, &wg, txDataChan)
	}

	go writeCSV(txDataChan, done)

	wg.Wait()
	close(txDataChan)
	done <- true

	// End timing
	endTime := time.Now()
	fmt.Printf("Execution Time: %v\n", endTime.Sub(startTime))
}
