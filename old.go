package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"math/big"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

var writer *csv.Writer

func writeCSV(transaction types.Transaction, block *types.Block) {

	from, err := types.Sender(types.LatestSignerForChainID(transaction.ChainId()), &transaction)
	if err != nil {
		log.Fatalf("Failed to get sender: %v", err)
	}

	var to string
	if transaction.To() != nil {
		to = transaction.To().Hex()
	}

	value := transaction.Value().String()

	t := time.Unix(int64(block.Time()), 0).String()

	writer.Write([]string{transaction.Hash().Hex(), from.Hex(), to, value, t})
}

func main() {
	file, err := os.Create("transactions.csv")
	if err != nil {
		log.Fatalf("Failed to create file: %v", err)
	}
	defer file.Close()

	writer = csv.NewWriter(file)
	defer writer.Flush()

	writer.Write([]string{"Transaction Hash", "From", "To", "Value", "Timestamp"})

	ethclient, err := ethclient.Dial("http://127.0.0.1:8545")
	if err != nil {
		panic(err)
	}

	startTime := time.Now()

	walletAddress := common.HexToAddress("0xe0cfe78cebeec4d2127a89b4cf0a0a77db4dec5b")
	startBlock := big.NewInt(0)
	endBlock := big.NewInt(6000)
	signer := types.LatestSignerForChainID(big.NewInt(222))

	for startBlock.Cmp(endBlock) != 0 {
		block, err := ethclient.BlockByNumber(context.Background(), startBlock)
		if err != nil {
			log.Fatalf("Failed to get block 0: %v", err)
		}

		transactions := block.Transactions()
		for _, tx := range transactions {
			from, err := types.Sender(signer, tx)
			if err != nil {
				log.Fatalf("Failed to get sender: %v", err)
			}

			if (tx.To() != nil && *tx.To() == walletAddress) || from == walletAddress {
				writeCSV(*tx, block)
				continue
			}
		}

		startBlock.Add(startBlock, big.NewInt(1))

	}

	endTime := time.Now()
	fmt.Printf("Execution Time: %v\n", endTime.Sub(startTime))
}
