package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/dfuse-io/bstream"
	pbcodec "github.com/dfuse-io/dfuse-eosio/pb/dfuse/eosio/codec/v1"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"github.com/sebastianmontero/dfuse-firehose-client/dfclient"
	"github.com/sebastianmontero/hypha-document-cache-go/dgraph"
	"github.com/sebastianmontero/hypha-document-cache-go/doccache"
	log "github.com/sirupsen/logrus"
)

var (
	contract  = os.Getenv("CONTRACT_NAME")
	docTable  = os.Getenv("DOC_TABLE_NAME")
	edgeTable = os.Getenv("EDGE_TABLE_NAME")
)

type deltaStreamHandler struct {
	cursor   string
	doccache *doccache.Doccache
}

func (m *deltaStreamHandler) OnDelta(delta *dfclient.TableDelta, cursor string, forkStep pbbstream.ForkStep) {
	fmt.Println("Cursor: ", cursor, "Fork Step:", forkStep, "\nOn Delta: ", delta)
	if delta.TableName == docTable {
		chainDoc := &doccache.ChainDocument{}
		switch delta.Operation {
		case pbcodec.DBOp_OPERATION_INSERT, pbcodec.DBOp_OPERATION_UPDATE:
			fmt.Println("Unmarshalling new data: ", string(delta.NewData))
			err := json.Unmarshal(delta.NewData, chainDoc)
			if err != nil {
				fmt.Println("Error unmarshalling doc new data: ", err)
				panic(fmt.Sprintln("Error unmarshalling doc new data: ", err))
			}
			fmt.Println("Storing doc: ", chainDoc)
			err = m.doccache.StoreDocument(chainDoc, cursor)
			if err != nil {
				fmt.Println("Failed to store doc: ", err)
				panic(fmt.Sprintln("Failed to store doc: ", err))
			}
		case pbcodec.DBOp_OPERATION_REMOVE:
			err := json.Unmarshal(delta.OldData, chainDoc)
			if err != nil {
				fmt.Println("Error unmarshalling doc old data: ", err)
				panic(fmt.Sprintln("Error unmarshalling doc old data: ", err))
			}
			err = m.doccache.DeleteDocument(chainDoc, cursor)
			if err != nil {
				fmt.Println("Failed to delete doc: ", err)
				panic(fmt.Sprintln("Failed to delete doc: ", err))
			}
		}
	} else if delta.TableName == edgeTable {
		switch delta.Operation {
		case pbcodec.DBOp_OPERATION_INSERT, pbcodec.DBOp_OPERATION_REMOVE:
			var (
				deltaData []byte
				deleteOp  bool
			)
			chainEdge := &doccache.ChainEdge{}
			if delta.Operation == pbcodec.DBOp_OPERATION_INSERT {
				deltaData = delta.NewData
			} else {
				deltaData = delta.OldData
				deleteOp = true
			}
			err := json.Unmarshal(deltaData, chainEdge)
			if err != nil {
				fmt.Println("Error unmarshalling edge data: ", err)
				// panic(fmt.Sprintln("Error unmarshalling edge data: ", err))
			}
			err = m.doccache.MutateEdge(chainEdge, deleteOp, cursor)
			if err != nil {
				fmt.Printf("Failed to mutate doc, deleteOp: %v, edge: %v, err: %v", deleteOp, chainEdge, err)
				// panic(fmt.Sprintf("Failed to mutate doc, deleteOp: %v, edge: %v, err: %v ", deleteOp, chainEdge, err))
			}
		case pbcodec.DBOp_OPERATION_UPDATE:
			panic(fmt.Sprintln("Edge updating is not handled: ", delta))
		}
	}
	m.cursor = cursor
}

func (m *deltaStreamHandler) OnError(err error) {
	fmt.Println("On Error: ", err)
}

func (m *deltaStreamHandler) OnComplete(lastBlockRef bstream.BlockRef) {
	fmt.Println("On Complete Last Block Ref: ", lastBlockRef)
}

func main() {
	firehoseEndpoint := os.Getenv("FIREHOSE_ENDPOINT")
	dfuseAPIKey := "server_eeb2882943ae420bfb3eb9bf3d78ed9d"
	eosEndpoint := os.Getenv("EOS_ENDPOINT")
	dgraphEndpoint := fmt.Sprintf("%v:%v", os.Getenv("DGRAPH_ALPHA_HOST"), os.Getenv("DGRAPH_ALPHA_EXTERNAL_PORT"))
	startBlock, err := strconv.ParseInt(os.Getenv("START_BLOCK"), 10, 64)
	if err != nil {
		panic(fmt.Sprintln("Unable to parse start block: ", err))
	}

	fmt.Printf(
		"Env Vars contract: %v \ndocTable: %v \nedgeTable: %v \nfirehoseEndpoint: %v \neosEndpoint: %v \n dgraphEndpoint: %v \n startBlock: %v",
		contract,
		docTable,
		edgeTable,
		firehoseEndpoint,
		eosEndpoint,
		dgraphEndpoint,
		startBlock,
	)

	client, err := dfclient.NewDfClient(firehoseEndpoint, dfuseAPIKey, eosEndpoint, log.InfoLevel)
	if err != nil {
		panic(fmt.Sprintln("Error creating dfclient: ", err))
	}
	dg, err := dgraph.New(dgraphEndpoint)
	if err != nil {
		panic(fmt.Sprintln("Error creating dgraph client: ", err))
	}
	cache, err := doccache.New(dg)
	if err != nil {
		panic(fmt.Sprintln("Error creating doccache client: ", err))
	}

	// client.BlockStream(&pbbstream.BlocksRequestV2{
	// 	StartBlockNum:     87822500,
	// 	StartCursor:       "",
	// 	StopBlockNum:      87823501,
	// 	ForkSteps:         []pbbstream.ForkStep{pbbstream.ForkStep_STEP_IRREVERSIBLE},
	// 	IncludeFilterExpr: "receiver in ['eosio.token']",
	// 	Details:           pbbstream.BlockDetails_BLOCK_DETAILS_FULL,
	// }, &blockStreamHandler{})
	deltaRequest := &dfclient.DeltaStreamRequest{
		StartBlockNum:  startBlock,
		StartCursor:    "",
		StopBlockNum:   0,
		ForkSteps:      []pbbstream.ForkStep{pbbstream.ForkStep_STEP_NEW, pbbstream.ForkStep_STEP_UNDO},
		ReverseUndoOps: true,
	}
	// deltaRequest.AddTables("eosio.token", []string{"balance"})
	deltaRequest.AddTables(contract, []string{docTable, edgeTable})
	client.DeltaStream(deltaRequest, &deltaStreamHandler{
		doccache: cache,
	})
}
