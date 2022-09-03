package types

import (
	"context"
	"errors"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"time"
)

type CBlock struct {
	Table  string
	Batch  driver.Batch
	Blocks []Block
}

const CHBlock = `CREATE TABLE IF NOT EXISTS cblock (
		BodySize                 UInt32,
		Epoch                    UInt32 ,
		EpochSlot                UInt32,
		Era                      VARCHAR(16), 
		Hash                     FixedString(64), 
		IssuerVkey               FixedString(64),
		Number                   UInt64,  
		Slot                     UInt64,
		SlotLeader               VARCHAR(56), 
		TxCount                  UInt32,
		Fees                     UInt64,        
		TotalOutput              UInt64,          
		InputCount               UInt32,          
		OutputCount              UInt32,           
		MintCount                UInt32,        
		MetaCount                UInt32,            
		NativeWitnessesCount     UInt32,           
		PlutusDatumCount        UInt32,           
		PlutusRdmrCount         UInt32,         
		PlutusWitnessesCount     UInt32,        
		Cip25AssetCount       UInt32,            
		Cip20Count         UInt32,              
		PoolRegistrationCount    UInt32,        
		PoolRetirementCount   UInt32,           
		StakeDelegationCount    UInt32,    
		StakeRegistrationCount     UInt32,       
		StakeDeregistrationCount  UInt32,      
		Confirmations   UInt32,
		TimeStamp       DateTime64                            
	) ENGINE = MergeTree()
	PARTITION BY toYYYYMM(TimeStamp)
	ORDER BY (TimeStamp, Number);`

type Block struct {
	BodySize                 uint        `bson:"body_size" json:"body_size"`
	CborHex                  interface{} `bson:"cbor_hex" json:"cbor_hex"`
	Epoch                    uint        `bson:"epoch" json:"epoch"`
	EpochSlot                uint        `bson:"epoch_slot" json:"epoch_slot"`
	Era                      string      `bson:"era" json:"era"`
	Hash                     string      `bson:"hash" json:"hash"`
	IssuerVkey               string      `bson:"issuer_vkey" json:"issuer_vkey"`
	Number                   uint        `bson:"number" json:"number"`
	Slot                     uint        `bson:"slot" json:"slot"`
	SlotLeader               string      `json:"slot_leader" bson:"slot_leader"`
	TxCount                  uint        `bson:"tx_count" json:"tx_count"`
	Fees                     int64       `bson:"fees" json:"fees"`
	TotalOutput              int64       `bson:"total_output" json:"total_output"`
	InputCount               uint        `bson:"input_count" json:"input_count"`
	OutputCount              uint        `bson:"output_count" json:"output_count"`
	MintCount                int64       `bson:"mint_count" json:"mint_count"`
	MetaCount                uint        `bson:"metadata_count" json:"metadata_count"`
	NativeWitnessesCount     uint        `bson:"native_witnesses_count" json:"native_witnesses_count"`
	PlutusDatumCount         uint        `bson:"plutus_datum_count" json:"plutus_datum_count"`
	PlutusRdmrCount          uint        `bson:"plutus_redeemer_count" json:"plutus_redeemer_count"`
	PlutusWitnessesCount     uint        `bson:"plutus_witnesses_count" json:"plutus_witnesses_count"`
	Cip25AssetCount          uint        `bson:"cip25_asset_count" json:"cip25_asset_count"`
	Cip20Count               uint        `bson:"cip20_count" json:"cip20_count"`
	PoolRegistrationCount    uint        `bson:"pool_registration_count" json:"pool_registration_count"`
	PoolRetirementCount      uint        `bson:"pool_retirement_count" json:"pool_retirement_count"`
	StakeDelegationCount     uint        `bson:"stake_delegation_count" json:"stake_delegation_count"`
	StakeRegistrationCount   uint        `bson:"stake_registration_count" json:"stake_registration_count"`
	StakeDeregistrationCount uint        `bson:"stake_deregistration_count" json:"stake_deregistration_count"`
	Confirmations            uint        `json:"confirmations" bson:"confirmations"`
	TimeStamp                time.Time   `json:"time_stamp" bson:"time_stamp"`
}

func (cb *CBlock) Drop(conn clickhouse.Conn) (err error) {
	if cb.Table == "" {
		err = errors.New("table is missing")
		return
	}
	var query = "DROP TABLE IF EXISTS " + cb.Table
	return conn.Exec(context.Background(), query)
}

func (cb *CBlock) CreateTable(conn clickhouse.Conn) (err error) {
	return conn.Exec(context.Background(), CHBlock)
}

func (cb *CBlock) PrepareBatch(conn clickhouse.Conn) (err error) {
	var query = "INSERT INTO " + cb.Table
	cb.Batch, err = conn.PrepareBatch(context.Background(), query)
	return
}

func (cb *CBlock) BulkInsert(conn clickhouse.Conn) error {
	batch := cb.Batch
	for _, block := range cb.Blocks {
		batch.Append(block)
	}
	return batch.Send()
}
