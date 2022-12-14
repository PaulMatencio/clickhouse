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
	BodySize                 uint32    `bson:"body_size" json:"body_size"`
	Epoch                    uint32    `bson:"epoch" json:"epoch"`
	EpochSlot                uint32    `bson:"epoch_slot" json:"epoch_slot"`
	Era                      string    `bson:"era" json:"era"`
	Hash                     string    `bson:"hash" json:"hash"`
	IssuerVkey               string    `bson:"issuer_vkey" json:"issuer_vkey"`
	Number                   uint64    `bson:"number" json:"number"`
	Slot                     uint64    `bson:"slot" json:"slot"`
	SlotLeader               string    `json:"slot_leader" bson:"slot_leader"`
	TxCount                  uint32    `bson:"tx_count" json:"tx_count"`
	Fees                     uint64    `bson:"fees" json:"fees"`
	TotalOutput              uint64    `bson:"total_output" json:"total_output"`
	InputCount               uint32    `bson:"input_count" json:"input_count"`
	OutputCount              uint32    `bson:"output_count" json:"output_count"`
	MintCount                uint64    `bson:"mint_count" json:"mint_count"`
	MetaCount                uint32    `bson:"metadata_count" json:"metadata_count"`
	NativeWitnessesCount     uint32    `bson:"native_witnesses_count" json:"native_witnesses_count"`
	PlutusDatumCount         uint32    `bson:"plutus_datum_count" json:"plutus_datum_count"`
	PlutusRdmrCount          uint32    `bson:"plutus_redeemer_count" json:"plutus_redeemer_count"`
	PlutusWitnessesCount     uint32    `bson:"plutus_witnesses_count" json:"plutus_witnesses_count"`
	Cip25AssetCount          uint32    `bson:"cip25_asset_count" json:"cip25_asset_count"`
	Cip20Count               uint32    `bson:"cip20_count" json:"cip20_count"`
	PoolRegistrationCount    uint32    `bson:"pool_registration_count" json:"pool_registration_count"`
	PoolRetirementCount      uint32    `bson:"pool_retirement_count" json:"pool_retirement_count"`
	StakeDelegationCount     uint32    `bson:"stake_delegation_count" json:"stake_delegation_count"`
	StakeRegistrationCount   uint32    `bson:"stake_registration_count" json:"stake_registration_count"`
	StakeDeregistrationCount uint32    `bson:"stake_deregistration_count" json:"stake_deregistration_count"`
	Confirmations            uint32    `json:"confirmations" bson:"confirmations"`
	Datetime                 time.Time `json:"datetime,omitempty" bson:"datetime,omitempty"`
}

/*
type Block struct {
	BodySize                 int       `bson:"body_size" json:"body_size"`
	Epoch                    int       `bson:"epoch" json:"epoch"`
	EpochSlot                int       `bson:"epoch_slot" json:"epoch_slot"`
	Era                      string    `bson:"era" json:"era"`
	Hash                     string    `bson:"hash" json:"hash"`
	IssuerVkey               string    `bson:"issuer_vkey" json:"issuer_vkey"`
	Number                   int       `bson:"number" json:"number"`
	Slot                     int       `bson:"slot" json:"slot"`
	SlotLeader               string    `json:"slot_leader" bson:"slot_leader"`
	TxCount                  int       `bson:"tx_count" json:"tx_count"`
	Fees                     int64     `bson:"fees" json:"fees"`
	TotalOutput              int64     `bson:"total_output" json:"total_output"`
	InputCount               int       `bson:"input_count" json:"input_count"`
	OutputCount              int       `bson:"output_count" json:"output_count"`
	MintCount                int64     `bson:"mint_count" json:"mint_count"`
	MetaCount                int       `bson:"metadata_count" json:"metadata_count"`
	NativeWitnessesCount     int       `bson:"native_witnesses_count" json:"native_witnesses_count"`
	PlutusDatumCount         int       `bson:"plutus_datum_count" json:"plutus_datum_count"`
	PlutusRdmrCount          int       `bson:"plutus_redeemer_count" json:"plutus_redeemer_count"`
	PlutusWitnessesCount     int       `bson:"plutus_witnesses_count" json:"plutus_witnesses_count"`
	Cip25AssetCount          int       `bson:"cip25_asset_count" json:"cip25_asset_count"`
	Cip20Count               int       `bson:"cip20_count" json:"cip20_count"`
	PoolRegistrationCount    int       `bson:"pool_registration_count" json:"pool_registration_count"`
	PoolRetirementCount      int       `bson:"pool_retirement_count" json:"pool_retirement_count"`
	StakeDelegationCount     int       `bson:"stake_delegation_count" json:"stake_delegation_count"`
	StakeRegistrationCount   int       `bson:"stake_registration_count" json:"stake_registration_count"`
	StakeDeregistrationCount int       `bson:"stake_deregistration_count" json:"stake_deregistration_count"`
	Confirmations            int       `json:"confirmations" bson:"confirmations"`
	Datetime                 time.Time `json:"datetime,omitempty" bson:"datetime,omitempty"`
}
*/

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
