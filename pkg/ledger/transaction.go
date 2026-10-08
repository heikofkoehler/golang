package ledger

import (
	"time"
)

// TxType classifies the financial operation.
type TxType string

const (
	TxTypeDeposit       TxType = "DEPOSIT"
	TxTypeWithdrawal    TxType = "WITHDRAWAL"
	TxTypeTransfer      TxType = "TRANSFER"
	TxTypeMultiTransfer TxType = "MULTI_TRANSFER"
)

// TxStatus tracks transaction settlement.
type TxStatus string

const (
	TxStatusCommitted  TxStatus = "COMMITTED"
	TxStatusRolledBack TxStatus = "ROLLED_BACK"
	TxStatusFailed     TxStatus = "FAILED"
)

// TxLeg specifies a transfer leg between two accounts.
type TxLeg struct {
	FromAccount string
	ToAccount   string
	Amount      int64
}

// TransactionRecord represents an immutable audit log entry.
type TransactionRecord struct {
	ID        string
	Type      TxType
	Status    TxStatus
	Legs      []TxLeg
	Timestamp time.Time
	Error     string
}
