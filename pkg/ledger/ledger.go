package ledger

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Ledger manages accounts and orchestrates transactional, deadlock-free transfers.
type Ledger struct {
	accountsMu sync.RWMutex
	accounts   map[string]*Account

	historyMu sync.RWMutex
	history   []*TransactionRecord
}

// NewLedger constructs a new banking ledger.
func NewLedger() *Ledger {
	return &Ledger{
		accounts: make(map[string]*Account),
		history:  make([]*TransactionRecord, 0),
	}
}

// CreateAccount registers a new account with the ledger.
func (l *Ledger) CreateAccount(id string, initialBalance int64) (*Account, error) {
	l.accountsMu.Lock()
	defer l.accountsMu.Unlock()

	if _, exists := l.accounts[id]; exists {
		return nil, ErrAccountExists
	}

	acc, err := NewAccount(id, initialBalance)
	if err != nil {
		return nil, err
	}

	l.accounts[id] = acc

	if initialBalance > 0 {
		l.recordTransaction(&TransactionRecord{
			ID:        newUUID(),
			Type:      TxTypeDeposit,
			Status:    TxStatusCommitted,
			Legs:      []TxLeg{{ToAccount: id, Amount: initialBalance}},
			Timestamp: time.Now(),
		})
	}

	return acc, nil
}

// GetAccount retrieves an account by ID.
func (l *Ledger) GetAccount(id string) (*Account, error) {
	l.accountsMu.RLock()
	defer l.accountsMu.RUnlock()

	acc, exists := l.accounts[id]
	if !exists {
		return nil, ErrAccountNotFound
	}
	return acc, nil
}

// Deposit credits an account directly.
func (l *Ledger) Deposit(accountID string, amount int64) (*TransactionRecord, error) {
	acc, err := l.GetAccount(accountID)
	if err != nil {
		return nil, err
	}

	acc.mu.Lock()
	err = acc.depositLocked(amount)
	acc.mu.Unlock()

	tx := &TransactionRecord{
		ID:        newUUID(),
		Type:      TxTypeDeposit,
		Status:    TxStatusCommitted,
		Legs:      []TxLeg{{ToAccount: accountID, Amount: amount}},
		Timestamp: time.Now(),
	}

	if err != nil {
		tx.Status = TxStatusFailed
		tx.Error = err.Error()
	}

	l.recordTransaction(tx)
	return tx, err
}

// Withdraw debits an account directly.
func (l *Ledger) Withdraw(accountID string, amount int64) (*TransactionRecord, error) {
	acc, err := l.GetAccount(accountID)
	if err != nil {
		return nil, err
	}

	acc.mu.Lock()
	err = acc.withdrawLocked(amount)
	acc.mu.Unlock()

	tx := &TransactionRecord{
		ID:        newUUID(),
		Type:      TxTypeWithdrawal,
		Status:    TxStatusCommitted,
		Legs:      []TxLeg{{FromAccount: accountID, Amount: amount}},
		Timestamp: time.Now(),
	}

	if err != nil {
		tx.Status = TxStatusFailed
		tx.Error = err.Error()
	}

	l.recordTransaction(tx)
	return tx, err
}

// Transfer atomically transfers funds from one account to another.
//
// Concurrency Trap Solved: Deadlock Prevention via Deterministic Lock Ordering.
// If Thread 1: Transfer(A, B) and Thread 2: Transfer(B, A), a naive implementation
// deadlocks when Thread 1 holds lock(A) waiting for B, while Thread 2 holds lock(B) waiting for A.
// We eliminate circular wait by sorting account IDs lexicographically and always acquiring
// locks in that deterministic sequence.
func (l *Ledger) Transfer(fromID, toID string, amount int64) (*TransactionRecord, error) {
	if fromID == toID {
		return nil, ErrSelfTransfer
	}
	if amount <= 0 {
		return nil, ErrNegativeAmount
	}

	// Lookup accounts under read lock
	l.accountsMu.RLock()
	fromAcc, fromExists := l.accounts[fromID]
	toAcc, toExists := l.accounts[toID]
	l.accountsMu.RUnlock()

	if !fromExists || !toExists {
		return nil, ErrAccountNotFound
	}

	// Deterministic Lock Ordering
	first, second := fromAcc, toAcc
	if fromID > toID {
		first, second = toAcc, fromAcc
	}

	first.mu.Lock()
	second.mu.Lock()
	defer second.mu.Unlock()
	defer first.mu.Unlock()

	tx := &TransactionRecord{
		ID:        newUUID(),
		Type:      TxTypeTransfer,
		Legs:      []TxLeg{{FromAccount: fromID, ToAccount: toID, Amount: amount}},
		Timestamp: time.Now(),
	}

	// Verify balance invariant before mutating
	if fromAcc.balance < amount {
		tx.Status = TxStatusRolledBack
		tx.Error = ErrInsufficientFunds.Error()
		l.recordTransaction(tx)
		return tx, fmt.Errorf("%w: balance %d < %d", ErrInsufficientFunds, fromAcc.balance, amount)
	}

	// Atomic transfer
	fromAcc.balance -= amount
	toAcc.balance += amount
	tx.Status = TxStatusCommitted

	l.recordTransaction(tx)
	return tx, nil
}

// MultiTransfer atomically transfers funds across an arbitrary number of accounts.
//
// Concurrency Trap Solved: Multi-party Deadlock & Atomic Rollback.
// 1. Gathers all unique account IDs.
// 2. Sorts IDs to enforce a global lock acquisition hierarchy.
// 3. Atomically verifies all solvency constraints before altering any balance.
// 4. Safely rolls back if any leg cannot be satisfied.
func (l *Ledger) MultiTransfer(legs []TxLeg) (*TransactionRecord, error) {
	if len(legs) == 0 {
		return nil, ErrInvalidTransaction
	}

	tx := &TransactionRecord{
		ID:        newUUID(),
		Type:      TxTypeMultiTransfer,
		Legs:      legs,
		Timestamp: time.Now(),
	}

	// Collect unique accounts and compute proposed net delta per account
	accountSet := make(map[string]struct{})
	netDeltas := make(map[string]int64)

	for _, leg := range legs {
		if leg.Amount <= 0 {
			tx.Status = TxStatusFailed
			tx.Error = "transfer leg amount must be positive"
			l.recordTransaction(tx)
			return tx, ErrNegativeAmount
		}
		if leg.FromAccount == leg.ToAccount {
			tx.Status = TxStatusFailed
			tx.Error = "cannot transfer to the same account"
			l.recordTransaction(tx)
			return tx, ErrSelfTransfer
		}

		accountSet[leg.FromAccount] = struct{}{}
		accountSet[leg.ToAccount] = struct{}{}

		netDeltas[leg.FromAccount] -= leg.Amount
		netDeltas[leg.ToAccount] += leg.Amount
	}

	// Fetch all accounts
	l.accountsMu.RLock()
	accountMap := make(map[string]*Account)
	for id := range accountSet {
		acc, exists := l.accounts[id]
		if !exists {
			l.accountsMu.RUnlock()
			tx.Status = TxStatusFailed
			tx.Error = fmt.Sprintf("account %s not found", id)
			l.recordTransaction(tx)
			return tx, ErrAccountNotFound
		}
		accountMap[id] = acc
	}
	l.accountsMu.RUnlock()

	// Sort account IDs lexicographically for deterministic lock ordering
	sortedIDs := make([]string, 0, len(accountSet))
	for id := range accountSet {
		sortedIDs = append(sortedIDs, id)
	}
	sort.Strings(sortedIDs)

	// Acquire locks in sorted order
	for _, id := range sortedIDs {
		accountMap[id].mu.Lock()
	}
	defer func() {
		// Unlock in reverse order
		for i := len(sortedIDs) - 1; i >= 0; i-- {
			accountMap[sortedIDs[i]].mu.Unlock()
		}
	}()

	// Atomic validation: verify that no account incurs a negative balance
	for id, delta := range netDeltas {
		acc := accountMap[id]
		if acc.balance+delta < 0 {
			tx.Status = TxStatusRolledBack
			tx.Error = fmt.Sprintf("insufficient funds for account %s: balance %d + delta %d < 0",
				id, acc.balance, delta)
			l.recordTransaction(tx)
			return tx, fmt.Errorf("%w for account %s", ErrInsufficientFunds, id)
		}
	}

	// Commit mutations atomically
	for id, delta := range netDeltas {
		accountMap[id].balance += delta
	}
	tx.Status = TxStatusCommitted

	l.recordTransaction(tx)
	return tx, nil
}

// TotalSystemBalance calculates the total funds across all accounts in the ledger.
// Locks all accounts in sorted order to provide a strictly consistent snapshot.
func (l *Ledger) TotalSystemBalance() int64 {
	l.accountsMu.RLock()
	var sortedIDs []string
	accMap := make(map[string]*Account, len(l.accounts))
	for id, acc := range l.accounts {
		sortedIDs = append(sortedIDs, id)
		accMap[id] = acc
	}
	l.accountsMu.RUnlock()

	sort.Strings(sortedIDs)

	for _, id := range sortedIDs {
		accMap[id].mu.RLock()
	}
	defer func() {
		for i := len(sortedIDs) - 1; i >= 0; i-- {
			accMap[sortedIDs[i]].mu.RUnlock()
		}
	}()

	var total int64
	for _, id := range sortedIDs {
		total += accMap[id].balance
	}
	return total
}

// History returns a copy of the ledger's transaction audit log.
func (l *Ledger) History() []*TransactionRecord {
	l.historyMu.RLock()
	defer l.historyMu.RUnlock()

	records := make([]*TransactionRecord, len(l.history))
	copy(records, l.history)
	return records
}

func (l *Ledger) recordTransaction(tx *TransactionRecord) {
	l.historyMu.Lock()
	l.history = append(l.history, tx)
	l.historyMu.Unlock()
}

func newUUID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
