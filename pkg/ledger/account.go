package ledger

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrNegativeAmount     = errors.New("amount must be strictly positive")
	ErrInsufficientFunds  = errors.New("insufficient funds for withdrawal")
	ErrAccountNotFound    = errors.New("account not found")
	ErrAccountExists      = errors.New("account already exists")
	ErrSelfTransfer       = errors.New("cannot transfer to the same account")
	ErrInvalidTransaction = errors.New("invalid transaction specification")
)

// Account models a single banking customer account.
//
// Concurrency Design:
// Fine-grained per-account locking prevents coarse global ledger contention.
// Balance is stored as int64 representing currency in smallest divisible unit (cents).
type Account struct {
	id      string
	mu      sync.RWMutex
	balance int64
}

// NewAccount creates an account with an initial balance.
func NewAccount(id string, initialBalance int64) (*Account, error) {
	if initialBalance < 0 {
		return nil, ErrNegativeAmount
	}
	return &Account{
		id:      id,
		balance: initialBalance,
	}, nil
}

// ID returns the account identifier.
func (a *Account) ID() string {
	return a.id
}

// Balance returns current account balance thread-safely.
func (a *Account) Balance() int64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.balance
}

// Deposit credits the account with amount. Must be called while holding a.mu.Lock().
func (a *Account) depositLocked(amount int64) error {
	if amount <= 0 {
		return ErrNegativeAmount
	}
	a.balance += amount
	return nil
}

// Withdraw debits the account with amount. Must be called while holding a.mu.Lock().
//
// Concurrency Trap Solved: Balance Invariant Violation.
// Atomically verifies balance >= amount before debiting.
func (a *Account) withdrawLocked(amount int64) error {
	if amount <= 0 {
		return ErrNegativeAmount
	}
	if a.balance < amount {
		return fmt.Errorf("%w: balance %d < withdrawal %d", ErrInsufficientFunds, a.balance, amount)
	}
	a.balance -= amount
	return nil
}
