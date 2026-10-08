package ledger

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLedger_DeadlockFreedom_BidirectionalTransfers(t *testing.T) {
	// Concurrency Trap: Thread 1: A -> B, Thread 2: B -> A
	// Without deterministic lock ordering, this deadlocks immediately.
	l := NewLedger()

	accA, _ := l.CreateAccount("acc-A", 100_000)
	accB, _ := l.CreateAccount("acc-B", 100_000)

	initialTotal := accA.Balance() + accB.Balance()

	const iterations = 500
	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: transfers A -> B
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_, _ = l.Transfer("acc-A", "acc-B", 10)
		}
	}()

	// Goroutine 2: transfers B -> A
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_, _ = l.Transfer("acc-B", "acc-A", 10)
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Succeeded without deadlock!
	case <-time.After(3 * time.Second):
		t.Fatalf("Deadlock detected during bidirectional transfers!")
	}

	finalTotal := accA.Balance() + accB.Balance()
	if initialTotal != finalTotal {
		t.Fatalf("Money conservation violated: initial %d != final %d", initialTotal, finalTotal)
	}
}

func TestLedger_BalanceInvariant_NoDoubleSpendOrNegativeBalance(t *testing.T) {
	// Account with 100 cents. 20 concurrent goroutines attempt to withdraw 100 cents each.
	// Exactly ONE must succeed; 19 must fail. Balance must end at 0, never negative.
	l := NewLedger()
	acc, _ := l.CreateAccount("target-acc", 100)

	const goroutines = 20
	var successfulWithdrawals atomic.Int64
	var failedWithdrawals atomic.Int64

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_, err := l.Withdraw("target-acc", 100)
			if err == nil {
				successfulWithdrawals.Add(1)
			} else {
				failedWithdrawals.Add(1)
			}
		}()
	}

	wg.Wait()

	if successfulWithdrawals.Load() != 1 {
		t.Errorf("expected exactly 1 successful withdrawal, got %d", successfulWithdrawals.Load())
	}
	if failedWithdrawals.Load() != goroutines-1 {
		t.Errorf("expected %d failed withdrawals, got %d", goroutines-1, failedWithdrawals.Load())
	}
	if acc.Balance() != 0 {
		t.Errorf("expected balance to be exactly 0, got %d", acc.Balance())
	}
}

func TestLedger_MultiTransfer_AtomicRollbackOnPartialFailure(t *testing.T) {
	// Scenario: Account A has 100.
	// Legs:
	// Leg 1: A -> B (60)
	// Leg 2: A -> C (60)
	// Total needed from A is 120, but A only has 100.
	// Entire transaction must rollback atomically; neither B nor C must receive a cent.
	l := NewLedger()
	accA, _ := l.CreateAccount("acc-A", 100)
	accB, _ := l.CreateAccount("acc-B", 50)
	accC, _ := l.CreateAccount("acc-C", 50)

	legs := []TxLeg{
		{FromAccount: "acc-A", ToAccount: "acc-B", Amount: 60},
		{FromAccount: "acc-A", ToAccount: "acc-C", Amount: 60},
	}

	tx, err := l.MultiTransfer(legs)
	if err == nil {
		t.Fatalf("expected MultiTransfer to fail due to insufficient funds, got nil")
	}

	if tx.Status != TxStatusRolledBack {
		t.Errorf("expected status ROLLED_BACK, got %s", tx.Status)
	}

	// Verify all balances remain unchanged
	if accA.Balance() != 100 {
		t.Errorf("account A balance modified on rollback: %d", accA.Balance())
	}
	if accB.Balance() != 50 {
		t.Errorf("account B balance modified on rollback: %d", accB.Balance())
	}
	if accC.Balance() != 50 {
		t.Errorf("account C balance modified on rollback: %d", accC.Balance())
	}
}

func TestLedger_MultiPartyTransfer_Success(t *testing.T) {
	l := NewLedger()
	accA, _ := l.CreateAccount("acc-A", 200)
	accB, _ := l.CreateAccount("acc-B", 50)
	accC, _ := l.CreateAccount("acc-C", 50)

	legs := []TxLeg{
		{FromAccount: "acc-A", ToAccount: "acc-B", Amount: 60},
		{FromAccount: "acc-A", ToAccount: "acc-C", Amount: 40},
	}

	tx, err := l.MultiTransfer(legs)
	if err != nil {
		t.Fatalf("multi-transfer failed: %v", err)
	}
	if tx.Status != TxStatusCommitted {
		t.Errorf("expected status COMMITTED, got %s", tx.Status)
	}

	if accA.Balance() != 100 {
		t.Errorf("expected A to have 100, got %d", accA.Balance())
	}
	if accB.Balance() != 110 {
		t.Errorf("expected B to have 110, got %d", accB.Balance())
	}
	if accC.Balance() != 90 {
		t.Errorf("expected C to have 90, got %d", accC.Balance())
	}
}

func TestLedger_HighConcurrentNPartyTransfers_TotalConservation(t *testing.T) {
	// 10 accounts with 10,000 each. 50 goroutines executing random transfers concurrently.
	// Audits total system balance at the end.
	l := NewLedger()
	const numAccounts = 10
	const initialBalance = 10_000
	const expectedSystemTotal = int64(numAccounts * initialBalance)

	for i := 0; i < numAccounts; i++ {
		l.CreateAccount(fmt.Sprintf("user-%02d", i), initialBalance)
	}

	const workers = 30
	const opsPerWorker = 100
	var wg sync.WaitGroup
	wg.Add(workers)

	for w := 0; w < workers; w++ {
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))

			for i := 0; i < opsPerWorker; i++ {
				fromIdx := rng.Intn(numAccounts)
				toIdx := rng.Intn(numAccounts)
				if fromIdx == toIdx {
					continue
				}

				fromID := fmt.Sprintf("user-%02d", fromIdx)
				toID := fmt.Sprintf("user-%02d", toIdx)
				amount := int64(rng.Intn(50) + 1)

				_, _ = l.Transfer(fromID, toID, amount)
			}
		}(int64(w + 100))
	}

	wg.Wait()

	total := l.TotalSystemBalance()
	if total != expectedSystemTotal {
		t.Fatalf("Total system balance invariant violated! expected %d, got %d",
			expectedSystemTotal, total)
	}
}
