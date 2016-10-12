// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  // CPSC 638:
  //
  // Implement this method!
  return true;
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  // CPSC 638:
  //
  // Implement this method!
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 638:
  //
  // Implement this method!
  return UNLOCKED;
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  // CPSC 638:
  //
  // Implement this method!
	//
	// Pseudocode:
	// 	1) Make a new Lock Request for txn, with lock mode = write
	// 	2) Get the size of the lock_table_ deque
	// 	3) Add it to the lock_table_ deque
	// 	4) If the size was zero
	// 			return true,
	// 	5) Else
	// 			increment our position on the txn_waits_ map
	// 			return false

	LockRequest lr = LockRequest(EXCLUSIVE, txn);

	// might need to initialize this queue
	if(lock_table_.count(key) == 0)
		lock_table_[key] = new deque<LockRequest>();

	int orig_deque_size = lock_table_[key]->size();

	lock_table_[key]->push_back(lr);

	// only return true if the queue was empty beforehand
	if(orig_deque_size == 0)
		return true;
	else{
		if(txn_waits_.count(txn) == 0)
			txn_waits_[txn] = 1;
		else
			txn_waits_[txn]++;

		return false;
	}
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  // CPSC 638:
  //
	// Pseudocode:
	// 	1) Make a new Lock Request for txn, with lock mode = read
	// 	2) Add it to the lock_table_ deque
	// 	3) Keep scanning through the deque until we hit an exclusive lock or 
	// 		 our waiting request
	// 	3) If we hit our waiting request
	// 			return true,
	// 	4) Else
	// 			increment our position on the txn_waits_ map
	// 			return false
	
	LockRequest lr = LockRequest(SHARED, txn);

	// might need to initialize this queue
	if(lock_table_.count(key) == 0)
		lock_table_[key] = new deque<LockRequest>();
	
	lock_table_[key]->push_back(lr);

	deque<LockRequest>::iterator i;
	for(i = lock_table_[key]->begin(); i != lock_table_[key]->end(); i++){
		if(i->txn_ == txn)
			return true;
		else if(i->mode_ == EXCLUSIVE)
			break;
	}

	// we didn't return, so the lock is being held by someone else
	if(txn_waits_.count(txn) == 0)
		txn_waits_[txn] = 1;
	else
		txn_waits_[txn]++;

	return false;
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  // CPSC 638:
  //
  // Implement this method!
	//
	// Pseudocode:
	// 	1) Get the appropriate deque from lock_table
	// 	2) Scan through ^ and find the appropriate element's index
	// 	3) If we never found it, call DIE(*) or something
	// 	4) Find if this release should turn the lock over, i.e.
	// 		a) It's an exclusive lock
	// 		b) It's the only shared lock in the line
	// 	5) If so ^
	// 		-> Erase it from the deque
	// 		-> If the next element in the deque is exclusive, decrement his
	// 			 txn_waits and possibly move him to ready_txns_
	// 		-> If the next element in the deque is shared, continue iterating
	// 			 through all shared locks and do the above ^
	// 	6) Else
	// 		-> Erase it from the deque
	// 	7) Delete txn's entry in txn_waits_
	
	if(lock_table_.count(key) == 0)
		DIE("Tried to release a key that didn't exist: " << key);
	
	deque<LockRequest>* locks = lock_table_[key];

	// find the index of this txn
	deque<LockRequest>::iterator i;
	for(i = locks->begin(); i != locks->end(); i++){
		if(i->txn_ == txn)
			break;
	}

	// do we have to pass on the lock to anyone else?
	bool pass_lock;
	if(i != locks->begin())
		pass_lock = false; // we have to be first in line to give it up
	else if(i->mode_ == EXCLUSIVE)
		pass_lock = true; // if we're exclusive, someone else gets it now
	else if(i->mode_ == SHARED && i + 1 == locks->end())
		pass_lock = false; // if we're the last, no one else gets it
	else if(i->mode_ == SHARED && (i + 1)->mode_ == EXCLUSIVE)
		pass_lock = true; // if the next guy is an exclusive lock, he gets it
	else
		pass_lock = false;

	locks->erase(i);

	txn_waits_.erase(txn);
	if(!pass_lock)
		return;

	// turn over the lock to the next bunch!
	bool exclusive = locks->size() > 0 && locks->front().mode_ == EXCLUSIVE;
	for(i = locks->begin(); i != locks->end(); i++){
		if((i->mode_ == EXCLUSIVE && exclusive)){
			// decrement and potentially move to queue
			txn_waits_[i->txn_] --;
			if(txn_waits_[i->txn_] == 0){
				ready_txns_->push_back(i->txn_);
				txn_waits_.erase(i->txn_);
			}
			break;
		}else if (i->mode_ == EXCLUSIVE){
			break;
		}else if (i->mode_ == SHARED && exclusive){
			break;
		}else if (i->mode_ == SHARED && !exclusive){
			// same, but don't break
			txn_waits_[i->txn_] --;
			if(txn_waits_[i->txn_] == 0){
				ready_txns_->push_back(i->txn_);
				txn_waits_.erase(i->txn_);
			}
		}
	}
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
	if(lock_table_.count(key) == 0)
		DIE("Tried to get status of a key that didn't exist: " << key);
	
	deque<LockRequest>* locks = lock_table_[key];

	if(locks->size() == 0)
		return UNLOCKED;
	
	bool exclusive = locks->size() > 0 && locks->front().mode_ == EXCLUSIVE;

	if(exclusive){
		owners->push_back(locks->front().txn_);
		return EXCLUSIVE;
	}
	
	deque<LockRequest>::iterator i;
	for(i = locks->begin(); i != locks->end(); i++){
		if(i->mode_ == SHARED){
			owners->push_back(i->txn_);
			std::cout << "Adding " << i->txn_ << std::endl;
		}
		else
			break;
	}

	return SHARED;
}

