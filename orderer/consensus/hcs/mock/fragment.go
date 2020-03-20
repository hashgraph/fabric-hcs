// Code generated by counterfeiter. DO NOT EDIT.
package mock

import (
	"sync"

	"github.com/hyperledger/fabric-protos-go/orderer"
)

type FragmentSupport struct {
	ExpireByAgeStub        func(uint64) int
	expireByAgeMutex       sync.RWMutex
	expireByAgeArgsForCall []struct {
		arg1 uint64
	}
	expireByAgeReturns struct {
		result1 int
	}
	expireByAgeReturnsOnCall map[int]struct {
		result1 int
	}
	ExpireByFragmentKeyStub        func([]byte) (int, error)
	expireByFragmentKeyMutex       sync.RWMutex
	expireByFragmentKeyArgsForCall []struct {
		arg1 []byte
	}
	expireByFragmentKeyReturns struct {
		result1 int
		result2 error
	}
	expireByFragmentKeyReturnsOnCall map[int]struct {
		result1 int
		result2 error
	}
	IsPendingStub        func() bool
	isPendingMutex       sync.RWMutex
	isPendingArgsForCall []struct {
	}
	isPendingReturns struct {
		result1 bool
	}
	isPendingReturnsOnCall map[int]struct {
		result1 bool
	}
	MakeFragmentsStub        func([]byte, []byte, uint64) []*orderer.HcsMessageFragment
	makeFragmentsMutex       sync.RWMutex
	makeFragmentsArgsForCall []struct {
		arg1 []byte
		arg2 []byte
		arg3 uint64
	}
	makeFragmentsReturns struct {
		result1 []*orderer.HcsMessageFragment
	}
	makeFragmentsReturnsOnCall map[int]struct {
		result1 []*orderer.HcsMessageFragment
	}
	ReassembleStub        func(*orderer.HcsMessageFragment) []byte
	reassembleMutex       sync.RWMutex
	reassembleArgsForCall []struct {
		arg1 *orderer.HcsMessageFragment
	}
	reassembleReturns struct {
		result1 []byte
	}
	reassembleReturnsOnCall map[int]struct {
		result1 []byte
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FragmentSupport) ExpireByAge(arg1 uint64) int {
	fake.expireByAgeMutex.Lock()
	ret, specificReturn := fake.expireByAgeReturnsOnCall[len(fake.expireByAgeArgsForCall)]
	fake.expireByAgeArgsForCall = append(fake.expireByAgeArgsForCall, struct {
		arg1 uint64
	}{arg1})
	fake.recordInvocation("ExpireByAge", []interface{}{arg1})
	fake.expireByAgeMutex.Unlock()
	if fake.ExpireByAgeStub != nil {
		return fake.ExpireByAgeStub(arg1)
	}
	if specificReturn {
		return ret.result1
	}
	fakeReturns := fake.expireByAgeReturns
	return fakeReturns.result1
}

func (fake *FragmentSupport) ExpireByAgeCallCount() int {
	fake.expireByAgeMutex.RLock()
	defer fake.expireByAgeMutex.RUnlock()
	return len(fake.expireByAgeArgsForCall)
}

func (fake *FragmentSupport) ExpireByAgeCalls(stub func(uint64) int) {
	fake.expireByAgeMutex.Lock()
	defer fake.expireByAgeMutex.Unlock()
	fake.ExpireByAgeStub = stub
}

func (fake *FragmentSupport) ExpireByAgeArgsForCall(i int) uint64 {
	fake.expireByAgeMutex.RLock()
	defer fake.expireByAgeMutex.RUnlock()
	argsForCall := fake.expireByAgeArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FragmentSupport) ExpireByAgeReturns(result1 int) {
	fake.expireByAgeMutex.Lock()
	defer fake.expireByAgeMutex.Unlock()
	fake.ExpireByAgeStub = nil
	fake.expireByAgeReturns = struct {
		result1 int
	}{result1}
}

func (fake *FragmentSupport) ExpireByAgeReturnsOnCall(i int, result1 int) {
	fake.expireByAgeMutex.Lock()
	defer fake.expireByAgeMutex.Unlock()
	fake.ExpireByAgeStub = nil
	if fake.expireByAgeReturnsOnCall == nil {
		fake.expireByAgeReturnsOnCall = make(map[int]struct {
			result1 int
		})
	}
	fake.expireByAgeReturnsOnCall[i] = struct {
		result1 int
	}{result1}
}

func (fake *FragmentSupport) ExpireByFragmentKey(arg1 []byte) (int, error) {
	var arg1Copy []byte
	if arg1 != nil {
		arg1Copy = make([]byte, len(arg1))
		copy(arg1Copy, arg1)
	}
	fake.expireByFragmentKeyMutex.Lock()
	ret, specificReturn := fake.expireByFragmentKeyReturnsOnCall[len(fake.expireByFragmentKeyArgsForCall)]
	fake.expireByFragmentKeyArgsForCall = append(fake.expireByFragmentKeyArgsForCall, struct {
		arg1 []byte
	}{arg1Copy})
	fake.recordInvocation("ExpireByFragmentKey", []interface{}{arg1Copy})
	fake.expireByFragmentKeyMutex.Unlock()
	if fake.ExpireByFragmentKeyStub != nil {
		return fake.ExpireByFragmentKeyStub(arg1)
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	fakeReturns := fake.expireByFragmentKeyReturns
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FragmentSupport) ExpireByFragmentKeyCallCount() int {
	fake.expireByFragmentKeyMutex.RLock()
	defer fake.expireByFragmentKeyMutex.RUnlock()
	return len(fake.expireByFragmentKeyArgsForCall)
}

func (fake *FragmentSupport) ExpireByFragmentKeyCalls(stub func([]byte) (int, error)) {
	fake.expireByFragmentKeyMutex.Lock()
	defer fake.expireByFragmentKeyMutex.Unlock()
	fake.ExpireByFragmentKeyStub = stub
}

func (fake *FragmentSupport) ExpireByFragmentKeyArgsForCall(i int) []byte {
	fake.expireByFragmentKeyMutex.RLock()
	defer fake.expireByFragmentKeyMutex.RUnlock()
	argsForCall := fake.expireByFragmentKeyArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FragmentSupport) ExpireByFragmentKeyReturns(result1 int, result2 error) {
	fake.expireByFragmentKeyMutex.Lock()
	defer fake.expireByFragmentKeyMutex.Unlock()
	fake.ExpireByFragmentKeyStub = nil
	fake.expireByFragmentKeyReturns = struct {
		result1 int
		result2 error
	}{result1, result2}
}

func (fake *FragmentSupport) ExpireByFragmentKeyReturnsOnCall(i int, result1 int, result2 error) {
	fake.expireByFragmentKeyMutex.Lock()
	defer fake.expireByFragmentKeyMutex.Unlock()
	fake.ExpireByFragmentKeyStub = nil
	if fake.expireByFragmentKeyReturnsOnCall == nil {
		fake.expireByFragmentKeyReturnsOnCall = make(map[int]struct {
			result1 int
			result2 error
		})
	}
	fake.expireByFragmentKeyReturnsOnCall[i] = struct {
		result1 int
		result2 error
	}{result1, result2}
}

func (fake *FragmentSupport) IsPending() bool {
	fake.isPendingMutex.Lock()
	ret, specificReturn := fake.isPendingReturnsOnCall[len(fake.isPendingArgsForCall)]
	fake.isPendingArgsForCall = append(fake.isPendingArgsForCall, struct {
	}{})
	fake.recordInvocation("IsPending", []interface{}{})
	fake.isPendingMutex.Unlock()
	if fake.IsPendingStub != nil {
		return fake.IsPendingStub()
	}
	if specificReturn {
		return ret.result1
	}
	fakeReturns := fake.isPendingReturns
	return fakeReturns.result1
}

func (fake *FragmentSupport) IsPendingCallCount() int {
	fake.isPendingMutex.RLock()
	defer fake.isPendingMutex.RUnlock()
	return len(fake.isPendingArgsForCall)
}

func (fake *FragmentSupport) IsPendingCalls(stub func() bool) {
	fake.isPendingMutex.Lock()
	defer fake.isPendingMutex.Unlock()
	fake.IsPendingStub = stub
}

func (fake *FragmentSupport) IsPendingReturns(result1 bool) {
	fake.isPendingMutex.Lock()
	defer fake.isPendingMutex.Unlock()
	fake.IsPendingStub = nil
	fake.isPendingReturns = struct {
		result1 bool
	}{result1}
}

func (fake *FragmentSupport) IsPendingReturnsOnCall(i int, result1 bool) {
	fake.isPendingMutex.Lock()
	defer fake.isPendingMutex.Unlock()
	fake.IsPendingStub = nil
	if fake.isPendingReturnsOnCall == nil {
		fake.isPendingReturnsOnCall = make(map[int]struct {
			result1 bool
		})
	}
	fake.isPendingReturnsOnCall[i] = struct {
		result1 bool
	}{result1}
}

func (fake *FragmentSupport) MakeFragments(arg1 []byte, arg2 []byte, arg3 uint64) []*orderer.HcsMessageFragment {
	var arg1Copy []byte
	if arg1 != nil {
		arg1Copy = make([]byte, len(arg1))
		copy(arg1Copy, arg1)
	}
	var arg2Copy []byte
	if arg2 != nil {
		arg2Copy = make([]byte, len(arg2))
		copy(arg2Copy, arg2)
	}
	fake.makeFragmentsMutex.Lock()
	ret, specificReturn := fake.makeFragmentsReturnsOnCall[len(fake.makeFragmentsArgsForCall)]
	fake.makeFragmentsArgsForCall = append(fake.makeFragmentsArgsForCall, struct {
		arg1 []byte
		arg2 []byte
		arg3 uint64
	}{arg1Copy, arg2Copy, arg3})
	fake.recordInvocation("MakeFragments", []interface{}{arg1Copy, arg2Copy, arg3})
	fake.makeFragmentsMutex.Unlock()
	if fake.MakeFragmentsStub != nil {
		return fake.MakeFragmentsStub(arg1, arg2, arg3)
	}
	if specificReturn {
		return ret.result1
	}
	fakeReturns := fake.makeFragmentsReturns
	return fakeReturns.result1
}

func (fake *FragmentSupport) MakeFragmentsCallCount() int {
	fake.makeFragmentsMutex.RLock()
	defer fake.makeFragmentsMutex.RUnlock()
	return len(fake.makeFragmentsArgsForCall)
}

func (fake *FragmentSupport) MakeFragmentsCalls(stub func([]byte, []byte, uint64) []*orderer.HcsMessageFragment) {
	fake.makeFragmentsMutex.Lock()
	defer fake.makeFragmentsMutex.Unlock()
	fake.MakeFragmentsStub = stub
}

func (fake *FragmentSupport) MakeFragmentsArgsForCall(i int) ([]byte, []byte, uint64) {
	fake.makeFragmentsMutex.RLock()
	defer fake.makeFragmentsMutex.RUnlock()
	argsForCall := fake.makeFragmentsArgsForCall[i]
	return argsForCall.arg1, argsForCall.arg2, argsForCall.arg3
}

func (fake *FragmentSupport) MakeFragmentsReturns(result1 []*orderer.HcsMessageFragment) {
	fake.makeFragmentsMutex.Lock()
	defer fake.makeFragmentsMutex.Unlock()
	fake.MakeFragmentsStub = nil
	fake.makeFragmentsReturns = struct {
		result1 []*orderer.HcsMessageFragment
	}{result1}
}

func (fake *FragmentSupport) MakeFragmentsReturnsOnCall(i int, result1 []*orderer.HcsMessageFragment) {
	fake.makeFragmentsMutex.Lock()
	defer fake.makeFragmentsMutex.Unlock()
	fake.MakeFragmentsStub = nil
	if fake.makeFragmentsReturnsOnCall == nil {
		fake.makeFragmentsReturnsOnCall = make(map[int]struct {
			result1 []*orderer.HcsMessageFragment
		})
	}
	fake.makeFragmentsReturnsOnCall[i] = struct {
		result1 []*orderer.HcsMessageFragment
	}{result1}
}

func (fake *FragmentSupport) Reassemble(arg1 *orderer.HcsMessageFragment) []byte {
	fake.reassembleMutex.Lock()
	ret, specificReturn := fake.reassembleReturnsOnCall[len(fake.reassembleArgsForCall)]
	fake.reassembleArgsForCall = append(fake.reassembleArgsForCall, struct {
		arg1 *orderer.HcsMessageFragment
	}{arg1})
	fake.recordInvocation("Reassemble", []interface{}{arg1})
	fake.reassembleMutex.Unlock()
	if fake.ReassembleStub != nil {
		return fake.ReassembleStub(arg1)
	}
	if specificReturn {
		return ret.result1
	}
	fakeReturns := fake.reassembleReturns
	return fakeReturns.result1
}

func (fake *FragmentSupport) ReassembleCallCount() int {
	fake.reassembleMutex.RLock()
	defer fake.reassembleMutex.RUnlock()
	return len(fake.reassembleArgsForCall)
}

func (fake *FragmentSupport) ReassembleCalls(stub func(*orderer.HcsMessageFragment) []byte) {
	fake.reassembleMutex.Lock()
	defer fake.reassembleMutex.Unlock()
	fake.ReassembleStub = stub
}

func (fake *FragmentSupport) ReassembleArgsForCall(i int) *orderer.HcsMessageFragment {
	fake.reassembleMutex.RLock()
	defer fake.reassembleMutex.RUnlock()
	argsForCall := fake.reassembleArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FragmentSupport) ReassembleReturns(result1 []byte) {
	fake.reassembleMutex.Lock()
	defer fake.reassembleMutex.Unlock()
	fake.ReassembleStub = nil
	fake.reassembleReturns = struct {
		result1 []byte
	}{result1}
}

func (fake *FragmentSupport) ReassembleReturnsOnCall(i int, result1 []byte) {
	fake.reassembleMutex.Lock()
	defer fake.reassembleMutex.Unlock()
	fake.ReassembleStub = nil
	if fake.reassembleReturnsOnCall == nil {
		fake.reassembleReturnsOnCall = make(map[int]struct {
			result1 []byte
		})
	}
	fake.reassembleReturnsOnCall[i] = struct {
		result1 []byte
	}{result1}
}

func (fake *FragmentSupport) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.expireByAgeMutex.RLock()
	defer fake.expireByAgeMutex.RUnlock()
	fake.expireByFragmentKeyMutex.RLock()
	defer fake.expireByFragmentKeyMutex.RUnlock()
	fake.isPendingMutex.RLock()
	defer fake.isPendingMutex.RUnlock()
	fake.makeFragmentsMutex.RLock()
	defer fake.makeFragmentsMutex.RUnlock()
	fake.reassembleMutex.RLock()
	defer fake.reassembleMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FragmentSupport) recordInvocation(key string, args []interface{}) {
	fake.invocationsMutex.Lock()
	defer fake.invocationsMutex.Unlock()
	if fake.invocations == nil {
		fake.invocations = map[string][][]interface{}{}
	}
	if fake.invocations[key] == nil {
		fake.invocations[key] = [][]interface{}{}
	}
	fake.invocations[key] = append(fake.invocations[key], args)
}
