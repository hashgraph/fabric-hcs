package hcs

import (
	crand "crypto/rand"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/hashgraph/hedera-sdk-go"
	cb "github.com/hyperledger/fabric-protos-go/common"
	ab "github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/common/msgprocessor"
	"github.com/hyperledger/fabric/orderer/consensus"
	"github.com/hyperledger/fabric/orderer/consensus/hcs/factory"
	mockhcs "github.com/hyperledger/fabric/orderer/consensus/hcs/mock"
	mockblockcutter "github.com/hyperledger/fabric/orderer/mocks/common/blockcutter"
	mockmultichannel "github.com/hyperledger/fabric/orderer/mocks/common/multichannel"
	"github.com/hyperledger/fabric/protoutil"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

//go:generate counterfeiter -o mock/orderer_capabilities.go --fake-name OrdererCapabilities . ordererCapabilities

type ordererCapabilities interface {
	channelconfig.OrdererCapabilities
}

//go:generate counterfeiter -o mock/channel_capabilities.go --fake-name ChannelCapabilities . channelCapabilities

type channelCapabilities interface {
	channelconfig.ChannelCapabilities
}

//go:generate counterfeiter -o mock/channel_config.go --fake-name ChannelConfig . channelConfig

type channelConfig interface {
	channelconfig.Channel
}

//go:generate counterfeiter -o mock/hcs_client_factory.go --fake-name HcsClientFactory . hcsClientFactory

type hcsClientFactory interface {
	factory.HcsClientFactory
}

//go:generate counterfeiter -o mock/consensus_client.go --fake-name ConsensusClient . consensusClient

type consensusClient interface {
	factory.ConsensusClient
}

//go:generate counterfeiter -o mock/mirror_client.go --fake-name MirrorClient . mirrorClient

type mirrorClient interface {
	factory.MirrorClient
}

const (
	GetConsensusClientFuncName = "GetConsensusClient"
	GetMirrorClientFuncName    = "GetMirrorClient"

	TestOperatorPrivateKey = "302e020100300506032b657004220420e373811ccb438637a4358db3cbb72dd899eeda6b764c0b8128c61063752b4fe4"
)

func newMockOrderer(batchTimeout time.Duration, hcs *ab.Hcs) *mockhcs.OrdererConfig {
	mockCapabilities := &mockhcs.OrdererCapabilities{}
	mockCapabilities.ResubmissionReturns(false)
	mockOrderer := &mockhcs.OrdererConfig{}
	mockOrderer.CapabilitiesReturns(mockCapabilities)
	mockOrderer.BatchTimeoutReturns(batchTimeout)
	mockOrderer.HcsReturns(hcs)
	return mockOrderer
}

func newMockChannel() *mockhcs.ChannelConfig {
	mockCapabilities := &mockhcs.ChannelCapabilities{}
	mockCapabilities.ConsensusTypeMigrationReturns(false)
	mockChannel := &mockhcs.ChannelConfig{}
	mockChannel.CapabilitiesReturns(mockCapabilities)
	return mockChannel
}

var (
	goodHcsConfig = ab.Hcs{TopicId: "0.0.19610"}

	extraShortTimeout = 1 * time.Millisecond
	shortTimeout      = 1 * time.Second
	longTimeout       = 1 * time.Hour

	hitBranch = 50 * time.Millisecond
)

func TestChain(t *testing.T) {

	oldestConsensusTimestamp := unixEpoch
	newestConsensusTimestamp := unixEpoch.Add(time.Hour * 1000)
	lastOriginalOffsetProcessed := uint64(0)
	lastResubmittedConfigOffset := uint64(0)
	lastFragmentId := uint64(0)

	newMocks := func(t *testing.T) (mockConsenter *consenterImpl, mockSupport *mockmultichannel.ConsenterSupport) {
		mockConsenter = &consenterImpl{
			&localconfig.Hcs{
				Nodes:             map[string]string{"127.0.0.1:50211": "0.0.3", "127.0.0.2:50211": "0.0.4"},
				MirrorNodeAddress: "127.0.0.5:5600",
				Operator: localconfig.HcsOperator{
					Id: "0.0.19882",
					PrivateKey: localconfig.HcsPrivateKey{
						Enabled: true,
						Type:    "ed25519",
						Key:     TestOperatorPrivateKey,
					},
				},
			},
		}

		mockSupport = &mockmultichannel.ConsenterSupport{
			ChannelIDVal:     channelNameForTest(t),
			HeightVal:        uint64(3),
			SharedConfigVal:  newMockOrderer(shortTimeout, &goodHcsConfig),
			ChannelConfigVal: newMockChannel(),
		}
		return mockConsenter, mockSupport
	}

	waitNumBlocksUntil := func(blocks chan *cb.Block, expected int, duration time.Duration) int {
		received := 0
		timer := time.After(duration)
		for {
			if received == expected {
				return received
			}

			select {
			case _, ok := <-blocks:
				if ok {
					received++
				} else {
					return received
				}
			case <-timer:
				return received
			}
		}
	}

	t.Run("New", func(t *testing.T) {
		mockConsenter, mockSupport := newMocks(t)
		hcf := newDefaultMockHcsClientFactory()
		chain, err := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)

		assert.NoError(t, err, "Expected newChain to return without errors")
		select {
		case <-chain.Errored():
			logger.Debug("Errored() returned a closed channel as expected")
		default:
			t.Fatal("Errored() should have returned a closed channel")
		}

		select {
		case <-chain.haltChan:
			t.Fatal("haltChan should have been open")
		default:
			logger.Debug("haltChan is open as it should be")
		}

		select {
		case <-chain.startChan:
			t.Fatal("startChan should have been open")
		default:
			logger.Debug("startChan is open as it should be")
		}

		assert.Equal(t, chain.lastCutBlockNumber, mockSupport.Height()-1)
		assert.Equal(t, chain.lastConsensusTimestampPersisted, unixEpoch)
		assert.Equal(t, chain.lastOriginalSequenceProcessed, lastOriginalOffsetProcessed)
		assert.Equal(t, chain.lastResubmittedConfigSequence, lastResubmittedConfigOffset)
		assert.Equal(t, chain.lastFragmentId, uint64(0))
	})

	t.Run("Start", func(t *testing.T) {
		mockConsenter, mockSupport := newMocks(t)
		hcf := newDefaultMockHcsClientFactory()
		chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)

		chain.Start()
		select {
		case <-chain.startChan:
			logger.Debug("startChan is closed as it should be")
		case <-time.After(shortTimeout):
			t.Fatal("startChan should have been closed by now")
		}

		// Trigger the haltChan clause in the processMessagesToBlocks goroutine
		close(chain.haltChan)
		returnValues := hcf.GetReturnValues()
		assert.Equalf(t, 2, len(returnValues[GetConsensusClientFuncName]), "Expected %s called 2 times", GetConsensusClientFuncName)
		assert.Equalf(t, 1, len(returnValues[GetMirrorClientFuncName]), "Expected %s called once", GetMirrorClientFuncName)

		v := reflect.ValueOf(returnValues[GetMirrorClientFuncName][0]).Index(0)
		mc := v.Interface().(*mockhcs.MirrorClient)
		assert.Equal(t, 1, mc.SubscribeTopicCallCount(), "Expected SubscribeTopic called once")
		_, start, end := mc.SubscribeTopicArgsForCall(0)
		assert.Equal(t, unixEpoch, *start, "Expected startTime passed to SubscribeTopic to be unixEpoch")
		assert.Nil(t, end, "Expected endTime passed to SubscribeTopic to be unixEpoch")
	})

	t.Run("StartWithNonUnixEpochLastConsensusTimestamp", func(t *testing.T) {
		mockConsenter, mockSupport := newMocks(t)
		hcf := newDefaultMockHcsClientFactory()
		chain, _ := newChain(mockConsenter, mockSupport, hcf, newestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)

		chain.Start()
		select {
		case <-chain.startChan:
			logger.Debug("startChan is closed as it should be")
		case <-time.After(shortTimeout):
			t.Fatal("startChan should have been closed by now")
		}

		// Trigger the haltChan clause in the processMessagesToBlocks goroutine
		close(chain.haltChan)
		returnValues := hcf.GetReturnValues()
		assert.Equalf(t, 1, len(returnValues[GetMirrorClientFuncName]), "Expected %s called once", GetMirrorClientFuncName)

		v := reflect.ValueOf(returnValues[GetMirrorClientFuncName][0]).Index(0)
		mc := v.Interface().(*mockhcs.MirrorClient)
		assert.Equal(t, 1, mc.SubscribeTopicCallCount(), "Expected SubscribeTopic called once")
		_, start, end := mc.SubscribeTopicArgsForCall(0)
		assert.Equal(t, newestConsensusTimestamp.Add(time.Nanosecond), *start, "Expected startTime passed to SubscribeTopic to be unixEpoch")
		assert.Nil(t, end, "Expected endTime passed to SubscribeTopic to be unixEpoch")
	})

	t.Run("Halt", func(t *testing.T) {
		mockConsenter, mockSupport := newMocks(t)
		hcf := newDefaultMockHcsClientFactory()
		chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)

		chain.Start()
		select {
		case <-chain.startChan:
			logger.Debug("startChan is closed as it should be")
		case <-time.After(shortTimeout):
			t.Fatal("startChan should have been closed by now")
		}

		// Wait till the start phase has completed, then:
		chain.Halt()

		select {
		case <-chain.haltChan:
			logger.Debug("haltChan is closed as it should be")
		case <-time.After(shortTimeout):
			t.Fatal("haltChan should have been closed")
		}

		select {
		case <-chain.errorChan:
			logger.Debug("errorChan is closed as it should be")
		case <-time.After(shortTimeout):
			t.Fatal("errorChan should have been closed")
		}

		// verify Close() is called once
		returnValues := hcf.GetReturnValues()
		for funcName, retVals := range returnValues {
			for _, ret := range retVals {
				numCalls := 0
				v := reflect.ValueOf(ret).Index(0)
				switch funcName {
				case GetConsensusClientFuncName:
					client := v.Interface().(*mockhcs.ConsensusClient)
					numCalls = client.CloseCallCount()
				case GetMirrorClientFuncName:
					client := v.Interface().(*mockhcs.MirrorClient)
					numCalls = client.CloseCallCount()
				}
				assert.Equal(t, 1, numCalls, "Expect Close called once")
			}
		}
	})

	t.Run("DoubleHalt", func(t *testing.T) {
		mockConsenter, mockSupport := newMocks(t)
		hcf := newDefaultMockHcsClientFactory()
		chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)

		chain.Start()
		select {
		case <-chain.startChan:
			logger.Debug("startChan is closed as it should be")
		case <-time.After(shortTimeout):
			t.Fatal("startChan should have been closed by now")
		}

		chain.Halt()
		assert.NotPanics(t, func() { chain.Halt() }, "Calling Halt() more than once shouldn't panic")
	})

	t.Run("HaltBeforeStart", func(t *testing.T) {
		mockConsenter, mockSupport := newMocks(t)
		hcf := newDefaultMockHcsClientFactory()
		chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)

		go func() {
			time.Sleep(shortTimeout)
			chain.Start()
			select {
			case <-chain.startChan:
				logger.Debug("startChan is closed as it should be")
			case <-time.After(shortTimeout):
				t.Fatal("startChan should have been closed by now")
			}
		}()

		done := make(chan struct{})
		go func() {
			chain.Halt()
			close(done)
		}()
		// halt should return once chain is started
		select {
		case <-done:
			logger.Debug("Halt returns as expected")
		case <-time.After(3 * shortTimeout):
			close(chain.startChan)
			t.Fatalf("Halt should have returned")
		}
	})

	t.Run("StartWithTopicProducerError", func(t *testing.T) {
		mockConsenter, mockSupport := newMocks(t)
		getConsensusClient := func(network map[string]hedera.AccountID, operator hedera.AccountID, privateKey hedera.Ed25519PrivateKey) (factory.ConsensusClient, error) {
			return nil, fmt.Errorf("foo error")
		}
		hcf := newMockHcsClientFactory(getConsensusClient, nil)
		chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)

		assert.Panics(t, func() { startThread(chain) }, "Expected the Start() call to panic")
	})

	t.Run("StartWithTopicConsumerError", func(t *testing.T) {
		mockConsenter, mockSupport := newMocks(t)
		getMirrorClient := func(endpoint string) (factory.MirrorClient, error) {
			return nil, fmt.Errorf("foo error")
		}
		hcf := newMockHcsClientFactory(nil, getMirrorClient)
		chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)

		assert.Panics(t, func() { startThread(chain) }, "Expected the Start() call to panic")
	})

	t.Run("StartWithTopicSubscriptionError", func(t *testing.T) {
		mockConsenter, mockSupport := newMocks(t)
		getMirrorClient := func(endpoint string) (factory.MirrorClient, error) {
			mc := mockhcs.MirrorClient{}
			mc.SubscribeTopicCalls(func(
				topicId hedera.ConsensusTopicID,
				start *time.Time,
				end *time.Time,
			) (factory.MirrorSubscriptionHandle, error) {
				return nil, fmt.Errorf("foo error")
			})
			return &mc, nil
		}
		hcf := newMockHcsClientFactory(nil, getMirrorClient)
		chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)

		assert.Panics(t, func() { startThread(chain) }, "Expected the Start() call to panic")
	})

	t.Run("enqueueIfNotStarted", func(t *testing.T) {
		mockConsenter, mockSupport := newMocks(t)
		hcf := newDefaultMockHcsClientFactory()
		chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)

		// We don't need to create a legit envelope here as it's not inspected during this test
		assert.False(t, chain.enqueue(newNormalMessage([]byte("testMessage"), uint64(1), uint64(0)), false), "Expected enqueue call to return false")
	})

	t.Run("enqueueProper", func(t *testing.T) {
		mockConsenter, mockSupport := newMocks(t)
		getConsensusClient := func(network map[string]hedera.AccountID, operator hedera.AccountID, privateKey hedera.Ed25519PrivateKey) (factory.ConsensusClient, error) {
			cs := mockhcs.ConsensusClient{}
			cs.CloseCalls(func() error { return nil })
			cs.SubmitConsensusMessageCalls(func(message []byte, id hedera.ConsensusTopicID) error {
				return nil
			})
			return &cs, nil
		}
		hcf := newMockHcsClientFactory(getConsensusClient, nil)
		chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)

		chain.Start()
		select {
		case <-chain.startChan:
			logger.Debug("startChan is closed as it should be")
		case <-time.After(shortTimeout):
			t.Fatal("startChan should have been closed by now")
		}

		assert.True(t, chain.enqueue(newNormalMessage([]byte("testMessage"), uint64(0), uint64(0)), false), "Expect enqueue call to return true")
		chain.Halt()
	})

	t.Run("enqueueIfHalted", func(t *testing.T) {
		mockConsenter, mockSupport := newMocks(t)
		hcf := newDefaultMockHcsClientFactory()
		chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)

		chain.Start()
		select {
		case <-chain.startChan:
			logger.Debug("startChan is closed as it should be")
		case <-time.After(shortTimeout):
			t.Fatal("startChan should have been closed by now")
		}
		chain.Halt()

		// haltChan should close access to the post path.
		// We don't need to create a legit envelope here as it's not inspected during this test
		assert.False(t, chain.enqueue(newNormalMessage([]byte("testMessage"), uint64(0), uint64(0)), false), "Expected enqueue call to return false")
	})

	t.Run("enqueueError", func(t *testing.T) {
		mockConsenter, mockSupport := newMocks(t)
		getConsensusClient := func(network map[string]hedera.AccountID, operator hedera.AccountID, privateKey hedera.Ed25519PrivateKey) (factory.ConsensusClient, error) {
			cs := mockhcs.ConsensusClient{}
			cs.CloseCalls(func() error { return nil })
			cs.SubmitConsensusMessageCalls(func(message []byte, id hedera.ConsensusTopicID) error {
				return fmt.Errorf("foo error")
			})
			return &cs, nil
		}
		hcf := newMockHcsClientFactory(getConsensusClient, nil)
		chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)

		chain.Start()
		select {
		case <-chain.startChan:
			logger.Debug("startChan is closed as it should be")
		case <-time.After(shortTimeout):
			t.Fatal("startChan should have been closed by now")
		}
		defer chain.Halt()

		assert.False(t, chain.enqueue(newNormalMessage([]byte("testMessage"), uint64(0), uint64(0)), false), "Expected enqueue call to return false")
	})

	t.Run("Order", func(t *testing.T) {
		t.Run("ErrorIfNotStarted", func(t *testing.T) {
			mockConsenter, mockSupport := newMocks(t)
			hcf := newDefaultMockHcsClientFactory()
			chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)

			// We don't need to create a legit envelope here as it's not inspected during this test
			assert.Error(t, chain.Order(&cb.Envelope{}, uint64(0)))
		})

		t.Run("Proper", func(t *testing.T) {
			mockConsenter, mockSupport := newMocks(t)
			mockSupport.BlockCutterVal = mockblockcutter.NewReceiver()
			hcf := newDefaultMockHcsClientFactory()
			chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)
			savedLastCubBlockNumber := chain.lastCutBlockNumber

			chain.Start()
			select {
			case <-chain.startChan:
				logger.Debug("startChan is closed as it should be")
			case <-time.After(shortTimeout):
				t.Fatal("startChan should have been closed by now")
			}

			assert.NoError(t, chain.Order(&cb.Envelope{}, uint64(0)), "Expect Order successfully")
			close(mockSupport.BlockCutterVal.Block)
			chain.Halt()
			assert.Equal(t, savedLastCubBlockNumber, chain.lastCutBlockNumber, "Expect no block cut")
		})

		t.Run("TwoSingleEnvBlocks", func(t *testing.T) {
			mockConsenter, mockSupport := newMocks(t)
			mockSupport.Blocks = make(chan *cb.Block)
			mockSupport.BlockCutterVal = mockblockcutter.NewReceiver()
			mockSupport.BlockCutterVal.CutNext = true
			hcf := newDefaultMockHcsClientFactory()
			chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)
			savedLastCubBlockNumber := chain.lastCutBlockNumber
			close(mockSupport.BlockCutterVal.Block)

			chain.Start()
			select {
			case <-chain.startChan:
				logger.Debug("startChan is closed as it should be")
			case <-time.After(shortTimeout):
				t.Fatal("startChan should have been closed by now")
			}
			close(getRespSyncChan(chain.topicSubscriptionHandle))

			assert.NoError(t, chain.Order(&cb.Envelope{}, uint64(0)), "Expect Order successfully")
			assert.NoError(t, chain.Order(&cb.Envelope{}, uint64(0)), "Expect Order successfully")
			numBlocksWritten := waitNumBlocksUntil(mockSupport.Blocks, 2, shortTimeout)
			chain.Halt()
			assert.Equal(t, savedLastCubBlockNumber+2, chain.lastCutBlockNumber, "Expect two blocks cut")
			assert.Equal(t, 2, numBlocksWritten, "Expect two blocks written")
		})

		t.Run("BatchLengthTwo", func(t *testing.T) {
			mockConsenter, mockSupport := newMocks(t)
			mockSupport.Blocks = make(chan *cb.Block)
			mockSupport.BlockCutterVal = mockblockcutter.NewReceiver()
			hcf := newDefaultMockHcsClientFactory()
			chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)
			savedLastCubBlockNumber := chain.lastCutBlockNumber

			chain.Start()
			select {
			case <-chain.startChan:
				logger.Debug("startChan is closed as it should be")
			case <-time.After(shortTimeout):
				t.Fatal("startChan should have been closed by now")
			}
			close(getRespSyncChan(chain.topicSubscriptionHandle))

			assert.NoError(t, chain.Order(&cb.Envelope{}, uint64(0)), "Expect Order successfully")
			mockSupport.BlockCutterVal.Block <- struct{}{}
			close(mockSupport.BlockCutterVal.Block)
			mockSupport.BlockCutterVal.IsolatedTx = true
			assert.NoError(t, chain.Order(&cb.Envelope{}, uint64(0)), "Expect Order successfully")
			numBlocksWritten := waitNumBlocksUntil(mockSupport.Blocks, 2, shortTimeout)
			chain.Halt()
			assert.Equal(t, savedLastCubBlockNumber+2, chain.lastCutBlockNumber, "Expect two blocks cut")
			assert.Equal(t, 2, numBlocksWritten, "Expect two blocks written")
		})
	})

	t.Run("Configure", func(t *testing.T) {
		t.Run("ErrorIfNotStarted", func(t *testing.T) {
			mockConsenter, mockSupport := newMocks(t)
			hcf := newDefaultMockHcsClientFactory()
			chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)

			// We don't need to create a legit envelope here as it's not inspected during this test
			assert.Error(t, chain.Configure(&cb.Envelope{}, uint64(0)))
		})

		t.Run("Proper", func(t *testing.T) {
			mockConsenter, mockSupport := newMocks(t)
			mockSupport.Blocks = make(chan *cb.Block)
			mockSupport.BlockCutterVal = mockblockcutter.NewReceiver()
			hcf := newDefaultMockHcsClientFactory()
			chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)
			savedLastCutBlockNumber := chain.lastCutBlockNumber
			// no synchronization with blockcutter needed
			close(mockSupport.BlockCutterVal.Block)

			chain.Start()
			select {
			case <-chain.startChan:
				logger.Debug("startChan is closed as it should be")
			case <-time.After(shortTimeout):
				t.Fatal("startChan should have been closed by now")
			}
			close(getRespSyncChan(chain.topicSubscriptionHandle))

			// We don't need to create a legit envelope here as it's not inspected during this test
			assert.NoError(t, chain.Configure(&cb.Envelope{}, uint64(0)), "Expect Configure successfully")
			numBlocksWritten := waitNumBlocksUntil(mockSupport.Blocks, 1, shortTimeout)
			chain.Halt()
			assert.Equal(t, savedLastCutBlockNumber+1, chain.lastCutBlockNumber, "Expect one block cut")
			assert.Equal(t, 1, numBlocksWritten, "Expect one block written")
		})

		t.Run("ProperWithPendingNormalMessage", func(*testing.T) {
			mockConsenter, mockSupport := newMocks(t)
			mockSupport.Blocks = make(chan *cb.Block)
			mockSupport.BlockCutterVal = mockblockcutter.NewReceiver()
			hcf := newDefaultMockHcsClientFactory()
			chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)
			savedLastCutBlockNumber := chain.lastCutBlockNumber
			// no synchronization with blockcutter needed
			close(mockSupport.BlockCutterVal.Block)

			chain.Start()
			select {
			case <-chain.startChan:
				logger.Debug("startChan is closed as it should be")
			case <-time.After(shortTimeout):
				t.Fatalf("startChan should have been closed by now")
			}
			close(getRespSyncChan(chain.topicSubscriptionHandle))

			assert.NoError(t, chain.Order(&cb.Envelope{}, uint64(0)), "Expect Order successfully")
			assert.NoError(t, chain.Configure(&cb.Envelope{}, uint64(0)), "Expect Configure successfully")
			numBlocksWritten := waitNumBlocksUntil(mockSupport.Blocks, 2, shortTimeout)
			chain.Halt()
			assert.Equal(t, savedLastCutBlockNumber+2, chain.lastCutBlockNumber, "Expect two blocks cut")
			assert.Equal(t, 2, numBlocksWritten, "Expect two blocks written")
		})
	})

	t.Run("TimeToCut", func(t *testing.T) {
		t.Run("Proper", func(t *testing.T) {
			mockConsenter, mockSupport := newMocks(t)
			mockSupport.Blocks = make(chan *cb.Block)
			mockSupport.BlockCutterVal = mockblockcutter.NewReceiver()
			hcf := newDefaultMockHcsClientFactory()
			chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)
			savedLastCubBlockNumber := chain.lastCutBlockNumber
			close(mockSupport.BlockCutterVal.Block)

			chain.Start()
			select {
			case <-chain.startChan:
				logger.Debug("startChan is closed as it should be")
			case <-time.After(shortTimeout):
				t.Fatal("startChan should have been closed by now")
			}
			close(getRespSyncChan(chain.topicSubscriptionHandle))

			assert.NoError(t, chain.Order(&cb.Envelope{}, uint64(0)), "Expect Order successfully")
			numBlocksWritten := waitNumBlocksUntil(mockSupport.Blocks, 1, 3*shortTimeout)
			chain.Halt()
			assert.Equal(t, savedLastCubBlockNumber+1, chain.lastCutBlockNumber, "Expect one block cut")
			assert.Equal(t, 1, numBlocksWritten, "Expect one block written")
		})

		t.Run("WithError", func(t *testing.T) {
			mockConsenter, mockSupport := newMocks(t)
			mockGetConsensusClient := func(network map[string]hedera.AccountID, operator hedera.AccountID, privateKey hedera.Ed25519PrivateKey) (factory.ConsensusClient, error) {
				mc := mockhcs.ConsensusClient{}
				mc.SubmitConsensusMessageCalls(func(data []byte, topicId hedera.ConsensusTopicID) error {
					return fmt.Errorf("foo error")
				})
				return &mc, nil
			}
			hcf := newMockHcsClientFactory(mockGetConsensusClient, nil)
			chain, _ := newChain(mockConsenter, mockSupport, hcf, oldestConsensusTimestamp, lastOriginalOffsetProcessed, lastResubmittedConfigOffset, lastFragmentId)

			chain.Start()
			select {
			case <-chain.startChan:
				logger.Debug("startChan is closed as it should be")
			case <-time.After(shortTimeout):
				t.Fatal("startChan should have been closed by now")
			}

			assert.Error(t, chain.sendTimeToCut(), "Expect error from sendTimeToCut")
			chain.Halt()
		})
	})
}

func TestSetupProducerForChannel(t *testing.T) {
	network := map[string]hedera.AccountID{
		"127.0.0.1:52011": {
			Shard:   0,
			Realm:   0,
			Account: 19988,
		},
		"127.0.0.2:52011": {
			Shard:   0,
			Realm:   0,
			Account: 19989,
		},
	}
	operator := hedera.AccountID{
		Shard:   0,
		Realm:   0,
		Account: 20000,
	}
	operatorPrivateKey, _ := hedera.Ed25519PrivateKeyFromString(TestOperatorPrivateKey)

	t.Run("Proper", func(t *testing.T) {
		hcf := newDefaultMockHcsClientFactory()
		p, sp, err := setupTopicProducer(hcf, network, operator, operatorPrivateKey)

		assert.NoError(t, err, "Expected the setupTopicProducer call to return without errors")
		assert.NoError(t, p.Close(), "Expected to close the producer without errors")
		assert.NoError(t, sp.Close(), "Expected to close the producer without errors")
	})

	t.Run("WithError", func(t *testing.T) {
		getConsensusClient := func(network map[string]hedera.AccountID, operator hedera.AccountID, privateKey hedera.Ed25519PrivateKey) (factory.ConsensusClient, error) {
			return nil, fmt.Errorf("foo error")
		}
		hcf := newMockHcsClientFactory(getConsensusClient, nil)
		p, sp, err := setupTopicProducer(hcf, network, operator, operatorPrivateKey)
		assert.Error(t, err, "Expected the setupProducerForChannel call to return an error")
		assert.Nil(t, p, "Expect the returned producter to be nil")
		assert.Nil(t, sp, "Expect the returned producter to be nil")
	})
}

func TestProcessMessages(t *testing.T) {
	newBareMinimumChain := func(t *testing.T, lastCutBlockNumber uint64, mockSupport consensus.ConsenterSupport, hcf *hcsClientFactoryWithRecord) *chainImpl {
		errorChan := make(chan struct{})
		close(errorChan)
		haltChan := make(chan struct{})

		mockConsenter := &consenterImpl{
			&localconfig.Hcs{
				Nodes:             map[string]string{"127.0.0.1:50211": "0.0.3", "127.0.0.2:50211": "0.0.4"},
				MirrorNodeAddress: "127.0.0.5:5600",
				Operator: localconfig.HcsOperator{
					Id: "0.0.19882",
					PrivateKey: localconfig.HcsPrivateKey{
						Enabled: true,
						Type:    "ed25519",
						Key:     TestOperatorPrivateKey,
					},
				},
			},
		}

		topicProducer, _ := hcf.GetConsensusClient(nil, hedera.AccountID{}, hedera.Ed25519PrivateKey{})
		assert.NotNil(t, topicProducer, "Expected topic producer created successfully")
		topicConsumer, _ := hcf.GetMirrorClient("")
		assert.NotNil(t, topicConsumer, "Expected topic consumer created successfully")
		topicId := hedera.ConsensusTopicID{0, 0, 19882}
		topicSubscriptionHandle, _ := topicConsumer.SubscribeTopic(topicId, &unixEpoch, nil)
		assert.NotNil(t, topicSubscriptionHandle, "Expected topic subscription handle created successfuly")

		return &chainImpl{
			consenter:        mockConsenter,
			ConsenterSupport: mockSupport,

			lastCutBlockNumber: lastCutBlockNumber,

			topicId:                 topicId,
			topicProducer:           topicProducer,
			singleNodeTopicProducer: topicProducer,
			topicConsumer:           topicConsumer,
			topicSubscriptionHandle: topicSubscriptionHandle,

			errorChan:              errorChan,
			haltChan:               haltChan,
			doneProcessingMessages: make(chan struct{}),

			fragmenter: newFragmentSupport(),
		}
	}
	var err error

	t.Run("TimeToCut", func(t *testing.T) {
		t.Run("PendingMsgToCutProper", func(t *testing.T) {
			mockSupport := &mockmultichannel.ConsenterSupport{
				BlockCutterVal:  mockblockcutter.NewReceiver(),
				Blocks:          make(chan *cb.Block),
				ChannelIDVal:    channelNameForTest(t),
				SharedConfigVal: newMockOrderer(shortTimeout/2, &goodHcsConfig),
			}
			hcf := newDefaultMockHcsClientFactory()
			chain := newBareMinimumChain(t, 3, mockSupport, hcf)

			done := make(chan struct{})
			go func() {
				err = chain.processMessages()
				done <- struct{}{}
			}()
			close(mockSupport.BlockCutterVal.Block)
			close(getRespSyncChan(chain.topicSubscriptionHandle))

			// plant a message directly to the mock blockcutter
			mockSupport.BlockCutterVal.Ordered(newMockEnvelope("foo message"))

			// cut ancestors
			mockSupport.BlockCutterVal.CutAncestors = true

			hcsMessage := newNormalMessage(protoutil.MarshalOrPanic(newMockEnvelope("foo message 2")), uint64(0), uint64(0))
			fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(hcsMessage), "test fragment key", 0)
			assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
			assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")

			// wait for the first block
			<-mockSupport.Blocks

			logger.Debug("closing haltChan to exit processMessages")
			close(chain.haltChan) // cause processMessages to exit
			logger.Debug("haltChan closed")
			<-done

			if chain.timer != nil {
				go func() {
					// fire the timer for garbage collection
					<-chain.timer
				}()
			}

			assert.NoError(t, err, "Expected processMessages to exit without errors")
			assert.NotEmpty(t, mockSupport.BlockCutterVal.CurBatch(), "Expected the blockcutter to be non-empty")
			assert.NotNil(t, chain.timer, "Expected the cutTimer to be non-nil when there are pending envelopes")
		})

		t.Run("ReceiveTimeToCutProper", func(t *testing.T) {
			lastCutBlockNumber := uint64(3)
			mockSupport := &mockmultichannel.ConsenterSupport{
				BlockCutterVal:  mockblockcutter.NewReceiver(),
				Blocks:          make(chan *cb.Block),
				ChannelIDVal:    channelNameForTest(t),
				SharedConfigVal: newMockOrderer(shortTimeout/2, &goodHcsConfig),
			}
			hcf := newDefaultMockHcsClientFactory()
			chain := newBareMinimumChain(t, lastCutBlockNumber, mockSupport, hcf)
			done := make(chan struct{})

			go func() {
				err = chain.processMessages()
				done <- struct{}{}
			}()
			close(mockSupport.BlockCutterVal.Block)
			close(getRespSyncChan(chain.topicSubscriptionHandle))

			// plant a message directly to the mock blockcutter
			mockSupport.BlockCutterVal.Ordered(newMockEnvelope("foo message"))

			msg := newTimeToCutMessage(lastCutBlockNumber + 1)
			fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 0)
			assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
			assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")

			// wait for the first block
			<-mockSupport.Blocks

			logger.Debug("closing haltChan to exit processMessages")
			close(chain.haltChan) // cause processMessages to exit
			logger.Debug("haltChan closed")
			<-done

			assert.NoError(t, err, "Expected the processMessages call to return without errors")
			assert.Equal(t, lastCutBlockNumber+1, chain.lastCutBlockNumber, "Expected lastCutBlockNumber to be increased by one")
		})

		t.Run("ReceiveTimeToCutZeroBatch", func(t *testing.T) {
			lastCutBlockNumber := uint64(3)
			mockSupport := &mockmultichannel.ConsenterSupport{
				BlockCutterVal:  mockblockcutter.NewReceiver(),
				Blocks:          make(chan *cb.Block),
				ChannelIDVal:    channelNameForTest(t),
				SharedConfigVal: newMockOrderer(shortTimeout/2, &goodHcsConfig),
			}
			defer close(mockSupport.BlockCutterVal.Block)
			hcf := newDefaultMockHcsClientFactory()
			chain := newBareMinimumChain(t, lastCutBlockNumber, mockSupport, hcf)
			done := make(chan struct{})
			respSyncChan := getRespSyncChan(chain.topicSubscriptionHandle)

			go func() {
				err = chain.processMessages()
				done <- struct{}{}
			}()

			msg := newTimeToCutMessage(lastCutBlockNumber + 1)
			fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 0)
			assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
			assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
			respSyncChan <- struct{}{} // sync with subscription handle to ensure the message is received by processMessages

			logger.Debug("closing haltChan to exit processMessages")
			close(chain.haltChan) // cause processMessages to exit
			logger.Debug("haltChan closed")
			<-done

			assert.Error(t, err, "Expected the processMessages call to return errors")
			assert.Equal(t, lastCutBlockNumber, chain.lastCutBlockNumber, "Expected lastCutBlockNumber to stay the same")
		})

		t.Run("ReceiveTimeToCutLargerThanExpected", func(t *testing.T) {
			lastCutBlockNumber := uint64(3)
			mockSupport := &mockmultichannel.ConsenterSupport{
				BlockCutterVal:  mockblockcutter.NewReceiver(),
				Blocks:          make(chan *cb.Block),
				ChannelIDVal:    channelNameForTest(t),
				SharedConfigVal: newMockOrderer(shortTimeout/2, &goodHcsConfig),
			}
			defer close(mockSupport.BlockCutterVal.Block)
			hcf := newDefaultMockHcsClientFactory()
			chain := newBareMinimumChain(t, lastCutBlockNumber, mockSupport, hcf)
			done := make(chan struct{})
			respSyncChan := getRespSyncChan(chain.topicSubscriptionHandle)

			go func() {
				err = chain.processMessages()
				done <- struct{}{}
			}()

			// larger than expected block number,
			msg := newTimeToCutMessage(lastCutBlockNumber + 2)
			fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 0)
			assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
			assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
			respSyncChan <- struct{}{} // sync with subscription handle to ensure the message is received by processMessages

			logger.Debug("closing haltChan to exit processMessages")
			close(chain.haltChan) // cause processMessages to exit
			logger.Debug("haltChan closed")
			<-done

			assert.Error(t, err, "Expected the processMessages call to return errors")
			assert.Equal(t, lastCutBlockNumber, chain.lastCutBlockNumber, "Expected lastCutBlockNumber to stay the same")
		})

		t.Run("ReceiveTimeToCutStale", func(t *testing.T) {
			mockSupport := &mockmultichannel.ConsenterSupport{
				BlockCutterVal:  mockblockcutter.NewReceiver(),
				Blocks:          make(chan *cb.Block),
				ChannelIDVal:    channelNameForTest(t),
				SharedConfigVal: newMockOrderer(shortTimeout/2, &goodHcsConfig),
			}
			lastCutBlockNumber := uint64(3)
			hcf := newDefaultMockHcsClientFactory()
			chain := newBareMinimumChain(t, lastCutBlockNumber, mockSupport, hcf)
			done := make(chan struct{})
			respSyncChan := getRespSyncChan(chain.topicSubscriptionHandle)

			go func() {
				err = chain.processMessages()
				done <- struct{}{}
			}()
			close(mockSupport.BlockCutterVal.Block)

			// larger than expected block number,
			msg := newTimeToCutMessage(lastCutBlockNumber)
			fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 0)
			assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
			assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
			respSyncChan <- struct{}{}

			logger.Debug("closing haltChan to exit processMessages")
			close(chain.haltChan) // cause processMessages to exit
			logger.Debug("haltChan closed")
			<-done

			assert.NoError(t, err, "Expected the processMessages call to return without errors")
			assert.Equal(t, lastCutBlockNumber, chain.lastCutBlockNumber, "Expected lastCutBlockNumber to stay the same")
		})
	})

	t.Run("Regular", func(t *testing.T) {
		t.Run("Error", func(t *testing.T) {
			lastCutBlockNumber := uint64(3)
			mockSupport := &mockmultichannel.ConsenterSupport{
				BlockCutterVal:  mockblockcutter.NewReceiver(),
				Blocks:          make(chan *cb.Block),
				ChannelIDVal:    channelNameForTest(t),
				SharedConfigVal: newMockOrderer(shortTimeout/2, &goodHcsConfig),
			}
			hcf := newDefaultMockHcsClientFactory()
			chain := newBareMinimumChain(t, lastCutBlockNumber, mockSupport, hcf)
			done := make(chan struct{})
			close(getRespSyncChan(chain.topicSubscriptionHandle))

			go func() {
				err = chain.processMessages()
				done <- struct{}{}
			}()
			close(mockSupport.BlockCutterVal.Block)

			msg := newNormalMessage([]byte("bytes won't unmarshal to envelope"), uint64(0), uint64(0))
			fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 0)
			assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
			assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")

			logger.Debug("closing haltChan to exit processMessages")
			close(chain.haltChan) // cause processMessages to exit
			logger.Debug("haltChan closed")
			<-done

			assert.NoError(t, err, "Expected the processMessages call to return without errors")
			assert.Empty(t, mockSupport.BlockCutterVal.CurBatch(), "Expect no message committed to blockcutter")
		})

		t.Run("Normal", func(t *testing.T) {
			//lastOriginalSequenceProcessed := uint64(0)

			t.Run("ReceiveTwoRegularAndCutTwoBlocks", func(t *testing.T) {
				lastCutBlockNumber := uint64(3)
				mockSupport := &mockmultichannel.ConsenterSupport{
					BlockCutterVal:  mockblockcutter.NewReceiver(),
					Blocks:          make(chan *cb.Block),
					ChannelIDVal:    channelNameForTest(t),
					SharedConfigVal: newMockOrderer(shortTimeout/2, &goodHcsConfig),
				}
				hcf := newDefaultMockHcsClientFactory()
				chain := newBareMinimumChain(t, lastCutBlockNumber, mockSupport, hcf)
				done := make(chan struct{})
				defer close(mockSupport.BlockCutterVal.Block)
				respSyncChan := getRespSyncChan(chain.topicSubscriptionHandle)
				defer close(respSyncChan)

				go func() {
					err = chain.processMessages()
					done <- struct{}{}
				}()

				// first message
				msg := newNormalMessage(protoutil.MarshalOrPanic(newMockEnvelope("test message")), uint64(0), uint64(0))
				fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 0)
				assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
				assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
				mockSupport.BlockCutterVal.Block <- struct{}{}
				block1ProtoTimestamp, err1 := ptypes.TimestampProto(getNextConsensusTimestamp(chain.topicSubscriptionHandle))
				assert.NoError(t, err1, "Expect conversion from time.Time to proto timestamp successful")
				respSyncChan <- struct{}{}

				mockSupport.BlockCutterVal.IsolatedTx = true

				// second message
				fragments[0].FragmentId++
				assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
				mockSupport.BlockCutterVal.Block <- struct{}{}
				block2ProtoTimestamp, err1 := ptypes.TimestampProto(getNextConsensusTimestamp(chain.topicSubscriptionHandle))
				assert.NoError(t, err1, "Expect conversion from time.Time to proto timestamp successful")
				respSyncChan <- struct{}{}

				var block1, block2 *cb.Block
				select {
				case block1 = <-mockSupport.Blocks:
				case <-time.After(shortTimeout):
					t.Fatalf("Did not receive the first block from the blockcutter as expected")
				}

				select {
				case block2 = <-mockSupport.Blocks:
				case <-time.After(shortTimeout):
					t.Fatalf("Did not receive the second block from the blockcutter as expected")
				}

				logger.Debug("Closing haltChan to exit the infinite for-loop")
				close(chain.haltChan) // Identical to chain.Halt()
				logger.Debug("haltChan closed")
				<-done

				//logger.Debugf("%v %v %v %v",
				//	block1ProtoTimestamp,
				//	extractEncodedConsensusTimestamp(block1.GetMetadata().Metadata[cb.BlockMetadataIndex_ORDERER]),
				//	block2ProtoTimestamp,
				//	extractEncodedConsensusTimestamp(block2.GetMetadata().Metadata[cb.BlockMetadataIndex_ORDERER]),
				//)
				assert.NoError(t, err, "Expected the procesMessages call to return without errors")
				assert.Equal(t, lastCutBlockNumber+2, chain.lastCutBlockNumber, "Expected 2 blocks cut")
				assert.Equal(t, block1ProtoTimestamp, extractEncodedConsensusTimestamp(block1.GetMetadata().Metadata[cb.BlockMetadataIndex_ORDERER]), "Expected encoded offset in first block to correct")
				assert.Equal(t, block2ProtoTimestamp, extractEncodedConsensusTimestamp(block2.GetMetadata().Metadata[cb.BlockMetadataIndex_ORDERER]), "Expected encoded offset in second block to correct")
			})

			t.Run("ReceiveRegularAndQueue", func(t *testing.T) {
				lastCutBlockNumber := uint64(3)
				mockSupport := &mockmultichannel.ConsenterSupport{
					BlockCutterVal:  mockblockcutter.NewReceiver(),
					Blocks:          make(chan *cb.Block),
					ChannelIDVal:    channelNameForTest(t),
					SharedConfigVal: newMockOrderer(shortTimeout/2, &goodHcsConfig),
				}
				hcf := newDefaultMockHcsClientFactory()
				chain := newBareMinimumChain(t, lastCutBlockNumber, mockSupport, hcf)
				done := make(chan struct{})
				close(mockSupport.BlockCutterVal.Block)
				close(getRespSyncChan(chain.topicSubscriptionHandle))

				go func() {
					err = chain.processMessages()
					done <- struct{}{}
				}()

				mockSupport.BlockCutterVal.CutNext = true

				msg := newNormalMessage(protoutil.MarshalOrPanic(newMockEnvelope("test message")), uint64(0), uint64(0))
				fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 0)
				assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
				assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
				<-mockSupport.Blocks

				close(chain.haltChan)
				logger.Debug("haltChan closed")
				<-done

				assert.NoError(t, err, "Expected the processMessages call to return without errors")
			})
		})

		// this ensures CONFIG messages are handled properly
		t.Run("Config", func(t *testing.T) {
			// a normal tx followed by a config tx, should yield two blocks
			t.Run("ReceiveConfigEnvelopeAndCut", func(t *testing.T) {
				lastCutBlockNumber := uint64(3)
				mockSupport := &mockmultichannel.ConsenterSupport{
					BlockCutterVal:  mockblockcutter.NewReceiver(),
					Blocks:          make(chan *cb.Block),
					ChannelIDVal:    channelNameForTest(t),
					SharedConfigVal: newMockOrderer(shortTimeout/2, &goodHcsConfig),
				}
				hcf := newDefaultMockHcsClientFactory()
				chain := newBareMinimumChain(t, lastCutBlockNumber, mockSupport, hcf)
				done := make(chan struct{})
				close(mockSupport.BlockCutterVal.Block)
				respSyncChan := getRespSyncChan(chain.topicSubscriptionHandle)
				defer close(respSyncChan)

				go func() {
					err = chain.processMessages()
					done <- struct{}{}
				}()

				// normal message
				msg := newNormalMessage(protoutil.MarshalOrPanic(newMockEnvelope("test message")), uint64(0), uint64(0))
				fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 0)
				assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
				assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
				normalBlockTimestamp, err1 := ptypes.TimestampProto(getNextConsensusTimestamp(chain.topicSubscriptionHandle))
				assert.NoError(t, err1, "Expect conversion from time.Time to proto timestamp successful")
				respSyncChan <- struct{}{}

				// config message
				msg = newConfigMessage(protoutil.MarshalOrPanic(newMockConfigEnvelope()), uint64(0), uint64(0))
				fragments = chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 1)
				assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
				assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
				configBlockTimestamp, err1 := ptypes.TimestampProto(getNextConsensusTimestamp(chain.topicSubscriptionHandle))
				assert.NoError(t, err1, "Expect conversion from time.Time to proto timestamp successful")
				respSyncChan <- struct{}{}

				var normalBlock, configBlock *cb.Block
				select {
				case normalBlock = <-mockSupport.Blocks:
				case <-time.After(shortTimeout):
					t.Fatalf("Did not receive a normal block from the blockcutter as expected")
				}

				select {
				case configBlock = <-mockSupport.Blocks:
				case <-time.After(shortTimeout):
					t.Fatalf("Did not receive a config block from the blockcutter as expected")
				}

				close(chain.haltChan)
				logger.Debug("haltChan closed")
				<-done

				assert.NoError(t, err, "Expected the processMessages call to return without errors")
				assert.Equal(t, lastCutBlockNumber+2, chain.lastCutBlockNumber, "Expected two blocks cut and writtern")
				assert.Equal(t, normalBlockTimestamp, extractEncodedConsensusTimestamp(normalBlock.GetMetadata().Metadata[cb.BlockMetadataIndex_ORDERER]), "Expected correct consensus timestamp in normal block")
				assert.Equal(t, configBlockTimestamp, extractEncodedConsensusTimestamp(configBlock.GetMetadata().Metadata[cb.BlockMetadataIndex_ORDERER]), "Expected correct consensus timestamp in config block")
			})

			t.Run("RevalidateConfigEnvInvalid", func(t *testing.T) {
				lastCutBlockNumber := uint64(3)
				mockSupport := &mockmultichannel.ConsenterSupport{
					BlockCutterVal:  mockblockcutter.NewReceiver(),
					Blocks:          make(chan *cb.Block),
					ChannelIDVal:    channelNameForTest(t),
					HeightVal:lastCutBlockNumber,
					ClassifyMsgVal: msgprocessor.ConfigMsg,
					SharedConfigVal: newMockOrderer(shortTimeout/2, &goodHcsConfig),
					SequenceVal:     uint64(1), // config sequence 1
					ProcessConfigMsgErr: fmt.Errorf("invalid config message"),
				}
				hcf := newDefaultMockHcsClientFactory()
				chain := newBareMinimumChain(t, lastCutBlockNumber, mockSupport, hcf)
				done := make(chan struct{})
				close(mockSupport.BlockCutterVal.Block)
				close(getRespSyncChan(chain.topicSubscriptionHandle))

				go func() {
					err = chain.processMessages()
					done <- struct{}{}
				}()

				// config message with configseq 0
				msg := newConfigMessage(protoutil.MarshalOrPanic(newMockConfigEnvelope()), uint64(0), uint64(0))
				fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 1)
				assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
				assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
				select {
				case <-mockSupport.Blocks:
					t.Fatalf("Expected no block being cut given invalid config message")
				case <-time.After(shortTimeout):
					// do nothing
				}

				close(chain.haltChan)
				logger.Debug("haltChan closed")
				<-done

				assert.NoError(t, err, "Expected the processMessages call to return without errors")
			})
		})
	})
}

func TestResubmission(t *testing.T) {
	blockIngressMsg := func(t *testing.T, block bool, fn func() error) {
		wait := make(chan struct{})
		go func() {
			fn()
			wait <- struct{}{}
		}()

		select {
		case <-wait:
			if block {
				t.Fatalf("Expected WaitReady to block")
			}
		case <-time.After(100 * time.Millisecond):
			if !block {
				t.Fatalf("Expected WaitReady not to block")
			}
		}
	}

	newBareMinimumChain := func(
		t *testing.T,
		lastCutBlockNumber uint64,
		lastOriginalSequenceProcessed uint64,
		inReprocessing bool,
		mockSupport consensus.ConsenterSupport,
		hcf *hcsClientFactoryWithRecord,
	) *chainImpl {
		startChan := make(chan struct{})
		close(startChan)
		errorChan := make(chan struct{})
		close(errorChan)
		haltChan := make(chan struct{})
		doneReprocessingMsgInFlight := make(chan struct{})
		if !inReprocessing {
			close(doneReprocessingMsgInFlight)
		}

		mockConsenter := &consenterImpl{
			&localconfig.Hcs{
				Nodes:             map[string]string{"127.0.0.1:50211": "0.0.3", "127.0.0.2:50211": "0.0.4"},
				MirrorNodeAddress: "127.0.0.5:5600",
				Operator: localconfig.HcsOperator{
					Id: "0.0.19882",
					PrivateKey: localconfig.HcsPrivateKey{
						Enabled: true,
						Type:    "ed25519",
						Key:     TestOperatorPrivateKey,
					},
				},
			},
		}

		topicProducer, _ := hcf.GetConsensusClient(nil, hedera.AccountID{}, hedera.Ed25519PrivateKey{})
		assert.NotNil(t, topicProducer, "Expected topic producer created successfully")
		topicConsumer, _ := hcf.GetMirrorClient("")
		assert.NotNil(t, topicConsumer, "Expected topic consumer created successfully")
		topicId := hedera.ConsensusTopicID{0, 0, 19882}
		topicSubscriptionHandle, _ := topicConsumer.SubscribeTopic(topicId, &unixEpoch, nil)
		assert.NotNil(t, topicSubscriptionHandle, "Expected topic subscription handle created successfuly")

		return &chainImpl{
			consenter:        mockConsenter,
			ConsenterSupport: mockSupport,

			lastOriginalSequenceProcessed: lastOriginalSequenceProcessed,
			lastCutBlockNumber: lastCutBlockNumber,

			topicId:                 topicId,
			topicProducer:           topicProducer,
			singleNodeTopicProducer: topicProducer,
			topicConsumer:           topicConsumer,
			topicSubscriptionHandle: topicSubscriptionHandle,

			startChan:startChan,
			errorChan:              errorChan,
			haltChan:               haltChan,
			doneProcessingMessages: make(chan struct{}),
			doneReprocessingMsgInFlight: doneReprocessingMsgInFlight,

			fragmenter: newFragmentSupport(),
		}
	}
	var err error

	t.Run("Normal", func(t *testing.T) {
		// this test emits a re-submitted message that does not require reprocessing
		// (by setting OriginalSequence < lastOriginalSequenceProcessed
		t.Run("AlreadyProcessedDiscard", func(t *testing.T) {
			lastCutBlockNumber := uint64(3)
			lastOriginalSequenceProcessed := uint64(5)
			mockSupport := &mockmultichannel.ConsenterSupport{
				BlockCutterVal:  mockblockcutter.NewReceiver(),
				Blocks:          make(chan *cb.Block),
				ChannelIDVal:    channelNameForTest(t),
				SharedConfigVal: newMockOrderer(shortTimeout/2, &goodHcsConfig),
			}
			hcf := newDefaultMockHcsClientFactory()
			chain := newBareMinimumChain(t, lastCutBlockNumber, lastOriginalSequenceProcessed, false, mockSupport, hcf)
			close(mockSupport.BlockCutterVal.Block)
			respSyncChan := getRespSyncChan(chain.topicSubscriptionHandle)
			defer close(respSyncChan)
			done := make(chan struct{})

			go func() {
				err = chain.processMessages()
				done <- struct{}{}
			}()

			mockSupport.BlockCutterVal.CutNext = true

			// normal message
			msg := newNormalMessage(protoutil.MarshalOrPanic(newMockEnvelope("test message")), uint64(0), lastOriginalSequenceProcessed-1)
			fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 0)
			assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
			assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
			respSyncChan <- struct{}{}

			select {
			case <-mockSupport.Blocks:
				t.Fatal("Expected no block being cut")
			case <-time.After(shortTimeout):
				// do nothing
			}

			close(chain.haltChan)
			logger.Debug("haltChan closed")
			<-done

			assert.NoError(t, err, "Expected processMessages to return without errors")
		})

		// This test emits a mock re-submitted message that requires reprocessing
		// (by setting OriginalSequence > lastOriginalSequenceProcessed)
		// Two normal messages are enqueued in this test case: reprocessed normal message where
		// `originalOffset` is not 0, followed by a normal msg  where `OriginalSequence` is 0.
		// It tests the case that even no block is cut, `lastOriginalSequenceProcessed` is still
		// updated. We inspect the block to verify correct `lastOriginalSequenceProcessed` in the
		// hcs metadata.
		t.Run("ResubmittedMsgEnqueue", func(t *testing.T) {
			lastCutBlockNumber := uint64(3)
			lastOriginalSequenceProcessed := uint64(5)
			mockSupport := &mockmultichannel.ConsenterSupport{
				BlockCutterVal:  mockblockcutter.NewReceiver(),
				Blocks:          make(chan *cb.Block),
				ChannelIDVal:    channelNameForTest(t),
				HeightVal:       lastCutBlockNumber,
				SharedConfigVal: newMockOrderer(longTimeout, &goodHcsConfig),
				SequenceVal: uint64(0),
			}
			hcf := newDefaultMockHcsClientFactory()
			chain := newBareMinimumChain(t, lastCutBlockNumber, lastOriginalSequenceProcessed, false, mockSupport, hcf)
			close(mockSupport.BlockCutterVal.Block)
			respSyncChan := getRespSyncChan(chain.topicSubscriptionHandle)
			defer close(respSyncChan)
			done := make(chan struct{})

			go func() {
				err = chain.processMessages()
				done <- struct{}{}
			}()

			// normal message which advances lastOriginalSequenceProcessed
			msg := newNormalMessage(protoutil.MarshalOrPanic(newMockEnvelope("test message")), uint64(0), lastOriginalSequenceProcessed+1)
			fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 0)
			assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
			assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
			respSyncChan <- struct{}{} // sync to make sure the message is received by processMessages

			select {
			case <-mockSupport.Blocks:
				t.Fatalf("Expected no block to be cut")
			case <-time.After(shortTimeout):
			}

			mockSupport.BlockCutterVal.CutNext = true
			msg = newNormalMessage(protoutil.MarshalOrPanic(newMockEnvelope("test message")), uint64(0), uint64(0))
			fragments = chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 0)
			assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
			assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
			respSyncChan <- struct{}{} // sync to make sure the message is received by processMessages

			select {
			case block := <-mockSupport.Blocks:
				metadata := &cb.Metadata{}
				proto.Unmarshal(block.Metadata.Metadata[cb.BlockMetadataIndex_ORDERER], metadata)
				hcsMetadata := &ab.HcsMetadata{}
				proto.Unmarshal(metadata.Value, hcsMetadata)
				assert.Equal(t, lastOriginalSequenceProcessed+1, hcsMetadata.LastOriginalSequenceProcessed)
			case <-time.After(shortTimeout):
				t.Fatal("Expected on block being cut")
			}

			close(chain.haltChan)
			logger.Debug("haltChan closed")
			<-done

			assert.NoError(t, err, "Expected the processMessages call to return without errors")
		})

		t.Run("InvalidDiscard", func(t *testing.T) {
			lastCutBlockNumber := uint64(3)
			lastOriginalSequenceProcessed := uint64(5)
			mockSupport := &mockmultichannel.ConsenterSupport{
				BlockCutterVal:  mockblockcutter.NewReceiver(),
				Blocks:          make(chan *cb.Block),
				ChannelIDVal:    channelNameForTest(t),
				HeightVal:       lastCutBlockNumber,
				SharedConfigVal: newMockOrderer(shortTimeout/2, &goodHcsConfig),
				SequenceVal:     uint64(1),
				ProcessNormalMsgErr: fmt.Errorf("invalid normal message"),
			}
			hcf := newDefaultMockHcsClientFactory()
			chain := newBareMinimumChain(t, lastCutBlockNumber, lastOriginalSequenceProcessed, false, mockSupport, hcf)
			defer close(mockSupport.BlockCutterVal.Block)
			respSyncChan := getRespSyncChan(chain.topicSubscriptionHandle)
			defer close(respSyncChan)
			done := make(chan struct{})

			go func() {
				err = chain.processMessages()
				done <- struct{}{}
			}()

			// config message which old configSeq, should try resubmit and receive error as message is invalidated
			msg := newNormalMessage(protoutil.MarshalOrPanic(newMockEnvelope("test message")), uint64(0), uint64(0))
			fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 0)
			assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
			assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
			respSyncChan <- struct{}{} // sync to make sure the message is received by processMessages

			select {
			case mockSupport.BlockCutterVal.Block <- struct{}{}:
				t.Fatalf("Expected no message committed to blockcutter")
			case <-mockSupport.Blocks:
				t.Fatalf("Expected no block being cut given invalid config message")
			case <-time.After(shortTimeout):
			}

			close(chain.haltChan)
			logger.Debug("haltChan closed")
			<-done

			assert.NoError(t, err, "Expected processMessages call to return without errors")
		})

		t.Run("ValidResubmit", func(t *testing.T) {
			lastCutBlockNumber := uint64(3)
			lastOriginalSequenceProcessed := uint64(0)
			mockSupport := &mockmultichannel.ConsenterSupport{
				BlockCutterVal:  mockblockcutter.NewReceiver(),
				Blocks:          make(chan *cb.Block),
				ChannelIDVal:    channelNameForTest(t),
				HeightVal:       lastCutBlockNumber,
				SharedConfigVal: newMockOrderer(longTimeout, &goodHcsConfig),
				SequenceVal:     uint64(1),
				ConfigSeqVal:    uint64(1),
			}
			hcf := newDefaultMockHcsClientFactory()
			chain := newBareMinimumChain(t, lastCutBlockNumber, lastOriginalSequenceProcessed, false, mockSupport, hcf)
			close(mockSupport.BlockCutterVal.Block)
			respSyncChan := getRespSyncChan(chain.topicSubscriptionHandle)
			defer close(respSyncChan)
			done := make(chan struct{})

			go func() {
				err = chain.processMessages()
				done <- struct{}{}
			}()

			// should cut one block after re-submitted message is processed
			mockSupport.BlockCutterVal.CutNext = true

			// config message which old configSeq, should try resubmit
			sequence := getNextSequenceNumber(chain.topicSubscriptionHandle)
			msg := newNormalMessage(protoutil.MarshalOrPanic(newMockEnvelope("test message")), uint64(0), uint64(0))
			fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 0)
			assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
			assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")

			select {
			case <-mockSupport.Blocks:
				t.Fatalf("Expected no block being cut given message with old configSeq")
			case <-time.After(shortTimeout):
			}

			// WaitReady should not block
			blockIngressMsg(t, false, chain.WaitReady)
			respSyncChan <- struct{}{} // sync to make sure the message is received by processMessages, and unblock the resubmitted message

			select {
			case respSyncChan <-struct{}{}:
			case <-time.After(100 *time.Millisecond):
				t.Fatalf("Expected message is resubmitted")
			}

			consensusClient := chain.topicProducer.(*mockhcs.ConsensusClient)
			assert.Equal(t, 1, consensusClient.SubmitConsensusMessageCallCount(), "Expect SubmitConsensusMessage called once")
			marshalledMsg, _ := consensusClient.SubmitConsensusMessageArgsForCall(0)
			fragment := &ab.HcsMessageFragment{}
			assert.NoError(t, proto.Unmarshal(marshalledMsg, fragment), "Expected data unmarshalled successfully to HcsMessageFragment")
			hcsMessage := &ab.HcsMessage{}
			assert.NoError(t, proto.Unmarshal(fragment.Fragment, hcsMessage), "Expected data unmarshalled successfully to HcsMessage")
			normalMessage := hcsMessage.Type.(*ab.HcsMessage_Regular).Regular
			assert.Equal(t, mockSupport.ConfigSeqVal, normalMessage.ConfigSeq, "Expect configseq to be current")
			assert.Equal(t, sequence, normalMessage.OriginalSeq, "Expect originalSeq to match")

			select {
			case <-mockSupport.Blocks:
			case <-time.After(100 * time.Millisecond):
				t.Fatalf("Expected one block being cut")
			}
			close(chain.haltChan)
			logger.Debug("haltChan closed")
			<-done

			assert.NoError(t, err, "Expected processMessages call to return without errors")
		})
	})

	t.Run("Config", func(t *testing.T) {
		// this test emits a mock re-submitted config message that does not require reprocessing as
		// OriginalSequence <= lastOriginalSequenceProcessed
		t.Run("AlreadyProcessedDiscard", func(t *testing.T) {
			lastCutBlockNumber := uint64(3)
			lastOriginalSequenceProcessed := uint64(5)
			mockSupport := &mockmultichannel.ConsenterSupport{
				BlockCutterVal:  mockblockcutter.NewReceiver(),
				Blocks:          make(chan *cb.Block),
				ChannelIDVal:    channelNameForTest(t),
				HeightVal:       lastCutBlockNumber,
				SharedConfigVal: newMockOrderer(longTimeout, &goodHcsConfig),
				SequenceVal:     uint64(1),
				ConfigSeqVal:    uint64(1),
			}
			hcf := newDefaultMockHcsClientFactory()
			chain := newBareMinimumChain(t, lastCutBlockNumber, lastOriginalSequenceProcessed, false, mockSupport, hcf)
			close(mockSupport.BlockCutterVal.Block)
			respSyncChan := getRespSyncChan(chain.topicSubscriptionHandle)
			defer close(respSyncChan)
			done := make(chan struct{})

			go func() {
				err = chain.processMessages()
				done <- struct{}{}
			}()

			// config message with configseq 0
			msg := newConfigMessage(protoutil.MarshalOrPanic(newMockConfigEnvelope()), uint64(0), lastOriginalSequenceProcessed-1)
			fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 1)
			assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
			assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
			respSyncChan <- struct{}{}

			select {
			case <- mockSupport.Blocks:
				t.Fatalf("Expected no block cut")
			case <-time.After(shortTimeout/2):
			}

			close(chain.haltChan)
			logger.Debug("haltChan closed")
			<-done

			assert.NoError(t, err, "Expected processMessages call to return without errors")
		})

		// scenario, some other orderer resubmitted message at offset X, whereas we didn't. That message was considered
		// invalid by us during re-validation, however some other orderer deemed it to be valid, and thus resubmitted it
		t.Run("Non-determinism", func(t *testing.T) {
			lastCutBlockNumber := uint64(3)
			lastOriginalSequenceProcessed := uint64(4)
			mockSupport := &mockmultichannel.ConsenterSupport{
				BlockCutterVal:  mockblockcutter.NewReceiver(),
				Blocks:          make(chan *cb.Block),
				ChannelIDVal:    channelNameForTest(t),
				HeightVal:       lastCutBlockNumber,
				SharedConfigVal: newMockOrderer(longTimeout, &goodHcsConfig),
				SequenceVal:     uint64(1),
				ConfigSeqVal:    uint64(1),
				ProcessConfigMsgVal: newMockConfigEnvelope(),
			}
			hcf := newDefaultMockHcsClientFactory()
			chain := newBareMinimumChain(t, lastCutBlockNumber, lastOriginalSequenceProcessed, false, mockSupport, hcf)
			close(mockSupport.BlockCutterVal.Block)
			respSyncChan := getRespSyncChan(chain.topicSubscriptionHandle)
			defer close(respSyncChan)
			done := make(chan struct{})

			go func() {
				err = chain.processMessages()
				done <- struct{}{}
			}()

			// check that WaitReady is not blocked at beginning
			blockIngressMsg(t, false, chain.WaitReady)

			mockSupport.ProcessConfigMsgErr = fmt.Errorf("invalid message found during revalidation")

			// emits a config message with lagged config sequence
			msg := newConfigMessage(protoutil.MarshalOrPanic(newMockConfigEnvelope()), uint64(0), uint64(0))
			fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 1)
			assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
			assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
			respSyncChan <- struct{}{}
			select {
			case <-mockSupport.Blocks:
				t.Fatalf("Expected no block being cut")
			case <-time.After(shortTimeout/2):
			}

			// check that WaitReady is still not blocked
			blockIngressMsg(t, false, chain.WaitReady)

			// some other orderer resubmitted the message
			// emits a config message with lagged config sequence
			msg = newConfigMessage(protoutil.MarshalOrPanic(newMockConfigEnvelope()), mockSupport.SequenceVal, lastOriginalSequenceProcessed+1)
			fragments = chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 1)
			assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
			assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
			respSyncChan <- struct{}{}

			select {
			case block := <-mockSupport.Blocks:
				metadata, err := protoutil.GetMetadataFromBlock(block, cb.BlockMetadataIndex_ORDERER)
				assert.NoError(t, err, "Expected get metadata from block successful")
				hcsMetadata := &ab.HcsMetadata{}
				assert.NoError(t, proto.Unmarshal(metadata.Value, hcsMetadata), "Expected unmarsal into HcsMetadata successful")

				assert.Equal(t, lastOriginalSequenceProcessed+1, hcsMetadata.LastResubmittedConfigSequence, "Expected lastResubmittedConfigSequence correct")
				assert.Equal(t, lastOriginalSequenceProcessed+1, hcsMetadata.LastOriginalSequenceProcessed, "Expected LastOriginalSequenceProcessed correct")
			case <-time.After(shortTimeout/2):
				t.Fatalf("Expected one block being cut")
			}

			close(chain.haltChan)
			logger.Debug("haltChan closed")
			<-done

			assert.NoError(t, err, "Expected processMessages call to return without error")
		})


		t.Run("ResubmittedMsgStillBehind", func(t *testing.T) {
			lastCutBlockNumber := uint64(3)
			lastOriginalSequenceProcessed := uint64(4)
			mockSupport := &mockmultichannel.ConsenterSupport{
				BlockCutterVal:  mockblockcutter.NewReceiver(),
				Blocks:          make(chan *cb.Block),
				ChannelIDVal:    channelNameForTest(t),
				HeightVal:       lastCutBlockNumber,
				SharedConfigVal: newMockOrderer(longTimeout, &goodHcsConfig),
				SequenceVal:     uint64(2),
				ConfigSeqVal:    uint64(2),
				ProcessConfigMsgVal: newMockConfigEnvelope(),
			}
			hcf := newDefaultMockHcsClientFactory()
			chain := newBareMinimumChain(t, lastCutBlockNumber, lastOriginalSequenceProcessed, true, mockSupport, hcf)
			setNextSequenceNumber(chain.topicSubscriptionHandle, lastOriginalSequenceProcessed+2)
			close(mockSupport.BlockCutterVal.Block)
			respSyncChan := getRespSyncChan(chain.topicSubscriptionHandle)
			defer close(respSyncChan)
			done := make(chan struct{})

			go func() {
				err = chain.processMessages()
				done <- struct{}{}
			}()

			// check that WaitReady is not blocked at beginning
			blockIngressMsg(t, true, chain.WaitReady)

			// emits a resubmitted config message with lagged config sequence
			msg := newConfigMessage(protoutil.MarshalOrPanic(newMockConfigEnvelope()), mockSupport.SequenceVal-1, lastOriginalSequenceProcessed+1)
			fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 1)
			assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
			assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
			select {
			case <-mockSupport.Blocks:
				t.Fatalf("Expected no block being cut")
			case <-time.After(shortTimeout/2):
			}

			// should still block since resubmitted config message is still behind current config seq
			blockIngressMsg(t, true, chain.WaitReady)
			respSyncChan <- struct{}{} // unblock topicSubscriptionHandle so the next resubmission will go through

			select {
			case <-mockSupport.Blocks:
			case <-time.After(shortTimeout/2):
				t.Fatalf("Expected block being cut")
			}
			respSyncChan <- struct{}{}

			// should no longer block
			blockIngressMsg(t, false, chain.WaitReady)

			close(chain.haltChan)
			logger.Debug("haltChan closed")
			<-done

			assert.NoError(t, err, "Expected processMessages call to return without errors")
		})

		t.Run("InvalidDiscard", func(t *testing.T) {
			lastCutBlockNumber := uint64(3)
			lastOriginalSequenceProcessed := uint64(4)
			mockSupport := &mockmultichannel.ConsenterSupport{
				BlockCutterVal:  mockblockcutter.NewReceiver(),
				Blocks:          make(chan *cb.Block),
				ChannelIDVal:    channelNameForTest(t),
				HeightVal:       lastCutBlockNumber,
				SharedConfigVal: newMockOrderer(longTimeout, &goodHcsConfig),
				SequenceVal:     uint64(1),
				ConfigSeqVal: uint64(1),
				ProcessConfigUpdateMsgErr: fmt.Errorf("invalid config message"),
			}
			hcf := newDefaultMockHcsClientFactory()
			chain := newBareMinimumChain(t, lastCutBlockNumber, lastOriginalSequenceProcessed, false, mockSupport, hcf)
			close(mockSupport.BlockCutterVal.Block)
			respSyncChan := getRespSyncChan(chain.topicSubscriptionHandle)
			defer close(respSyncChan)
			done := make(chan struct{})

			go func() {
				err = chain.processMessages()
				done <- struct{}{}
			}()

			// WaitReady should not be blocked
			blockIngressMsg(t, false, chain.WaitReady)

			// emits a config message with lagged configSeq, later it'll be invalidated
			msg := newConfigMessage(protoutil.MarshalOrPanic(newMockConfigEnvelope()), mockSupport.SequenceVal-1, uint64(0))
			fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 1)
			assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
			assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
			select {
			case <-mockSupport.Blocks:
				t.Fatalf("Expected no block being cut")
			case <-time.After(shortTimeout/2):
			}
			respSyncChan <- struct{}{}

			close(chain.haltChan)
			logger.Debug("haltChan closed")
			<-done

			assert.NoError(t, err, "Expected processMessages call to return without errors")
		})

		t.Run("ValidResumbit", func(t *testing.T) {
			lastCutBlockNumber := uint64(3)
			lastOriginalSequenceProcessed := uint64(4)
			mockSupport := &mockmultichannel.ConsenterSupport{
				BlockCutterVal:  mockblockcutter.NewReceiver(),
				Blocks:          make(chan *cb.Block),
				ChannelIDVal:    channelNameForTest(t),
				HeightVal:       lastCutBlockNumber,
				SharedConfigVal: newMockOrderer(longTimeout, &goodHcsConfig),
				SequenceVal:     uint64(1),
				ConfigSeqVal:    uint64(1),
				ProcessConfigMsgVal: newMockConfigEnvelope(),
			}
			hcf := newDefaultMockHcsClientFactory()
			chain := newBareMinimumChain(t, lastCutBlockNumber, lastOriginalSequenceProcessed, false, mockSupport, hcf)
			setNextSequenceNumber(chain.topicSubscriptionHandle, lastOriginalSequenceProcessed+2)
			close(mockSupport.BlockCutterVal.Block)
			close(getRespSyncChan(chain.topicSubscriptionHandle))
			done := make(chan struct{})

			// intercept the SubmitConsensusMessage call
			consensusClient := chain.topicProducer.(*mockhcs.ConsensusClient)
			oldStub := consensusClient.SubmitConsensusMessageStub
			consensusClient.SubmitConsensusMessageCalls(nil)
			consensusClient.SubmitConsensusMessageReturns(nil)

			go func() {
				err = chain.processMessages()
				done <- struct{}{}
			}()

			// check that WaitReady is not blocked at beginning
			blockIngressMsg(t, false, chain.WaitReady)

			// emits a config message with lagged sequence
			msg := newConfigMessage(protoutil.MarshalOrPanic(newMockConfigEnvelope()), mockSupport.SequenceVal-1, uint64(0))
			fragments := chain.fragmenter.makeFragments(protoutil.MarshalOrPanic(msg), "test fragment key", 1)
			assert.Equal(t, 1, len(fragments), "Expect one fragment created from test message")
			assert.NoError(t, hcf.InjectMessage(chain.topicId, protoutil.MarshalOrPanic(fragments[0])), "Expect message injected successfully")
			select {
			case <-mockSupport.Blocks:
				t.Fatalf("Expected no block being cut")
			case <-time.After(shortTimeout/2):
			}

			assert.Equal(t, 1, consensusClient.SubmitConsensusMessageCallCount(), "Expected SubmitConsensusMessage called once")
			data, topicId := consensusClient.SubmitConsensusMessageArgsForCall(0)

			// WaitReady should be blocked now,
			blockIngressMsg(t, true, chain.WaitReady)

			// now send the resubmitted config message
			assert.NoError(t, oldStub(data, topicId), "Expected SubmitConsensusMessage returns without errors")

			select {
			case <-mockSupport.Blocks:
			case <-time.After(shortTimeout/2):
				t.Fatalf("Expected block being cut")
			}

			// WaitReady is unblocked
			blockIngressMsg(t, false, chain.WaitReady)

			close(chain.haltChan)
			logger.Debug("haltChan closed")
			<-done

			assert.NoError(t, err, "Expected processMessages call to return without errors")
		})
	})
}

func TestParseConfig(t *testing.T) {
	mockHcsConfig := localconfig.Hcs{
		Nodes:             map[string]string{"127.0.0.1:50211": "0.0.3", "127.0.0.2:50211": "0.0.4"},
		MirrorNodeAddress: "127.0.0.5:5600",
		Operator: localconfig.HcsOperator{
			Id: "0.0.19882",
			PrivateKey: localconfig.HcsPrivateKey{
				Enabled: true,
				Type:    "ed25519",
				Key:     "302e020100300506032b657004220420e373811ccb438637a4358db3cbb72dd899eeda6b764c0b8128c61063752b4fe4",
			},
		},
	}
	mockSupport := mockmultichannel.ConsenterSupport{
		ChannelIDVal:     "mock-channel",
		HeightVal:        uint64(0),
		SharedConfigVal:  newMockOrderer(shortTimeout, &goodHcsConfig),
		ChannelConfigVal: newMockChannel(),
	}

	t.Run("WithValidConfig", func(t *testing.T) {
		chain := &chainImpl{consenter: &consenterImpl{&mockHcsConfig}, ConsenterSupport: &mockSupport}

		assert.NotPanics(t, func() { parseConfig(chain) }, "Expect no panics")
		assert.NotNil(t, chain.network, "Expect non-nil chain.network")
		assert.Equal(t, len(mockHcsConfig.Nodes), len(chain.network), "Expect chain.network has correct number of entries")
		assert.Equal(t, mockHcsConfig.Operator.Id, chain.operatorId.String(), "Expect correct operator ID string")
		assert.Equal(t, mockHcsConfig.Operator.PrivateKey.Key, chain.operatorPrivateKey.String(), "Expect correct operator private key")
	})

	t.Run("WithEmptyNodes", func(t *testing.T) {
		invalidMockHcsConfig := mockHcsConfig
		invalidMockHcsConfig.Nodes = make(map[string]string)
		chain := &chainImpl{consenter: &consenterImpl{&invalidMockHcsConfig}, ConsenterSupport: &mockSupport}
		assert.Panics(t, func() { parseConfig(chain) }, "Expect panic when Nodes is empty")
	})

	t.Run("WithInvalidAccountIDInNodes", func(t *testing.T) {
		invalidMockHcsConfig := mockHcsConfig
		invalidMockHcsConfig.Nodes = map[string]string{
			"127.0.0.1:50211": "0.0.3",
			"127.0.0.2:50211": "invalid account id",
		}
		chain := &chainImpl{consenter: &consenterImpl{&invalidMockHcsConfig}, ConsenterSupport: &mockSupport}
		assert.Panics(t, func() { parseConfig(chain) }, "Expect panic when account ID in Nodes is invalid")
	})

	t.Run("WithInvalidOperatorID", func(t *testing.T) {
		invalidMockHcsConfig := mockHcsConfig
		invalidMockHcsConfig.Operator.Id = "invalid operator id"
		chain := &chainImpl{consenter: &consenterImpl{&invalidMockHcsConfig}, ConsenterSupport: &mockSupport}
		assert.Panics(t, func() { parseConfig(chain) }, "Expect panic with invalid operator ID")
	})

	t.Run("WithInvalidPrivateKey", func(t *testing.T) {
		invalidMockHcsConfig := mockHcsConfig
		invalidMockHcsConfig.Operator.PrivateKey.Key = "invalid key string"
		chain := &chainImpl{consenter: &consenterImpl{&invalidMockHcsConfig}, ConsenterSupport: &mockSupport}
		assert.Panics(t, func() { parseConfig(chain) }, "Expect panic with invalid operator private key")
	})

	t.Run("WithInvalidHCSTopicID", func(t *testing.T) {
		invalidMockSupport := mockSupport
		invalidMockSupport.SharedConfigVal.Hcs().TopicId = "invalid topic id"
		chain := &chainImpl{consenter: &consenterImpl{&mockHcsConfig}, ConsenterSupport: &invalidMockSupport}
		assert.Panics(t, func() { parseConfig(chain) }, "Expect panic with invalid HCS topic ID")
	})
}

func TestNewConfigMessage(t *testing.T) {
	data := []byte("test message")
	configSeq := uint64(3)
	originalSeq := uint64(8)
	msg := newConfigMessage(data, configSeq, originalSeq)
	assert.IsType(t, &ab.HcsMessage_Regular{}, msg.Type, "Expected message type to be HcsMessage_Regular")
	regular := msg.Type.(*ab.HcsMessage_Regular)
	assert.IsType(t, &ab.HcsMessageRegular{}, regular.Regular, "Expected message type to be HcsMessageRegular")
	config := regular.Regular
	assert.Equal(t, data, config.Payload, "Expected payload to match")
	assert.Equal(t, configSeq, config.ConfigSeq, "Expected configSeq to match")
	assert.Equal(t, ab.HcsMessageRegular_CONFIG, config.Class, "Expected Class to be CONFIG")
	assert.Equal(t, originalSeq, config.OriginalSeq, "Expected OriginalSeq to match")
}

func TestNewNormalMessage(t *testing.T) {
	data := []byte("test message")
	configSeq := uint64(3)
	originalSeq := uint64(8)
	msg := newNormalMessage(data, configSeq, originalSeq)
	assert.IsType(t, &ab.HcsMessage_Regular{}, msg.Type, "Expected message type to be HcsMessage_Regular")
	regular := msg.Type.(*ab.HcsMessage_Regular)
	assert.IsType(t, &ab.HcsMessageRegular{}, regular.Regular, "Expected message type to be HcsMessageRegular")
	config := regular.Regular
	assert.Equal(t, data, config.Payload, "Expected payload to match")
	assert.Equal(t, configSeq, config.ConfigSeq, "Expected configSeq to match")
	assert.Equal(t, ab.HcsMessageRegular_NORMAL, config.Class, "Expected Class to be NORMAL")
	assert.Equal(t, originalSeq, config.OriginalSeq, "Expected OriginalSeq to match")
}

func TestNewTimeToCutMessage(t *testing.T) {
	blockNumber := uint64(9)
	msg := newTimeToCutMessage(blockNumber)
	assert.IsType(t, &ab.HcsMessage_TimeToCut{}, msg.Type, "Expected message type to be HcsMessage_TimeToCut")
	regular := msg.Type.(*ab.HcsMessage_TimeToCut)
	assert.IsType(t, &ab.HcsMessageTimeToCut{}, regular.TimeToCut, "Expected message type to be HcsMessageTimeToCut")
	ttc := regular.TimeToCut
	assert.Equal(t, blockNumber, ttc.BlockNumber, "Expected blockNumber to match")
}

func TestNewHcsMetadata(t *testing.T) {
	lastConsensusTimestampPersisted := ptypes.TimestampNow()
	lastOriginalSequenceProcessed := uint64(12)
	lastResubmittedConfigSequence := uint64(25)
	lastFragmentId := uint64(7)
	metadata := newHcsMetadata(
		lastConsensusTimestampPersisted,
		lastOriginalSequenceProcessed,
		lastResubmittedConfigSequence,
		lastFragmentId,
	)
	assert.Equal(t, lastConsensusTimestampPersisted, metadata.LastConsensusTimestampPersisted, "Exepcted correct LastConsensusTimestampPersisted")
	assert.Equal(t, lastOriginalSequenceProcessed, metadata.LastOriginalSequenceProcessed, "Expected correct LastOriginalSequenceProcessed")
	assert.Equal(t, lastResubmittedConfigSequence, metadata.LastResubmittedConfigSequence, "Expected correct LastResubmittedConfigSequence")
	assert.Equal(t, lastFragmentId, metadata.LastFragmentId, "Expected correct LastFragmentId")
}

func TestTimestampProtoOrPanic(t *testing.T) {
	t.Run("Proper", func(t *testing.T) {
		var ts *timestamp.Timestamp
		assert.NotPanics(t, func() { ts = timestampProtoOrPanic(&unixEpoch) }, "Expected no panic with valid time")
		assert.Equal(t, unixEpoch.Second(), int(ts.Seconds), "Expected seconds equal")
		assert.Equal(t, unixEpoch.Nanosecond(), int(ts.Nanos), "Expected nanoseconds equal")
	})

	t.Run("NilTime", func(t *testing.T) {
		invalidTime := time.Time{}.Add(-100 * time.Hour)
		assert.Panics(t, func() { timestampProtoOrPanic(&invalidTime) }, "Expected panic with nil passed in")
	})
}

func TestGCMCipher(t *testing.T) {
	makeTempFileWithData := func(t *testing.T, size uint32) string {
		f, err := ioutil.TempFile("/tmp", "test")
		assert.NoError(t, err, "Expect temp file successfully created")
		defer f.Close()
		if size != 0 {
			data := make([]byte, size)
			rand.Read(data)
			f.Write(data)
		}
		return f.Name()
	}

	t.Run("WithNonExistFile", func(t *testing.T) {
		name := makeTempFileWithData(t, 0)
		os.Remove(name)

		cipher := makeGCMCipher(name)
		assert.Nil(t, cipher, "Expect cipher not created when key file does not exist")
	})

	t.Run("WithEmptyFile", func(t *testing.T) {
		name := makeTempFileWithData(t, 0)
		defer os.Remove(name)
		assert.Panics(t, func() { makeGCMCipher(name) }, "Expect panic when file is empty")
	})

	t.Run("WithValidKeyFile", func(t *testing.T) {
		name := makeTempFileWithData(t, 32)
		defer os.Remove(name)
		cipher := makeGCMCipher(name)
		assert.NotNil(t, cipher, "Expect cipher created")
	})

	t.Run("EncryptDecrypt", func(t *testing.T) {
		name := makeTempFileWithData(t, 32)
		defer os.Remove(name)
		cipher := makeGCMCipher(name)
		chain := &chainImpl{
			gcmCipher:   cipher,
			nonceReader: crand.Reader,
		}

		plaintext := []byte("this is a simple plaintext")
		ciphertext, err := chain.encrypt(plaintext)
		assert.NoError(t, err, "Expect encrypt successfully")

		recovered, err := chain.decrypt(ciphertext)
		assert.NoError(t, err, "Expect decrypt successfully")
		assert.Equal(t, plaintext, recovered, "Expect plaintext and recovered plaintext the same")

		ciphertext = append(ciphertext, 0xde, 0xad, 0xbe, 0xef)
		recovered, err = chain.decrypt(ciphertext)
		assert.Errorf(t, err, "Expect error decrypting corrupted data")
		assert.Nil(t, recovered, "Expect nil decrypting corrupted data")
	})

	t.Run("EncryptWithBadNonceGenerator", func(t *testing.T) {
		name := makeTempFileWithData(t, 32)
		defer os.Remove(name)
		cipher := makeGCMCipher(name)
		chain := &chainImpl{
			gcmCipher:   cipher,
			nonceReader: &badReader{},
		}

		ciphertext, err := chain.encrypt([]byte("this is a simple plaintext"))
		assert.Error(t, err, "Expect err with bad reader")
		assert.Nil(t, ciphertext, "Expect nil ciphertext with bad reader")
	})
}

// Test helper functions

type mockHcsTransport struct {
	l        sync.Mutex
	channels map[hedera.ConsensusTopicID]chan []byte
}

func (t *mockHcsTransport) tryGetTransportW(topicId hedera.ConsensusTopicID) chan<- []byte {
	return t.getTransport(topicId, false)
}

func (t *mockHcsTransport) getTransportW(topicId hedera.ConsensusTopicID) chan<- []byte {
	return t.getTransport(topicId, true)
}

func (t *mockHcsTransport) getTransportR(topicId hedera.ConsensusTopicID) <-chan []byte {
	return t.getTransport(topicId, true)
}

func (t *mockHcsTransport) getTransport(topicId hedera.ConsensusTopicID, create bool) chan []byte {
	t.l.Lock()
	defer t.l.Unlock()

	ch, ok := t.channels[topicId]
	if !ok && create {
		ch = make(chan []byte)
		t.channels[topicId] = ch
	}
	return ch
}

func newMockHcsTransport() *mockHcsTransport {
	return &mockHcsTransport{channels: make(map[hedera.ConsensusTopicID]chan []byte)}
}

type hcsClientFactoryWithRecord struct {
	mockhcs.HcsClientFactory
	transport    *mockHcsTransport
	returnValues map[string][]interface{}
	l            sync.Mutex
}

func (f *hcsClientFactoryWithRecord) GetReturnValues() map[string][]interface{} {
	f.l.Lock()
	defer f.l.Unlock()

	dup := map[string][]interface{}{}
	for key, value := range f.returnValues {
		dup[key] = value
	}
	return dup
}

func (f *hcsClientFactoryWithRecord) InjectMessage(topicId hedera.ConsensusTopicID, msg []byte) error {
	if ch := f.transport.tryGetTransportW(topicId); ch == nil {
		return fmt.Errorf("transport for topic %v does not exist", topicId)
	} else {
		ch <- msg
		return nil
	}
}

func newDefaultMockHcsClientFactory() *hcsClientFactoryWithRecord {
	return newMockHcsClientFactory(nil, nil)
}

func newMockHcsClientFactory(
	getConsensusClient func(map[string]hedera.AccountID, hedera.AccountID, hedera.Ed25519PrivateKey) (factory.ConsensusClient, error),
	getMirrorClient func(string) (factory.MirrorClient, error),
) *hcsClientFactoryWithRecord {
	mock := &hcsClientFactoryWithRecord{transport: newMockHcsTransport(), returnValues: make(map[string][]interface{})}

	recordReturnValue := func(key string, returnValues []interface{}) {
		mock.l.Lock()
		if mock.returnValues == nil {
			mock.returnValues = map[string][]interface{}{}
		}
		if mock.returnValues[key] == nil {
			mock.returnValues[key] = []interface{}{}
		}
		mock.returnValues[key] = append(mock.returnValues[key], returnValues)
		mock.l.Unlock()
	}
	defaultGetConsensusClient := func(network map[string]hedera.AccountID, account hedera.AccountID, key hedera.Ed25519PrivateKey) (client factory.ConsensusClient, err error) {
		cs := mockhcs.ConsensusClient{}
		cs.CloseCalls(func() error {
			return nil
		})
		cs.SubmitConsensusMessageCalls(func(message []byte, topicId hedera.ConsensusTopicID) error {
			if message == nil {
				return errors.Errorf("message is nil")
			}
			ch := mock.transport.getTransportW(topicId)
			ch <- message
			return nil
		})
		client = &cs
		return client, err
	}
	defaultGetMirrorClient := func(endpoint string) (client factory.MirrorClient, err error) {
		mc := mockhcs.MirrorClient{}
		mc.CloseCalls(func() error {
			return nil
		})
		mc.SubscribeTopicCalls(func(
			topicId hedera.ConsensusTopicID,
			start *time.Time,
			end *time.Time,
		) (factory.MirrorSubscriptionHandle, error) {
			transport := mock.transport.getTransportR(topicId)
			handle := newMockMirrorSubscriptionHandle(transport)
			handle.start()
			return handle, nil
		})
		client = &mc
		return client, err
	}

	innerGetConsensusClient := defaultGetConsensusClient
	if getConsensusClient != nil {
		innerGetConsensusClient = getConsensusClient
	}
	getConsensusClientWithRecord := func(network map[string]hedera.AccountID, account hedera.AccountID, key hedera.Ed25519PrivateKey) (client factory.ConsensusClient, err error) {
		defer func() {
			recordReturnValue(GetConsensusClientFuncName, []interface{}{client, err})
		}()
		client, err = innerGetConsensusClient(network, account, key)
		return client, err
	}

	innerGetMirrorClient := defaultGetMirrorClient
	if getMirrorClient != nil {
		innerGetMirrorClient = getMirrorClient
	}
	getMirrorClientWithRecord := func(endpoint string) (client factory.MirrorClient, err error) {
		defer func() {
			recordReturnValue(GetMirrorClientFuncName, []interface{}{client, err})
		}()
		client, err = innerGetMirrorClient(endpoint)
		return client, err
	}

	mock.GetConsensusClientCalls(getConsensusClientWithRecord)
	mock.GetMirrorClientCalls(getMirrorClientWithRecord)
	return mock
}

type mockMirrorSubscriptionHandle struct {
	transport          <-chan []byte
	respChan           chan *hedera.MirrorConsensusTopicResponse
	errChan            chan error
	done               chan struct{}
	nextSequenceNumber     uint64
	nextConsensusTimestamp time.Time
	respSyncChan       chan struct{}
}

func (h *mockMirrorSubscriptionHandle) start() {
	go func() {
		for {
			select {
			case msg, ok := <-h.transport:
				if !ok {
					h.errChan <- fmt.Errorf("transport error")
					return
				}
				// build consensus response
				resp := hedera.MirrorConsensusTopicResponse{
					ConsensusTimeStamp: h.nextConsensusTimestamp,
					Message:            msg,
					RunningHash:        nil,
					SequenceNumber:     h.nextSequenceNumber,
				}
				h.respChan <- &resp
				<-h.respSyncChan
				h.nextConsensusTimestamp = h.nextConsensusTimestamp.Add(time.Nanosecond)
				h.nextSequenceNumber++
			case <-h.done:
				h.errChan <- fmt.Errorf("subscripton is cancelled by caller")
				return
			}
		}
	}()
}

func (h *mockMirrorSubscriptionHandle) Unsubscribe() {
	close(h.done)
}

func (h *mockMirrorSubscriptionHandle) Responses() <-chan *hedera.MirrorConsensusTopicResponse {
	return h.respChan
}

func (h *mockMirrorSubscriptionHandle) Errors() <-chan error {
	return h.errChan
}

func newMockMirrorSubscriptionHandle(transport <-chan []byte) *mockMirrorSubscriptionHandle {
	return &mockMirrorSubscriptionHandle{
		transport:    transport,
		respChan:     make(chan *hedera.MirrorConsensusTopicResponse),
		errChan:      make(chan error),
		done:         make(chan struct{}),
		respSyncChan: make(chan struct{}),
		nextConsensusTimestamp: time.Now(),
		nextSequenceNumber: uint64(1),
	}
}

func getNextSequenceNumber(handle factory.MirrorSubscriptionHandle) uint64 {
	mockHandle := handle.(*mockMirrorSubscriptionHandle)
	return mockHandle.nextSequenceNumber
}

func setNextSequenceNumber(handle factory.MirrorSubscriptionHandle, sequence uint64) {
	mockHandle := handle.(*mockMirrorSubscriptionHandle)
	mockHandle.nextSequenceNumber = sequence
}

func getNextConsensusTimestamp(handle factory.MirrorSubscriptionHandle) time.Time {
	mockHandle := handle.(*mockMirrorSubscriptionHandle)
	return mockHandle.nextConsensusTimestamp
}

func getRespSyncChan(handle factory.MirrorSubscriptionHandle) chan struct{} {
	mockHandle := handle.(*mockMirrorSubscriptionHandle)
	return mockHandle.respSyncChan
}

type badReader struct{}

func (reader *badReader) Read(p []byte) (int, error) {
	return 0, errors.New("bad read request")
}

func newMockEnvelope(content string) *cb.Envelope {
	return &cb.Envelope{Payload: protoutil.MarshalOrPanic(&cb.Payload{
		Header: &cb.Header{ChannelHeader: protoutil.MarshalOrPanic(&cb.ChannelHeader{ChannelId: "foo"})},
		Data:   []byte(content),
	})}
}

func newMockConfigEnvelope() *cb.Envelope {
	return &cb.Envelope{Payload: protoutil.MarshalOrPanic(&cb.Payload{
		Header: &cb.Header{ChannelHeader: protoutil.MarshalOrPanic(
			&cb.ChannelHeader{Type: int32(cb.HeaderType_CONFIG), ChannelId: "foo"})},
		Data: protoutil.MarshalOrPanic(&cb.ConfigEnvelope{}),
	})}
}

func extractEncodedConsensusTimestamp(marshalledOrdererMetadata []byte) *timestamp.Timestamp {
	omd := &cb.Metadata{}
	_ = proto.Unmarshal(marshalledOrdererMetadata, omd)
	metadata := &ab.HcsMetadata{}
	_ = proto.Unmarshal(omd.GetValue(), metadata)
	return metadata.LastConsensusTimestampPersisted
}
