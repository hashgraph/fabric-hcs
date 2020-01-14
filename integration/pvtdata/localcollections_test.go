/*
Copyright IBM Corp All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package e2e

import (
	"path/filepath"

	"github.com/hyperledger/fabric/integration/nwo"
	"github.com/hyperledger/fabric/integration/nwo/commands"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
	"github.com/tedsuo/ifrit"
)

var _ bool = FDescribe("LocalCollections", func() {
	var (
		testDir string
		network *nwo.Network
		process ifrit.Process
		orderer *nwo.Orderer
	)
	BeforeEach(func() {
		testDir, network, process, orderer, _ = initThreeOrgsSetupAndVerifyMembership(false)
	})

	AfterEach(func() {
		testCleanup(testDir, network, process)
	})

	It("using local collections with a chaincode that has collections defined", func() {
		By("installing and instantiating chaincode on all peers")
		chaincode := nwo.Chaincode{
			Name:              "pvtdatacc",
			Version:           "1.0",
			Path:              "github.com/hyperledger/fabric/integration/chaincode/simplepvtdata/cmd",
			Ctor:              `{"Args":["init"]}`,
			Policy:            `OR ('Org1MSP.member','Org2MSP.member', 'Org3MSP.member')`,
			CollectionsConfig: filepath.Join("testdata", "collection_configs", "collections_config1.json"),
		}
		nwo.DeployChaincode(network, "testchannel", orderer, chaincode)

		By("doing a put on peer0.org2")
		invokeChaincode(network, "org2", "peer0", "pvtdatacc", `{"Args":["put","+local","foo","bar1"]}`, "testchannel", orderer)

		By("expecting a successful query return from peer0.org2")
		query(network, network.Peer("org2", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 0, false, "bar1")

		By("expecting a failed query return from peer0.org1")
		query(network, network.Peer("org1", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 1, true, "private data matching public hash version is not available")

		By("expecting a failed query return from peer0.org3")
		query(network, network.Peer("org3", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 1, true, "private data matching public hash version is not available")

		By("doing a put on peer0.org1")
		invokeChaincode(network, "org1", "peer0", "pvtdatacc", `{"Args":["put","+local","foo","bar2"]}`, "testchannel", orderer)

		By("expecting a successful query return from peer0.org1")
		query(network, network.Peer("org1", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 0, false, "bar2")

		By("expecting a failed query return from peer0.org2")
		query(network, network.Peer("org2", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 1, true, "private data matching public hash version is not available")

		By("expecting a failed query return from peer0.org3")
		query(network, network.Peer("org3", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 1, true, "private data matching public hash version is not available")

		By("doing a put on peer0.org1 and peer0.org2")
		sess, err := network.PeerUserSession(network.Peer("org1", "peer0"), "User1", commands.ChaincodeInvoke{
			ChannelID: "testchannel",
			Orderer:   network.OrdererAddress(orderer, nwo.ListenPort),
			Name:      "pvtdatacc",
			Ctor:      `{"Args":["put", "+local","foo","bar3"]}`,
			PeerAddresses: []string{
				network.PeerAddress(network.Peer("org1", "peer0"), nwo.ListenPort),
				network.PeerAddress(network.Peer("org2", "peer0"), nwo.ListenPort),
			},

			WaitForEvent: true,
		})
		Expect(err).NotTo(HaveOccurred())
		Eventually(sess, network.EventuallyTimeout).Should(gexec.Exit(0))
		Expect(sess.Err).To(gbytes.Say("Chaincode invoke successful."))

		By("expecting a successful query return from peer0.org1")
		query(network, network.Peer("org1", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 0, false, "bar3")

		By("expecting a successful query return from peer0.org2")
		query(network, network.Peer("org2", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 0, false, "bar3")

		By("expecting a failed query return from peer0.org3")
		query(network, network.Peer("org3", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 1, true, "private data matching public hash version is not available")

		By("peer1.org2 joins the channel")
		org2peer1 := network.Peer("org2", "peer1")
		network.JoinChannel("testchannel", orderer, org2peer1)
		org2peer1.Channels = append(org2peer1.Channels, &nwo.PeerChannel{Name: "testchannel", Anchor: false})

		By("make sure all peers have the same ledger height")
		expectedPeers := []*nwo.Peer{
			network.Peer("org1", "peer0"),
			network.Peer("org2", "peer0"),
			network.Peer("org2", "peer1"),
			network.Peer("org3", "peer0")}
		waitUntilAllPeersSameLedgerHeight(network, expectedPeers, "testchannel", getLedgerHeight(network, network.Peer("org1", "peer0"), "testchannel"))

		By("install chaincode on peer1.org2 to be able to query it")
		nwo.InstallChaincode(network, chaincode, org2peer1)

		By("expecting a failed query return from peer1.org2")
		query(network, network.Peer("org2", "peer1"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 1, true, "private data matching public hash version is not available")

		By("doing a put on peer0.org2 and peer1.org2")
		sess, err = network.PeerUserSession(network.Peer("org1", "peer0"), "User1", commands.ChaincodeInvoke{
			ChannelID: "testchannel",
			Orderer:   network.OrdererAddress(orderer, nwo.ListenPort),
			Name:      "pvtdatacc",
			Ctor:      `{"Args":["put", "+local","foo","bar4"]}`,
			PeerAddresses: []string{
				network.PeerAddress(network.Peer("org2", "peer0"), nwo.ListenPort),
				network.PeerAddress(network.Peer("org2", "peer1"), nwo.ListenPort),
			},

			WaitForEvent: true,
		})
		Expect(err).NotTo(HaveOccurred())
		Eventually(sess, network.EventuallyTimeout).Should(gexec.Exit(0))
		Expect(sess.Err).To(gbytes.Say("Chaincode invoke successful."))

		By("expecting a successful query return from peer0.org2")
		query(network, network.Peer("org2", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 0, false, "bar4")

		By("expecting a successful query return from peer1.org2")
		query(network, network.Peer("org2", "peer1"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 0, false, "bar4")

		By("doing a put on peer0.org2 and peer1.org2")
		sess, err = network.PeerUserSession(network.Peer("org1", "peer0"), "User1", commands.ChaincodeInvoke{
			ChannelID: "testchannel",
			Orderer:   network.OrdererAddress(orderer, nwo.ListenPort),
			Name:      "pvtdatacc",
			Ctor:      `{"Args":["put", "+local","foo","bar5"]}`,
			PeerAddresses: []string{
				network.PeerAddress(network.Peer("org2", "peer0"), nwo.ListenPort),
				network.PeerAddress(network.Peer("org3", "peer0"), nwo.ListenPort),
			},

			WaitForEvent: true,
		})
		Expect(err).NotTo(HaveOccurred())
		Eventually(sess, network.EventuallyTimeout).Should(gexec.Exit(0))
		Expect(sess.Err).To(gbytes.Say("Chaincode invoke successful."))

		By("make sure all peers have the same ledger height")
		expectedPeers = []*nwo.Peer{
			network.Peer("org1", "peer0"),
			network.Peer("org2", "peer0"),
			network.Peer("org2", "peer1"),
			network.Peer("org3", "peer0")}
		waitUntilAllPeersSameLedgerHeight(network, expectedPeers, "testchannel", getLedgerHeight(network, network.Peer("org1", "peer0"), "testchannel"))

		By("expecting a successful query return from peer0.org2")
		query(network, network.Peer("org2", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 0, false, "bar5")

		By("expecting a successful query return from peer0.org3")
		query(network, network.Peer("org3", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 0, false, "bar5")

		By("expecting a failed query return from peer1.org2")
		query(network, network.Peer("org2", "peer1"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 1, true, "private data matching public hash version is not available")

		By("expecting a failed query return from peer0.org1")
		query(network, network.Peer("org1", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 1, true, "private data matching public hash version is not available")

		By("doing a put on peer0.org1 with multiple collections")
		sess, err = network.PeerUserSession(network.Peer("org1", "peer0"), "User1", commands.ChaincodeInvoke{
			ChannelID: "testchannel",
			Orderer:   network.OrdererAddress(orderer, nwo.ListenPort),
			Name:      "pvtdatacc",
			Ctor:      `{"Args":["put","+local","foo","bar6","collectionMarbles","foo","bar7"]}`,
			PeerAddresses: []string{
				network.PeerAddress(network.Peer("org1", "peer0"), nwo.ListenPort),
			},

			WaitForEvent: true,
		})
		Expect(err).NotTo(HaveOccurred())
		Eventually(sess, network.EventuallyTimeout).Should(gexec.Exit(0))
		Expect(sess.Err).To(gbytes.Say("Chaincode invoke successful."))

		By("expecting a successful query return from peer0.org1")
		query(network, network.Peer("org1", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get","+local","foo"]}`, 0, false, "bar6")

		By("expecting a failed query return from peer0.org2")
		query(network, network.Peer("org2", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get","+local","foo"]}`, 1, true, "private data matching public hash version is not available")

		By("expecting a failed query return from peer1.org2")
		query(network, network.Peer("org2", "peer1"), "testchannel", "pvtdatacc", `{"Args":["get","+local","foo"]}`, 1, true, "private data matching public hash version is not available")

		By("expecting a failed query return from peer0.org3")
		query(network, network.Peer("org3", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get","+local","foo"]}`, 1, true, "private data matching public hash version is not available")

		By("expecting a successful query return from peer0.org1")
		query(network, network.Peer("org1", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get","collectionMarbles","foo"]}`, 0, false, "bar7")

		By("expecting a successful query return from peer0.org2")
		query(network, network.Peer("org2", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get","collectionMarbles","foo"]}`, 0, false, "bar7")

		By("expecting a successful query return from peer1.org2")
		query(network, network.Peer("org2", "peer1"), "testchannel", "pvtdatacc", `{"Args":["get","collectionMarbles","foo"]}`, 0, false, "bar7")

		By("expecting a failed query return from peer0.org3")
		query(network, network.Peer("org3", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get","collectionMarbles","foo"]}`, 1, true, "private data matching public hash version is not available")
	})

	It("using local collections with a chaincode that has no collections defined", func() {
		By("installing and instantiating chaincode on all peers")
		chaincode := nwo.Chaincode{
			Name:    "pvtdatacc",
			Version: "1.0",
			Path:    "github.com/hyperledger/fabric/integration/chaincode/simplepvtdata/cmd",
			Ctor:    `{"Args":["init"]}`,
			Policy:  `OR ('Org1MSP.member','Org2MSP.member', 'Org3MSP.member')`,
		}
		nwo.DeployChaincode(network, "testchannel", orderer, chaincode)

		By("doing a put on peer0.org2")
		invokeChaincode(network, "org2", "peer0", "pvtdatacc", `{"Args":["put","+local","foo","bar1"]}`, "testchannel", orderer)

		By("expecting a successful query return from peer0.org2")
		query(network, network.Peer("org2", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 0, false, "bar1")

		By("expecting a failed query return from peer0.org1")
		query(network, network.Peer("org1", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 1, true, "private data matching public hash version is not available")

		By("expecting a failed query return from peer0.org3")
		query(network, network.Peer("org3", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 1, true, "private data matching public hash version is not available")

		By("doing a put on peer0.org1")
		invokeChaincode(network, "org1", "peer0", "pvtdatacc", `{"Args":["put","+local","foo","bar2"]}`, "testchannel", orderer)

		By("expecting a successful query return from peer0.org1")
		query(network, network.Peer("org1", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 0, false, "bar2")

		By("expecting a failed query return from peer0.org2")
		query(network, network.Peer("org2", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 1, true, "private data matching public hash version is not available")

		By("expecting a failed query return from peer0.org3")
		query(network, network.Peer("org3", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 1, true, "private data matching public hash version is not available")

		By("doing a put on peer0.org1 and peer0.org2")
		sess, err := network.PeerUserSession(network.Peer("org1", "peer0"), "User1", commands.ChaincodeInvoke{
			ChannelID: "testchannel",
			Orderer:   network.OrdererAddress(orderer, nwo.ListenPort),
			Name:      "pvtdatacc",
			Ctor:      `{"Args":["put", "+local","foo","bar3"]}`,
			PeerAddresses: []string{
				network.PeerAddress(network.Peer("org1", "peer0"), nwo.ListenPort),
				network.PeerAddress(network.Peer("org2", "peer0"), nwo.ListenPort),
			},

			WaitForEvent: true,
		})
		Expect(err).NotTo(HaveOccurred())
		Eventually(sess, network.EventuallyTimeout).Should(gexec.Exit(0))
		Expect(sess.Err).To(gbytes.Say("Chaincode invoke successful."))

		By("expecting a successful query return from peer0.org1")
		query(network, network.Peer("org1", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 0, false, "bar3")

		By("expecting a successful query return from peer0.org2")
		query(network, network.Peer("org2", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 0, false, "bar3")

		By("expecting a failed query return from peer0.org3")
		query(network, network.Peer("org3", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 1, true, "private data matching public hash version is not available")

		By("peer1.org2 joins the channel")
		org2peer1 := network.Peer("org2", "peer1")
		network.JoinChannel("testchannel", orderer, org2peer1)
		org2peer1.Channels = append(org2peer1.Channels, &nwo.PeerChannel{Name: "testchannel", Anchor: false})

		By("make sure all peers have the same ledger height")
		expectedPeers := []*nwo.Peer{
			network.Peer("org1", "peer0"),
			network.Peer("org2", "peer0"),
			network.Peer("org2", "peer1"),
			network.Peer("org3", "peer0")}
		waitUntilAllPeersSameLedgerHeight(network, expectedPeers, "testchannel", getLedgerHeight(network, network.Peer("org1", "peer0"), "testchannel"))

		By("install chaincode on peer1.org2 to be able to query it")
		nwo.InstallChaincode(network, chaincode, org2peer1)

		By("expecting a failed query return from peer1.org2")
		query(network, network.Peer("org2", "peer1"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 1, true, "private data matching public hash version is not available")

		By("doing a put on peer0.org2 and peer1.org2")
		sess, err = network.PeerUserSession(network.Peer("org1", "peer0"), "User1", commands.ChaincodeInvoke{
			ChannelID: "testchannel",
			Orderer:   network.OrdererAddress(orderer, nwo.ListenPort),
			Name:      "pvtdatacc",
			Ctor:      `{"Args":["put", "+local","foo","bar4"]}`,
			PeerAddresses: []string{
				network.PeerAddress(network.Peer("org2", "peer0"), nwo.ListenPort),
				network.PeerAddress(network.Peer("org2", "peer1"), nwo.ListenPort),
			},

			WaitForEvent: true,
		})
		Expect(err).NotTo(HaveOccurred())
		Eventually(sess, network.EventuallyTimeout).Should(gexec.Exit(0))
		Expect(sess.Err).To(gbytes.Say("Chaincode invoke successful."))

		By("expecting a successful query return from peer0.org2")
		query(network, network.Peer("org2", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 0, false, "bar4")

		By("expecting a successful query return from peer1.org2")
		query(network, network.Peer("org2", "peer1"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 0, false, "bar4")

		By("doing a put on peer0.org2 and peer1.org2")
		sess, err = network.PeerUserSession(network.Peer("org1", "peer0"), "User1", commands.ChaincodeInvoke{
			ChannelID: "testchannel",
			Orderer:   network.OrdererAddress(orderer, nwo.ListenPort),
			Name:      "pvtdatacc",
			Ctor:      `{"Args":["put", "+local","foo","bar5"]}`,
			PeerAddresses: []string{
				network.PeerAddress(network.Peer("org2", "peer0"), nwo.ListenPort),
				network.PeerAddress(network.Peer("org3", "peer0"), nwo.ListenPort),
			},

			WaitForEvent: true,
		})
		Expect(err).NotTo(HaveOccurred())
		Eventually(sess, network.EventuallyTimeout).Should(gexec.Exit(0))
		Expect(sess.Err).To(gbytes.Say("Chaincode invoke successful."))

		By("make sure all peers have the same ledger height")
		expectedPeers = []*nwo.Peer{
			network.Peer("org1", "peer0"),
			network.Peer("org2", "peer0"),
			network.Peer("org2", "peer1"),
			network.Peer("org3", "peer0")}
		waitUntilAllPeersSameLedgerHeight(network, expectedPeers, "testchannel", getLedgerHeight(network, network.Peer("org1", "peer0"), "testchannel"))

		By("expecting a successful query return from peer0.org2")
		query(network, network.Peer("org2", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 0, false, "bar5")

		By("expecting a successful query return from peer0.org3")
		query(network, network.Peer("org3", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 0, false, "bar5")

		By("expecting a failed query return from peer1.org2")
		query(network, network.Peer("org2", "peer1"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 1, true, "private data matching public hash version is not available")

		By("expecting a failed query return from peer0.org1")
		query(network, network.Peer("org1", "peer0"), "testchannel", "pvtdatacc", `{"Args":["get", "+local","foo"]}`, 1, true, "private data matching public hash version is not available")
	})
})

func query(network *nwo.Network, peer *nwo.Peer, chid, ccid, args string, expectedRv int, errorExpected bool, expectedMsg string) {
	sess, err := network.PeerUserSession(peer, "User1", commands.ChaincodeQuery{
		ChannelID: chid,
		Name:      ccid,
		Ctor:      args,
	})
	Expect(err).NotTo(HaveOccurred())
	Eventually(sess, network.EventuallyTimeout).Should(gexec.Exit(expectedRv))

	if errorExpected {
		Expect(sess.Err).To(gbytes.Say(expectedMsg))
	} else {
		Expect(sess).To(gbytes.Say(expectedMsg))
	}
}
