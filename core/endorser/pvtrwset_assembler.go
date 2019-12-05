/*
 *
 * Copyright IBM Corp. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 * /
 *
 */

package endorser

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/cauthdsl"
	"github.com/hyperledger/fabric/core/common/privdata"
	"github.com/hyperledger/fabric/msp/mgmt"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/hyperledger/fabric/protos/transientstore"
	"github.com/pkg/errors"
)

// PvtRWSetAssembler assembles private read write set for distribution
// augments with additional information if needed
type PvtRWSetAssembler interface {
	// AssemblePvtRWSet prepares TxPvtReadWriteSet for distribution
	// augmenting it into TxPvtReadWriteSetWithConfigInfo adding
	// information about collections config available related
	// to private read-write set
	AssemblePvtRWSet(privData *rwset.TxPvtReadWriteSet, txsim CollectionConfigRetriever) (*transientstore.TxPvtReadWriteSetWithConfigInfo, error)
}

// CollectionConfigRetriever encapsulates sub-functionality of ledger.TxSimulator
// to abstract minimum required functions set
type CollectionConfigRetriever interface {
	// GetState gets the value for given namespace and key. For a chaincode, the namespace corresponds to the chaincodeId
	GetState(namespace string, key string) ([]byte, error)
}

type rwSetAssembler struct {
}

// AssemblePvtRWSet prepares TxPvtReadWriteSet for distribution
// augmenting it into TxPvtReadWriteSetWithConfigInfo adding
// information about collections config available related
// to private read-write set
func (as *rwSetAssembler) AssemblePvtRWSet(privData *rwset.TxPvtReadWriteSet, txsim CollectionConfigRetriever) (*transientstore.TxPvtReadWriteSetWithConfigInfo, error) {
	txPvtRwSetWithConfig := &transientstore.TxPvtReadWriteSetWithConfigInfo{
		PvtRwset:          privData,
		CollectionConfigs: make(map[string]*common.CollectionConfigPackage),
	}

	msp := mgmt.GetLocalMSP()
	mspid, err := msp.GetIdentifier()
	if err != nil {
		panic(fmt.Sprintf("GetIdentifier failed with '%s'", err))
	}

	for _, pvtRwset := range privData.NsPvtRwset {
		namespace := pvtRwset.Namespace
		if _, found := txPvtRwSetWithConfig.CollectionConfigs[namespace]; !found {
			cb, err := txsim.GetState("lscc", privdata.BuildCollectionKVSKey(namespace))
			if err != nil {
				return nil, errors.WithMessage(err, fmt.Sprintf("error while retrieving collection config for chaincode %#v", namespace))
			}

			colCP := &common.CollectionConfigPackage{}
			if cb != nil {
				err = proto.Unmarshal(cb, colCP)
				if err != nil {
					return nil, errors.Wrapf(err, "invalid configuration for collection criteria %#v", namespace)
				}
			}

			// we add the local collection definition
			colCP.Config = append(colCP.Config, &common.CollectionConfig{
				Payload: &common.CollectionConfig_StaticCollectionConfig{
					StaticCollectionConfig: &common.StaticCollectionConfig{
						Name: "~local",
						MemberOrgsPolicy: &common.CollectionPolicyConfig{
							Payload: &common.CollectionPolicyConfig_SignaturePolicy{
								SignaturePolicy: cauthdsl.SignedByAnyMember([]string{mspid}),
							},
						},
					},
				},
			})

			txPvtRwSetWithConfig.CollectionConfigs[namespace] = colCP
		}
	}
	as.trimCollectionConfigs(txPvtRwSetWithConfig)
	return txPvtRwSetWithConfig, nil
}

func (as *rwSetAssembler) trimCollectionConfigs(pvtData *transientstore.TxPvtReadWriteSetWithConfigInfo) {
	flags := make(map[string]map[string]struct{})
	for _, pvtRWset := range pvtData.PvtRwset.NsPvtRwset {
		namespace := pvtRWset.Namespace
		for _, col := range pvtRWset.CollectionPvtRwset {
			if _, found := flags[namespace]; !found {
				flags[namespace] = make(map[string]struct{})
			}
			flags[namespace][col.CollectionName] = struct{}{}
		}
	}

	filteredConfigs := make(map[string]*common.CollectionConfigPackage)
	for namespace, configs := range pvtData.CollectionConfigs {
		filteredConfigs[namespace] = &common.CollectionConfigPackage{}
		for _, conf := range configs.Config {
			if colConf := conf.GetStaticCollectionConfig(); colConf != nil {
				if _, found := flags[namespace][colConf.Name]; found {
					filteredConfigs[namespace].Config = append(filteredConfigs[namespace].Config, conf)
				}
			}
		}
	}
	pvtData.CollectionConfigs = filteredConfigs
}
