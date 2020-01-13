/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package simplepvtdata

import (
	"fmt"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/protos/peer"
)

// SimplePrivateDataCC example Chaincode implementation
type SimplePrivateDataCC struct {
}

// Init initializes chaincode
// ===========================
func (t *SimplePrivateDataCC) Init(stub shim.ChaincodeStubInterface) peer.Response {
	return shim.Success(nil)
}

// Invoke - Our entry point for Invocations
// ========================================
func (t *SimplePrivateDataCC) Invoke(stub shim.ChaincodeStubInterface) peer.Response {
	function, args := stub.GetFunctionAndParameters()
	fmt.Println("invoke is running " + function)

	// Handle different functions
	switch function {
	case "put":
		for i := 0; i < len(args); i = i + 3 {
			err := stub.PutPrivateData(args[i], args[i+1], []byte(args[i+2]))
			if err != nil {
				return shim.Error(err.Error())
			}
		}

		return shim.Success([]byte{})

	case "get":
		data, err := stub.GetPrivateData(args[0], args[1])
		if err != nil {
			return shim.Error(err.Error())
		}

		return shim.Success(data)
	default:
		//error
		fmt.Println("invoke did not find func: " + function)
		return shim.Error("Received unknown function invocation")
	}
}
